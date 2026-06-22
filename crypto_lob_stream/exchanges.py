"""
Exchange abstraction for crypto-lob-stream.

Each exchange differs in five ways, and only these are abstracted here:
  1. WebSocket URL
  2. Subscription messages (for exchanges that subscribe via JSON after connect)
  3. Message parsing (trades and depth diffs)
  4. REST snapshot endpoint and parsing
  5. Symbol normalisation

The LOBStreamer itself stays exchange-agnostic and only consumes the
normalised dicts returned by parse_message and parse_snapshot.

Normalised record shapes (what the streamer expects back):

  trade:
    {"type": "trade", "asset": <UPPER>, "timestamp_ms": int,
     "trade_id": int, "price": float, "quantity": float, "buyer_maker": bool}

  depth (one record per price level):
    {"type": "depth", "asset": <UPPER>, "timestamp_ms": int,
     "side": "bid"|"ask", "price": float, "quantity": float,
     "first_update_id": int, "last_update_id": int}

  snapshot level (one record per price level):
    {"timestamp_ms": int, "asset": <UPPER>, "side": "bid"|"ask",
     "price": float, "quantity": float, "last_update_id": int}
"""

import time
from abc import ABC, abstractmethod
from typing import List


class Exchange(ABC):
    """Abstract base for an exchange adapter."""

    name: str = "exchange"

    # Max WebSocket frame size in bytes. Some exchanges (e.g. Coinbase) send
    # the full order book snapshot as a single frame that exceeds the
    # websockets library default of 1 MB. None means use the library default.
    ws_max_size = None

    # -- Connection --------------------------------------------------------

    @abstractmethod
    def ws_url(self, assets: List[str]) -> str:
        """Return the WebSocket URL to connect to."""

    def subscribe_messages(self, assets: List[str]) -> List[dict]:
        """
        Return JSON messages to send after connecting, for exchanges that
        subscribe via the socket rather than the URL. Binance returns [].
        """
        return []

    # -- Symbols -----------------------------------------------------------

    @abstractmethod
    def normalize_symbol(self, symbol: str) -> str:
        """Convert a user symbol to the exchange's native format."""

    # -- Live message parsing ---------------------------------------------

    @abstractmethod
    def parse_message(self, raw: dict) -> List[dict]:
        """
        Parse a raw decoded JSON message into a list of normalised records
        (trade and/or depth). Returns [] for control/heartbeat messages.
        """

    # -- Snapshot ----------------------------------------------------------

    @abstractmethod
    def snapshot_url(self, asset: str) -> str:
        """Return the REST URL for a full order book snapshot."""

    @abstractmethod
    def parse_snapshot(self, asset: str, raw: dict) -> List[dict]:
        """Parse a raw REST snapshot response into normalised level records."""

    # -- Socket-delivered snapshots ---------------------------------------

    def is_socket_snapshot(self, raw: dict) -> bool:
        """
        Return True if this raw message is a full book snapshot delivered
        over the socket (rather than via REST). Default False (REST-based
        exchanges like Binance). Exchanges that push snapshots override this.
        """
        return False

    def socket_snapshot_asset(self, raw: dict) -> str:
        """Return the asset symbol for a socket-delivered snapshot message."""
        return raw.get("product_id", "").upper()


# ──────────────────────────────────────────────────────────────────────────
# Binance
# ──────────────────────────────────────────────────────────────────────────

class BinanceExchange(Exchange):
    name = "binance"

    def ws_url(self, assets: List[str]) -> str:
        streams = []
        for asset in assets:
            a = asset.lower()
            streams.append(f"{a}@trade")
            streams.append(f"{a}@depth@100ms")
        return f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

    def normalize_symbol(self, symbol: str) -> str:
        # Binance uses concatenated uppercase, e.g. BTCUSDT
        return symbol.upper().replace("-", "").replace("/", "")

    def parse_message(self, raw: dict) -> List[dict]:
        stream_name = raw.get("stream", "")
        data = raw.get("data", {})
        if not stream_name:
            return []
        asset = stream_name.split("@")[0].upper()
        ts = int(time.time() * 1000)

        if "@trade" in stream_name:
            required = {"T", "t", "p", "q", "m"}
            if not required.issubset(data):
                return []
            return [{
                "type":         "trade",
                "asset":        asset,
                "timestamp_ms": int(data["T"]),
                "trade_id":     int(data["t"]),
                "price":        float(data["p"]),
                "quantity":     float(data["q"]),
                "buyer_maker":  bool(data["m"]),
            }]

        if "@depth" in stream_name:
            required = {"U", "u", "b", "a"}
            if not required.issubset(data):
                return []
            first_uid = int(data["U"])
            last_uid = int(data["u"])
            out = []
            for price, qty in data["b"]:
                out.append({
                    "type":            "depth",
                    "asset":           asset,
                    "timestamp_ms":    ts,
                    "side":            "bid",
                    "price":           float(price),
                    "quantity":        float(qty),
                    "first_update_id": first_uid,
                    "last_update_id":  last_uid,
                })
            for price, qty in data["a"]:
                out.append({
                    "type":            "depth",
                    "asset":           asset,
                    "timestamp_ms":    ts,
                    "side":            "ask",
                    "price":           float(price),
                    "quantity":        float(qty),
                    "first_update_id": first_uid,
                    "last_update_id":  last_uid,
                })
            return out

        return []

    def snapshot_url(self, asset: str) -> str:
        return f"https://api.binance.com/api/v3/depth?symbol={asset.upper()}&limit=1000"

    def parse_snapshot(self, asset: str, raw: dict) -> List[dict]:
        ts = int(time.time() * 1000)
        last_uid = int(raw["lastUpdateId"])
        records = []
        for price, qty in raw.get("bids", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "bid",
                "price":          float(price),
                "quantity":       float(qty),
                "last_update_id": last_uid,
            })
        for price, qty in raw.get("asks", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "ask",
                "price":          float(price),
                "quantity":       float(qty),
                "last_update_id": last_uid,
            })
        return records


# ──────────────────────────────────────────────────────────────────────────
# Coinbase (Advanced Trade / Exchange WebSocket feed)
# ──────────────────────────────────────────────────────────────────────────

class CoinbaseExchange(Exchange):
    """
    Coinbase Exchange public WebSocket feed (wss://ws-feed.exchange.coinbase.com).

    Coinbase uses a different model from Binance:
      - You connect first, then send a subscribe message.
      - The `level2_batch` channel sends an initial `snapshot` message followed
        by `l2update` messages. There is no separate REST snapshot fetch and no
        Binance-style U/u sequence IDs; ordering is by message arrival, and the
        `time` field on each l2update is used as the sequence reference.
      - The `matches` channel provides trades.

    Because Coinbase delivers its own snapshot over the socket, snapshot_url
    returns "" and the snapshot is captured from the first level2 `snapshot`
    message instead. last_update_id / first_update_id are set to the message
    timestamp in milliseconds, giving a monotonic ordering key.
    """

    name = "coinbase"

    # Coinbase level2 snapshots for liquid pairs exceed the 1 MB default.
    # Allow up to 16 MB frames.
    ws_max_size = 16 * 1024 * 1024

    def ws_url(self, assets: List[str]) -> str:
        return "wss://ws-feed.exchange.coinbase.com"

    def subscribe_messages(self, assets: List[str]) -> List[dict]:
        product_ids = [self.normalize_symbol(a) for a in assets]
        return [{
            "type": "subscribe",
            "product_ids": product_ids,
            "channels": ["level2_batch", "matches"],
        }]

    def normalize_symbol(self, symbol: str) -> str:
        # Coinbase uses dash-separated, e.g. BTC-USD
        s = symbol.upper().replace("/", "-")
        if "-" in s:
            return s
        # Best-effort: split common quote currencies for concatenated input
        for quote in ("USDT", "USDC", "USD", "EUR", "GBP", "BTC"):
            if s.endswith(quote) and len(s) > len(quote):
                return f"{s[:-len(quote)]}-{quote}"
        return s

    def _ms(self, iso_time: str) -> int:
        # Coinbase timestamps look like "2026-06-12T11:05:57.123456Z"
        from datetime import datetime, timezone
        try:
            iso = iso_time.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso)
            return int(dt.timestamp() * 1000)
        except Exception:
            return int(time.time() * 1000)

    def parse_message(self, raw: dict) -> List[dict]:
        msg_type = raw.get("type", "")

        # Trades
        if msg_type == "match" or msg_type == "last_match":
            asset = raw.get("product_id", "").upper()
            if not asset or "price" not in raw or "size" not in raw:
                return []
            # Coinbase "side" is the maker side; buyer_maker is True when the
            # maker was the buyer, i.e. an incoming sell matched a resting buy.
            buyer_maker = (raw.get("side") == "buy")
            return [{
                "type":         "trade",
                "asset":        asset,
                "timestamp_ms": self._ms(raw.get("time", "")),
                "trade_id":     int(raw.get("trade_id", 0)),
                "price":        float(raw["price"]),
                "quantity":     float(raw["size"]),
                "buyer_maker":  buyer_maker,
            }]

        # Incremental depth updates
        if msg_type == "l2update":
            asset = raw.get("product_id", "").upper()
            ts = self._ms(raw.get("time", ""))
            out = []
            for change in raw.get("changes", []):
                # change = [side, price, size]; side is "buy"/"sell"
                if len(change) != 3:
                    continue
                side_raw, price, size = change
                side = "bid" if side_raw == "buy" else "ask"
                out.append({
                    "type":            "depth",
                    "asset":           asset,
                    "timestamp_ms":    ts,
                    "side":            side,
                    "price":           float(price),
                    "quantity":        float(size),
                    "first_update_id": ts,
                    "last_update_id":  ts,
                })
            return out

        # Control messages (subscriptions, heartbeats, the initial snapshot)
        return []

    def snapshot_url(self, asset: str) -> str:
        # Coinbase delivers its snapshot over the socket, not via REST.
        return ""

    def is_socket_snapshot(self, raw: dict) -> bool:
        return raw.get("type") == "snapshot"

    def socket_snapshot_asset(self, raw: dict) -> str:
        return raw.get("product_id", "").upper()

    def parse_snapshot(self, asset: str, raw: dict) -> List[dict]:
        # Used when the first level2 message of type "snapshot" arrives.
        ts = int(time.time() * 1000)
        records = []
        for price, size in raw.get("bids", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "bid",
                "price":          float(price),
                "quantity":       float(size),
                "last_update_id": ts,
            })
        for price, size in raw.get("asks", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "ask",
                "price":          float(price),
                "quantity":       float(size),
                "last_update_id": ts,
            })
        return records


# ──────────────────────────────────────────────────────────────────────────
# OKX (v5 public WebSocket)
# ──────────────────────────────────────────────────────────────────────────

class OKXExchange(Exchange):
    """
    OKX v5 public WebSocket feed (wss://ws.okx.com:8443/ws/v5/public).

    Model:
      - Connect, then send a subscribe message listing channels + instId.
      - The `books` channel sends action="snapshot" first (full book, 400
        levels), then action="update" deltas. Quantity 0 removes a level.
      - The `trades` channel sends each execution.
      - Messages are wrapped: {"arg": {...}, "action": ..., "data": [...]}.
      - OKX provides seqId / prevSeqId on book messages for ordering, which
        map to last_update_id / first_update_id.
      - Symbol format is dash-separated, e.g. BTC-USDT.
    """

    name = "okx"
    ws_max_size = 16 * 1024 * 1024  # books snapshots can be large

    def ws_url(self, assets: List[str]) -> str:
        return "wss://ws.okx.com:8443/ws/v5/public"

    def subscribe_messages(self, assets: List[str]) -> List[dict]:
        args = []
        for a in assets:
            inst = self.normalize_symbol(a)
            args.append({"channel": "books", "instId": inst})
            args.append({"channel": "trades", "instId": inst})
        return [{"op": "subscribe", "args": args}]

    def normalize_symbol(self, symbol: str) -> str:
        s = symbol.upper().replace("/", "-")
        if "-" in s:
            return s
        for quote in ("USDT", "USDC", "USD", "EUR", "GBP", "BTC"):
            if s.endswith(quote) and len(s) > len(quote):
                return f"{s[:-len(quote)]}-{quote}"
        return s

    def parse_message(self, raw: dict) -> List[dict]:
        arg = raw.get("arg", {})
        channel = arg.get("channel", "")
        data = raw.get("data", [])
        if not channel or not data:
            return []

        inst = arg.get("instId", "").upper()

        # Trades
        if channel == "trades":
            out = []
            for t in data:
                try:
                    # OKX side is the taker side: "buy" or "sell".
                    # buyer_maker is True when the taker was a seller
                    # (i.e. the resting buy order — the maker — was hit).
                    buyer_maker = (t.get("side") == "sell")
                    out.append({
                        "type":         "trade",
                        "asset":        inst,
                        "timestamp_ms": int(t["ts"]),
                        "trade_id":     int(t["tradeId"]),
                        "price":        float(t["px"]),
                        "quantity":     float(t["sz"]),
                        "buyer_maker":  buyer_maker,
                    })
                except (ValueError, TypeError, KeyError):
                    continue
            return out

        # Order book updates (snapshots are handled via is_socket_snapshot)
        if channel == "books":
            action = raw.get("action", "")
            if action == "snapshot":
                return []  # handled by the snapshot path
            out = []
            for book in data:
                ts = int(book.get("ts", 0))
                seq = int(book.get("seqId", ts))
                prev = int(book.get("prevSeqId", seq))
                for price, qty, *_ in book.get("bids", []):
                    out.append({
                        "type":            "depth",
                        "asset":           inst,
                        "timestamp_ms":    ts,
                        "side":            "bid",
                        "price":           float(price),
                        "quantity":        float(qty),
                        "first_update_id": prev,
                        "last_update_id":  seq,
                    })
                for price, qty, *_ in book.get("asks", []):
                    out.append({
                        "type":            "depth",
                        "asset":           inst,
                        "timestamp_ms":    ts,
                        "side":            "ask",
                        "price":           float(price),
                        "quantity":        float(qty),
                        "first_update_id": prev,
                        "last_update_id":  seq,
                    })
            return out

        return []

    def snapshot_url(self, asset: str) -> str:
        # OKX delivers the snapshot over the socket on the books channel.
        return ""

    def is_socket_snapshot(self, raw: dict) -> bool:
        return (
            raw.get("arg", {}).get("channel") == "books"
            and raw.get("action") == "snapshot"
        )

    def socket_snapshot_asset(self, raw: dict) -> str:
        return raw.get("arg", {}).get("instId", "").upper()

    def parse_snapshot(self, asset: str, raw: dict) -> List[dict]:
        data = raw.get("data", [])
        if not data:
            return []
        book = data[0]
        ts = int(book.get("ts", 0)) or int(time.time() * 1000)
        seq = int(book.get("seqId", ts))
        records = []
        for price, qty, *_ in book.get("bids", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "bid",
                "price":          float(price),
                "quantity":       float(qty),
                "last_update_id": seq,
            })
        for price, qty, *_ in book.get("asks", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "ask",
                "price":          float(price),
                "quantity":       float(qty),
                "last_update_id": seq,
            })
        return records


# ──────────────────────────────────────────────────────────────────────────
# Kraken (v2 public WebSocket)
# ──────────────────────────────────────────────────────────────────────────

class KrakenExchange(Exchange):
    """
    Kraken v2 public WebSocket feed (wss://ws.kraken.com/v2).

    Model:
      - Connect, then send subscribe messages for the book and trade channels.
      - The `book` channel sends type="snapshot" first, then type="update".
        Unlike other exchanges, Kraken expresses levels as objects
        {"price": ..., "qty": ...} rather than [price, qty] arrays.
        qty 0 removes a level.
      - The `trade` channel sends executions with side/price/qty/trade_id/timestamp.
      - Symbol format is slash-separated, e.g. BTC/USD.
      - Kraken has no Binance-style sequence IDs on book updates; the message
        timestamp (ms) is used as the ordering key. A CRC32 checksum is provided
        for client-side book verification (not used here).
      - Control messages: subscription acks carry a "method" key; heartbeat and
        status arrive on their own channels. All are ignored.
    """

    name = "kraken"
    ws_max_size = 16 * 1024 * 1024

    def ws_url(self, assets: List[str]) -> str:
        return "wss://ws.kraken.com/v2"

    def subscribe_messages(self, assets: List[str]) -> List[dict]:
        symbols = [self.normalize_symbol(a) for a in assets]
        return [
            {"method": "subscribe",
             "params": {"channel": "book", "symbol": symbols, "depth": 25}},
            {"method": "subscribe",
             "params": {"channel": "trade", "symbol": symbols}},
        ]

    def normalize_symbol(self, symbol: str) -> str:
        s = symbol.upper().replace("-", "/")
        if "/" in s:
            return s
        for quote in ("USDT", "USDC", "USD", "EUR", "GBP", "BTC"):
            if s.endswith(quote) and len(s) > len(quote):
                return f"{s[:-len(quote)]}/{quote}"
        return s

    def _ms(self, iso_time: str) -> int:
        from datetime import datetime
        try:
            return int(datetime.fromisoformat(
                iso_time.replace("Z", "+00:00")
            ).timestamp() * 1000)
        except Exception:
            return int(time.time() * 1000)

    def parse_message(self, raw: dict) -> List[dict]:
        channel = raw.get("channel", "")
        msg_type = raw.get("type", "")
        data = raw.get("data", [])
        if not channel or not data:
            return []

        # Trades
        if channel == "trade":
            out = []
            for t in data:
                try:
                    # Kraken side is the aggressor (taker) side.
                    buyer_maker = (t.get("side") == "sell")
                    out.append({
                        "type":         "trade",
                        "asset":        t.get("symbol", "").upper(),
                        "timestamp_ms": self._ms(t.get("timestamp", "")),
                        "trade_id":     int(t.get("trade_id", 0)),
                        "price":        float(t["price"]),
                        "quantity":     float(t["qty"]),
                        "buyer_maker":  buyer_maker,
                    })
                except (ValueError, TypeError, KeyError):
                    continue
            return out

        # Book updates (snapshots handled via is_socket_snapshot)
        if channel == "book":
            if msg_type == "snapshot":
                return []
            out = []
            for book in data:
                asset = book.get("symbol", "").upper()
                ts = self._ms(book.get("timestamp", ""))
                for lvl in book.get("bids", []):
                    out.append({
                        "type":            "depth",
                        "asset":           asset,
                        "timestamp_ms":    ts,
                        "side":            "bid",
                        "price":           float(lvl["price"]),
                        "quantity":        float(lvl["qty"]),
                        "first_update_id": ts,
                        "last_update_id":  ts,
                    })
                for lvl in book.get("asks", []):
                    out.append({
                        "type":            "depth",
                        "asset":           asset,
                        "timestamp_ms":    ts,
                        "side":            "ask",
                        "price":           float(lvl["price"]),
                        "quantity":        float(lvl["qty"]),
                        "first_update_id": ts,
                        "last_update_id":  ts,
                    })
            return out

        return []

    def snapshot_url(self, asset: str) -> str:
        return ""

    def is_socket_snapshot(self, raw: dict) -> bool:
        return raw.get("channel") == "book" and raw.get("type") == "snapshot"

    def socket_snapshot_asset(self, raw: dict) -> str:
        data = raw.get("data", [])
        if data:
            return data[0].get("symbol", "").upper()
        return ""

    def parse_snapshot(self, asset: str, raw: dict) -> List[dict]:
        data = raw.get("data", [])
        if not data:
            return []
        book = data[0]
        ts = self._ms(book.get("timestamp", "")) or int(time.time() * 1000)
        records = []
        for lvl in book.get("bids", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "bid",
                "price":          float(lvl["price"]),
                "quantity":       float(lvl["qty"]),
                "last_update_id": ts,
            })
        for lvl in book.get("asks", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "ask",
                "price":          float(lvl["price"]),
                "quantity":       float(lvl["qty"]),
                "last_update_id": ts,
            })
        return records


# ──────────────────────────────────────────────────────────────────────────
# Bybit (v5 public spot WebSocket)
# ──────────────────────────────────────────────────────────────────────────

class BybitExchange(Exchange):
    """
    Bybit v5 public spot WebSocket feed (wss://stream.bybit.com/v5/public/spot).

    Model:
      - Connect, then send {"op": "subscribe", "args": [...]} listing topics
        like "orderbook.50.BTCUSDT" and "publicTrade.BTCUSDT".
      - The orderbook topic sends type="snapshot" first, then type="delta".
        Bids/asks are [price, qty] arrays; qty "0" removes a level. Bybit
        provides a real update id `u` and `seq`, used as the ordering key.
      - The publicTrade topic sends trade arrays (type snapshot or delta;
        both carry trade data and are treated identically).
      - Symbol format is concatenated, e.g. BTCUSDT.
      - Control messages: subscribe acks carry op/success; pong frames carry
        op="pong". All are ignored.
    """

    name = "bybit"
    ws_max_size = 16 * 1024 * 1024

    def ws_url(self, assets: List[str]) -> str:
        return "wss://stream.bybit.com/v5/public/spot"

    def subscribe_messages(self, assets: List[str]) -> List[dict]:
        args = []
        for a in assets:
            sym = self.normalize_symbol(a)
            args.append(f"orderbook.50.{sym}")
            args.append(f"publicTrade.{sym}")
        return [{"op": "subscribe", "args": args}]

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.upper().replace("-", "").replace("/", "")

    def parse_message(self, raw: dict) -> List[dict]:
        topic = raw.get("topic", "")
        if not topic:
            return []

        # Trades: publicTrade.<SYMBOL>
        if topic.startswith("publicTrade."):
            data = raw.get("data", [])
            out = []
            for t in data:
                try:
                    # Bybit "S" is the taker side (Buy/Sell).
                    buyer_maker = (t.get("S") == "Sell")
                    out.append({
                        "type":         "trade",
                        "asset":        t.get("s", "").upper(),
                        "timestamp_ms": int(t["T"]),
                        "trade_id":     self._trade_id(t.get("i", 0)),
                        "price":        float(t["p"]),
                        "quantity":     float(t["v"]),
                        "buyer_maker":  buyer_maker,
                    })
                except (ValueError, TypeError, KeyError):
                    continue
            return out

        # Orderbook: orderbook.<depth>.<SYMBOL>
        if topic.startswith("orderbook."):
            if raw.get("type") == "snapshot":
                return []  # handled by snapshot path
            data = raw.get("data", {})
            asset = data.get("s", "").upper()
            ts = int(raw.get("ts", 0))
            uid = int(data.get("u", ts))
            seq = int(data.get("seq", uid))
            out = []
            for price, qty in data.get("b", []):
                out.append({
                    "type":            "depth",
                    "asset":           asset,
                    "timestamp_ms":    ts,
                    "side":            "bid",
                    "price":           float(price),
                    "quantity":        float(qty),
                    "first_update_id": seq,
                    "last_update_id":  uid,
                })
            for price, qty in data.get("a", []):
                out.append({
                    "type":            "depth",
                    "asset":           asset,
                    "timestamp_ms":    ts,
                    "side":            "ask",
                    "price":           float(price),
                    "quantity":        float(qty),
                    "first_update_id": seq,
                    "last_update_id":  uid,
                })
            return out

        return []

    @staticmethod
    def _trade_id(raw_id) -> int:
        # Bybit trade IDs may be UUID strings; hash to a stable int if so.
        try:
            return int(raw_id)
        except (ValueError, TypeError):
            return abs(hash(str(raw_id))) % (10 ** 18)

    def snapshot_url(self, asset: str) -> str:
        return ""

    def is_socket_snapshot(self, raw: dict) -> bool:
        return (
            str(raw.get("topic", "")).startswith("orderbook.")
            and raw.get("type") == "snapshot"
        )

    def socket_snapshot_asset(self, raw: dict) -> str:
        return raw.get("data", {}).get("s", "").upper()

    def parse_snapshot(self, asset: str, raw: dict) -> List[dict]:
        data = raw.get("data", {})
        ts = int(raw.get("ts", 0)) or int(time.time() * 1000)
        uid = int(data.get("u", ts))
        records = []
        for price, qty in data.get("b", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "bid",
                "price":          float(price),
                "quantity":       float(qty),
                "last_update_id": uid,
            })
        for price, qty in data.get("a", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "ask",
                "price":          float(price),
                "quantity":       float(qty),
                "last_update_id": uid,
            })
        return records


# ──────────────────────────────────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────────────────────────────────

_EXCHANGES = {
    "binance": BinanceExchange,
    "coinbase": CoinbaseExchange,
    "okx": OKXExchange,
    "kraken": KrakenExchange,
    "bybit": BybitExchange,
}


def get_exchange(name: str) -> Exchange:
    """Return an exchange adapter instance by name."""
    key = name.lower()
    if key not in _EXCHANGES:
        available = ", ".join(sorted(_EXCHANGES))
        raise ValueError(
            f"Unknown exchange '{name}'. Available: {available}."
        )
    return _EXCHANGES[key]()


def available_exchanges() -> List[str]:
    return sorted(_EXCHANGES)