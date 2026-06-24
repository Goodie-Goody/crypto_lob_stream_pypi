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
import zlib
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple


class Exchange(ABC):
    """Abstract base for an exchange adapter."""

    name: str = "exchange"

    # Max WebSocket frame size in bytes. Some exchanges (e.g. Coinbase) send
    # the full order book snapshot as a single frame that exceeds the
    # websockets library default of 1 MB. None means use the library default.
    ws_max_size = None

    # True when first_update_id/last_update_id on depth records are real
    # exchange-assigned sequence numbers that should chain message-to-message
    # (Binance U/u, Bybit seq/u, OKX prevSeqId/seqId). False when they're a
    # message-timestamp placeholder with no real continuity invariant
    # (Coinbase, Kraken currently use the message time for both fields).
    # LOBStreamer's gap detector only runs for exchanges where this is True;
    # running it against a timestamp placeholder would just detect "the
    # clock moved", which isn't a gap.
    has_sequence_ids: bool = True

    # True when this adapter implements live order-book checksum
    # verification (see KrakenExchange below for the only current example).
    supports_checksum: bool = False

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

    # -- Optional auxiliary connection --------------------------------------

    # Optional second WebSocket connection, for exchanges where some
    # streams must be routed separately from the main one (currently used
    # by BinanceFuturesExchange -- see its docstring). Returning None
    # (default) means no auxiliary connection is needed.
    def aux_ws_url(self, assets: List[str]) -> Optional[str]:
        return None

    def aux_subscribe_messages(self, assets: List[str]) -> List[dict]:
        return []

    def parse_aux_message(self, raw: dict) -> List[dict]:
        """Parse a message arriving on the auxiliary connection. Defaults
        to the same parser as the main connection; override only if the
        aux connection needs different handling."""
        return self.parse_message(raw)

    # -- Open interest (futures/perps only) ---------------------------------

    # REST endpoint for open interest, for exchanges with no WebSocket push
    # for it at all (currently binance_futures -- confirmed via Binance's
    # own docs and independently by Tardis.dev, a professional data
    # vendor, REST-polling the identical endpoint themselves rather than
    # using a push stream that doesn't exist). Returning "" (default)
    # means open interest is delivered some other way (a WS channel,
    # handled in parse_message/parse_aux_message) or not supported.
    def open_interest_url(self, asset: str) -> str:
        return ""

    def parse_open_interest(self, asset: str, raw: dict) -> List[dict]:
        """Parse a REST open-interest response into a normalised record.
        Only relevant for exchanges where open_interest_url() is non-empty."""
        return []


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

    # l2update has no real exchange sequence number -- first/last_update_id
    # are both set to the message timestamp (see parse_message). Gap
    # detection against that would be meaningless, so it's disabled here.
    has_sequence_ids = False

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
# OKX -- USDT-margined perpetual swaps
# ──────────────────────────────────────────────────────────────────────────

class OKXSwapExchange(OKXExchange):
    """
    OKX v5 public WebSocket feed for USDT-margined perpetual swaps (instId
    suffix "-SWAP", e.g. BTC-USDT-SWAP). Same public endpoint, same
    `books`/`trades` channels and message shapes as spot OKX -- just a
    different instId -- plus `funding-rate`, `open-interest`, and
    `liquidation-orders` channels for "funding"/"open_interest"/
    "liquidation" record types (matching binance_futures' stress-research
    coverage).

    Differences from OKXExchange (spot):
      - normalize_symbol appends "-SWAP" to the spot-style instId.
      - subscribe_messages additionally subscribes to `funding-rate` and
        `open-interest` per instId, and `liquidation-orders` once for the
        whole SWAP instrument type (see below for why that one's different).
      - parse_message handles all three.

    `liquidation-orders` is subscribed differently from every other
    channel here: OKX pushes it per `instType` (e.g. all of SWAP), not per
    instId -- confirmed via a live capture in a ccxt issue thread, where a
    single subscription to instType=SWAP returned liquidations across
    many unrelated symbols (DYDX-USDT-SWAP in that example). So this
    subscribes once for the instrument type, and parse_message filters
    incoming events down to just the assets this feed actually tracks.

    mark_price is NOT populated in funding records (left as None). OKX's
    `funding-rate` channel (fields confirmed via OKX's own SDK source:
    fundingRate, fundingTime, nextFundingRate, nextFundingTime) doesn't
    include a mark price -- getting one would mean also subscribing to
    OKX's separate `mark-price` channel, whose exact push field name
    wasn't confirmed from documentation in the time available (today
    already had two bugs -- Kraken's checksum truncation, Binance's
    routed-path split -- that started as a confident-looking guess from
    docs alone, so this is left as a documented gap rather than a third
    one). funding_rate/next_funding_ms and the open-interest/liquidation
    fields below are solid; mark_price is None until verified.
    Recommend validating against a live connection.
    """

    name = "okx_swap"

    def __init__(self):
        # Populated by subscribe_messages(); used by parse_message to
        # filter the instType-wide liquidation-orders feed down to just
        # the assets this feed actually tracks (see class docstring).
        self._tracked_assets = set()

    def normalize_symbol(self, symbol: str) -> str:
        base = super().normalize_symbol(symbol)
        return base if base.endswith("-SWAP") else f"{base}-SWAP"

    def subscribe_messages(self, assets: List[str]) -> List[dict]:
        args = []
        for a in assets:
            inst = self.normalize_symbol(a)
            self._tracked_assets.add(inst)
            args.append({"channel": "books", "instId": inst})
            args.append({"channel": "trades", "instId": inst})
            args.append({"channel": "funding-rate", "instId": inst})
            args.append({"channel": "open-interest", "instId": inst})
        # Subscribed once for the whole instrument type, not per-asset --
        # see class docstring.
        args.append({"channel": "liquidation-orders", "instType": "SWAP"})
        return [{"op": "subscribe", "args": args}]

    def parse_message(self, raw: dict) -> List[dict]:
        arg = raw.get("arg", {})
        channel = arg.get("channel", "")

        if channel == "funding-rate":
            data = raw.get("data", [])
            out = []
            for d in data:
                try:
                    out.append({
                        "type":            "funding",
                        "asset":           d.get("instId", arg.get("instId", "")).upper(),
                        "timestamp_ms":    int(d.get("ts", time.time() * 1000)),
                        "mark_price":      None,
                        "funding_rate":    float(d["fundingRate"]),
                        "next_funding_ms": int(d["nextFundingTime"]),
                    })
                except (ValueError, TypeError, KeyError):
                    continue
            return out

        if channel == "open-interest":
            # REST payload fields confirmed (instId, instType, oi, oiCcy,
            # oiUsd, ts); the WS push is assumed to mirror them, per OKX's
            # general consistency between REST and WS field naming, but
            # this specific WS shape wasn't separately confirmed against
            # a live push -- if oiUsd is ever absent in practice,
            # open_interest_value below will just come through as None
            # rather than raising.
            data = raw.get("data", [])
            out = []
            for d in data:
                try:
                    out.append({
                        "type":                "open_interest",
                        "asset":               d.get("instId", "").upper(),
                        "timestamp_ms":        int(d.get("ts", time.time() * 1000)),
                        "open_interest":       float(d["oi"]),
                        "open_interest_value": float(d["oiUsd"]) if "oiUsd" in d else None,
                    })
                except (ValueError, TypeError, KeyError):
                    continue
            return out

        if channel == "liquidation-orders":
            # Pushed per instType (e.g. all of SWAP), not per instId --
            # filter down to just what this feed tracks.
            data = raw.get("data", [])
            out = []
            for d in data:
                inst_id = d.get("instId", "")
                if inst_id not in self._tracked_assets:
                    continue
                for detail in d.get("details", []):
                    try:
                        out.append({
                            "type":         "liquidation",
                            "asset":        inst_id.upper(),
                            "timestamp_ms": int(detail["ts"]),
                            "side":         detail["side"].lower(),
                            "price":        float(detail["bkPx"]),
                            "quantity":     float(detail["sz"]),
                        })
                    except (ValueError, TypeError, KeyError):
                        continue
            return out

        return super().parse_message(raw)


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

    # Kraken v2 book updates have no Binance-style sequence id -- the
    # message timestamp is used for both first/last_update_id (see
    # parse_message), so there's no real continuity invariant to check.
    # checksum verification (below) is the meaningful integrity check here.
    has_sequence_ids = False
    supports_checksum = True

    def __init__(self):
        # Live top-of-book mirror per asset, maintained only when checksum
        # verification is active: {asset: {"bid": {price: (price_str, qty_str)},
        #                                   "ask": {price: (price_str, qty_str)}}}
        # Keeping the original price/qty strings (not just floats) matters --
        # Kraken's checksum format is sensitive to exact digit formatting
        # (e.g. "65000.10" vs "65000.1" produce different checksums after
        # the decimal point is stripped), which a float round-trip would
        # lose. See verify_checksum() and LOBStreamer's json.loads call.
        self._books = {}
        # Must match the "depth" sent in subscribe_messages() -- Kraken
        # doesn't send explicit qty:0 removals for levels that fall out of
        # your subscribed depth (only for in-scope removals), so the local
        # mirror has to be truncated to the same depth itself.
        self._book_depth = 25

    def ws_url(self, assets: List[str]) -> str:
        return "wss://ws.kraken.com/v2"

    def subscribe_messages(self, assets: List[str]) -> List[dict]:
        symbols = [self.normalize_symbol(a) for a in assets]
        return [
            {"method": "subscribe",
             "params": {"channel": "book", "symbol": symbols, "depth": self._book_depth}},
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

    # -- Checksum verification (optional, off by default) ------------------
    #
    # Implements Kraken's documented v2 book-checksum algorithm:
    #   1. Take the best 10 ask levels (ascending price) and best 10 bid
    #      levels (descending price) of the live book.
    #   2. For each level's price and quantity: remove the decimal point,
    #      then strip leading zeros from the resulting digit string.
    #   3. Concatenate price+qty for all 10 asks, then all 10 bids, in that
    #      order, into one string.
    #   4. CRC32 the ASCII bytes of that string; compare to the integer
    #      "checksum" field Kraken sends on each book snapshot/update.
    #
    # This is implemented from Kraken's public docs, not verified against a
    # live connection from inside this environment -- the algorithm's
    # formatting edge cases (e.g. whole-number quantities, very small
    # prices) are exactly the kind of detail that's easy to get subtly
    # wrong from documentation alone. Treat the first live run as the real
    # test: LOBStreamer logs every computed-vs-received pair at DEBUG level
    # and writes mismatches to the checksum log, so a systematic mismatch
    # (rather than an occasional one) will be obvious immediately and
    # almost certainly means the format above needs adjusting, not that
    # the book itself is actually wrong.

    @staticmethod
    def _fmt_num(raw_value) -> str:
        """Kraken checksum number formatting: strip '.', then leading zeros."""
        s = str(raw_value)
        if "." in s:
            whole, frac = s.split(".", 1)
        else:
            whole, frac = s, ""
        digits = (whole + frac).lstrip("0")
        return digits or "0"

    def update_book_and_checksum(
        self, book: dict, is_snapshot: bool
    ) -> Tuple[Optional[str], Optional[bool], Optional[int], Optional[int]]:
        """
        Apply a Kraken v2 `book` channel payload (one element of the
        message's "data" list, snapshot or update) to this exchange's live
        per-asset book mirror, and verify the embedded checksum if present.

        Returns (asset, match, expected, received). `match` is None when
        the payload carries no "checksum" field (shouldn't normally happen
        on Kraken's book channel, but defensive).
        """
        asset = book.get("symbol", "").upper()
        if not asset:
            return None, None, None, None

        state = self._books.setdefault(asset, {"bid": {}, "ask": {}})
        if is_snapshot:
            state["bid"] = {}
            state["ask"] = {}

        for side_key, wire_key in (("bid", "bids"), ("ask", "asks")):
            for lvl in book.get(wire_key, []):
                price_str = str(lvl["price"])
                qty_str = str(lvl["qty"])
                price = float(price_str)
                if float(qty_str) == 0:
                    state[side_key].pop(price, None)
                else:
                    state[side_key][price] = (price_str, qty_str)

        # Kraken's docs are explicit that levels falling out of your
        # subscribed depth (not just the top 10 used for the checksum) are
        # NOT announced with an explicit qty:0 -- you're expected to
        # truncate locally. Without this, the mirror accumulates stale
        # deep levels indefinitely. depth defaults to 25 in
        # subscribe_messages(); truncate to the same value here.
        for side_key in ("bid", "ask"):
            if len(state[side_key]) > self._book_depth:
                ordered = sorted(
                    state[side_key].items(), reverse=(side_key == "bid")
                )
                state[side_key] = dict(ordered[: self._book_depth])

        received = book.get("checksum")
        if received is None:
            return asset, None, None, None

        asks_top = sorted(state["ask"].items())[:10]            # ascending
        bids_top = sorted(state["bid"].items(), reverse=True)[:10]  # descending

        parts = []
        for _, (price_str, qty_str) in asks_top:
            parts.append(self._fmt_num(price_str))
            parts.append(self._fmt_num(qty_str))
        for _, (price_str, qty_str) in bids_top:
            parts.append(self._fmt_num(price_str))
            parts.append(self._fmt_num(qty_str))

        payload = "".join(parts)
        expected = zlib.crc32(payload.encode("ascii"))
        received_int = int(received)
        return asset, (expected == received_int), expected, received_int


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
        provides a real per-symbol update id `u`, used as the ordering key
        (NOT `seq`, a different "cross sequence" counter on an unrelated
        scale -- see the comment in parse_message for why that matters).
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
            # "u" is the real per-symbol update counter and the correct
            # continuity id -- it's what parse_snapshot() seeds
            # last_update_id from too. "seq" is a *different* counter
            # ("cross sequence") that Bybit's own docs describe as being
            # for comparing freshness across different data sources (e.g.
            # REST vs WS), not a chainable per-message id -- it's on a
            # completely different numeric scale (tens of billions vs u's
            # hundreds of millions). An earlier version of this method
            # used seq for first_update_id, which meant the gap detector
            # was comparing two unrelated counters and reported a "gap"
            # on essentially every message; caught via a live run showing
            # thousands of gap events in under a minute, each with mismatched
            # orders of magnitude between expected/received -- a real gap
            # looks like a small jump, not 12 orders of magnitude.
            uid = int(data.get("u", ts))
            out = []
            for price, qty in data.get("b", []):
                out.append({
                    "type":            "depth",
                    "asset":           asset,
                    "timestamp_ms":    ts,
                    "side":            "bid",
                    "price":           float(price),
                    "quantity":        float(qty),
                    "first_update_id": uid,
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
                    "first_update_id": uid,
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
# Bybit -- USDT-margined linear perpetuals
# ──────────────────────────────────────────────────────────────────────────

class BybitLinearExchange(BybitExchange):
    """
    Bybit v5 public WebSocket feed for USDT-margined linear perpetuals --
    same orderbook/trade message shapes as Bybit spot, just a different
    host (wss://stream.bybit.com/v5/public/linear vs /spot) -- plus a
    `tickers.<symbol>` topic for "funding" and "open_interest" record
    types, and an `allLiquidation.<symbol>` topic for "liquidation"
    records (matching binance_futures' stress-research coverage).

    Confirmed via docs (and real sample payloads): Bybit's linear tickers
    message bundles markPrice, fundingRate, nextFundingTime, openInterest,
    and openInterestValue all into one push -- unlike OKX, which splits
    funding rate and open interest across two separate channels -- so
    both funding (with mark_price) and open_interest come from this one
    subscription, no extra channel needed.

    The first tickers message per symbol is a full snapshot; subsequent
    "delta" messages may omit fields that haven't changed since the last
    push. A funding/open_interest record is only emitted when that
    record's own required fields are all present on a given message, so
    expect fewer records than a channel that always re-sends everything --
    at least one of each per snapshot, typically more.
    """

    name = "bybit_linear"

    def ws_url(self, assets: List[str]) -> str:
        return "wss://stream.bybit.com/v5/public/linear"

    def subscribe_messages(self, assets: List[str]) -> List[dict]:
        args = []
        for a in assets:
            sym = self.normalize_symbol(a)
            args.append(f"orderbook.50.{sym}")
            args.append(f"publicTrade.{sym}")
            args.append(f"tickers.{sym}")
            # Confirmed real channel (replacing the deprecated
            # liquidation.{symbol}, which only pushed 1/sec and missed
            # most actual liquidations per Bybit's own changelog).
            args.append(f"allLiquidation.{sym}")
        return [{"op": "subscribe", "args": args}]

    def parse_message(self, raw: dict) -> List[dict]:
        topic = raw.get("topic", "")

        if topic.startswith("tickers."):
            data = raw.get("data", {})
            asset = data.get("symbol", "").upper()
            out = []

            funding_fields = {"markPrice", "fundingRate", "nextFundingTime"}
            if funding_fields.issubset(data):
                try:
                    out.append({
                        "type":            "funding",
                        "asset":           asset,
                        "timestamp_ms":    int(raw.get("ts", time.time() * 1000)),
                        "mark_price":      float(data["markPrice"]),
                        "funding_rate":    float(data["fundingRate"]),
                        "next_funding_ms": int(data["nextFundingTime"]),
                    })
                except (ValueError, TypeError, KeyError):
                    pass

            # Confirmed via Bybit's own tickers payload (and a real
            # sample capture): openInterest/openInterestValue arrive
            # bundled into this same ticker message -- no separate
            # channel needed, unlike OKX where these are split out.
            oi_fields = {"openInterest", "openInterestValue"}
            if oi_fields.issubset(data):
                try:
                    out.append({
                        "type":                "open_interest",
                        "asset":               asset,
                        "timestamp_ms":        int(raw.get("ts", time.time() * 1000)),
                        "open_interest":       float(data["openInterest"]),
                        "open_interest_value": float(data["openInterestValue"]),
                    })
                except (ValueError, TypeError, KeyError):
                    pass

            return out

        if topic.startswith("allLiquidation."):
            # {"topic": "allLiquidation.ROSEUSDT", "type": "snapshot",
            #  "ts": ..., "data": [{"T":..., "s":..., "S":..., "v":..., "p":...}]}
            out = []
            for d in raw.get("data", []):
                try:
                    out.append({
                        "type":         "liquidation",
                        "asset":        d["s"].upper(),
                        "timestamp_ms": int(d["T"]),
                        "side":         d["S"].lower(),
                        "price":        float(d["p"]),
                        "quantity":     float(d["v"]),
                    })
                except (ValueError, TypeError, KeyError):
                    continue
            return out

        return super().parse_message(raw)


# ──────────────────────────────────────────────────────────────────────────
# Binance USDⓢ-M Futures (perpetuals) -- public WebSocket
# ──────────────────────────────────────────────────────────────────────────

class BinanceFuturesExchange(BinanceExchange):
    """
    Binance USDⓢ-M Futures public WebSocket feed (fstream.binance.com).

    NEW, and the least battle-tested adapter in this file -- spot trade/depth
    message shapes carry over almost unchanged, but this has not been run
    against a live connection from inside this environment. Differences
    from BinanceExchange (spot) implemented here:

      - Different hosts: wss://fstream.binance.com for the socket,
        https://fapi.binance.com for REST snapshots (vs stream.binance.com /
        api.binance.com for spot).
      - Futures depth diffs carry an extra "pu" (previous final update id)
        field, which Binance's futures docs specify as the correct
        continuity anchor (each event's pu should equal the prior event's
        u). It's mapped to first_update_id here so it works unchanged
        through the existing gap detector, which already checks
        first_update_id against the previous last_update_id.
      - A markPrice@1s stream provides funding rate / mark price, producing
        a new "funding" record type that no spot exchange in this file
        emits (routed to its own buffer/Parquet table -- FUNDING_SCHEMA in
        schemas.py). Binance recently split its WebSocket infrastructure
        into /public, /market, and /private routed paths; a connection
        without one of those paths in the URL only receives /public stream
        data, and markPrice is a /market stream -- it's silently dropped
        rather than erroring, which makes this easy to miss. trade/depth
        are /public and work fine on the plain, unrouted connection (the
        one ws_url() below builds), so markPrice is fetched over a second,
        /market-routed connection instead (aux_ws_url() below). This was
        caught by an actual live run silently producing zero funding
        records despite the package running with no errors -- worth
        knowing if this Binance behaviour changes again.
      - Contract symbols (e.g. BTCUSDT perpetual vs BTCUSDT spot) happen to
        share the same string for the common perpetual contracts, so
        normalize_symbol is inherited unchanged from BinanceExchange. This
        will NOT hold for dated/quarterly futures contracts (e.g.
        BTCUSDT_250926), which aren't handled by this adapter.

    Recommend validating against a live connection -- and against a couple
    of different contracts -- before relying on this for research data.
    """

    name = "binance_futures"

    def ws_url(self, assets: List[str]) -> str:
        streams = []
        for asset in assets:
            a = asset.lower()
            streams.append(f"{a}@trade")
            streams.append(f"{a}@depth@100ms")
        return f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

    def aux_ws_url(self, assets: List[str]) -> str:
        streams = []
        for a in assets:
            streams.append(f"{a.lower()}@markPrice@1s")
            # Confirmed via Binance's official routed-stream mapping:
            # forceOrder is /market, same category as markPrice -- same
            # connection, no third one needed.
            streams.append(f"{a.lower()}@forceOrder")
        return f"wss://fstream.binance.com/market/stream?streams={'/'.join(streams)}"

    def parse_message(self, raw: dict) -> List[dict]:
        stream_name = raw.get("stream", "")
        data = raw.get("data", {})
        if not stream_name:
            return []
        asset = stream_name.split("@")[0].upper()

        if "@markPrice" in stream_name:
            required = {"E", "p", "r", "T"}
            if not required.issubset(data):
                return []
            return [{
                "type":            "funding",
                "asset":           asset,
                "timestamp_ms":    int(data["E"]),
                "mark_price":      float(data["p"]),
                "funding_rate":    float(data["r"]),
                "next_funding_ms": int(data["T"]),
            }]

        if "@forceOrder" in stream_name:
            # Liquidation Order Streams payload:
            # {"e":"forceOrder","E":..., "o":{"s":...,"S":...,"p":...,"q":...,"T":...}}
            order = data.get("o", {})
            required = {"s", "S", "p", "q", "T"}
            if not required.issubset(order):
                return []
            return [{
                "type":         "liquidation",
                "asset":        order["s"].upper(),
                "timestamp_ms": int(order["T"]),
                "side":         order["S"].lower(),
                "price":        float(order["p"]),
                "quantity":     float(order["q"]),
            }]

        if "@depth" in stream_name:
            required = {"U", "u", "b", "a"}
            if not required.issubset(data):
                return []
            ts = int(time.time() * 1000)
            # Futures continuity anchor is "pu" (previous final update id),
            # not "U" -- fall back to U if a futures-style message is ever
            # missing it, but pu is what Binance's docs say to chain on.
            first_uid = int(data.get("pu", data["U"]))
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

        # "@trade" falls through to BinanceExchange's handling unchanged --
        # the spot and futures trade payload shapes match.
        return super().parse_message(raw)

    def snapshot_url(self, asset: str) -> str:
        return f"https://fapi.binance.com/fapi/v1/depth?symbol={asset.upper()}&limit=1000"

    def open_interest_url(self, asset: str) -> str:
        # No WebSocket push for raw open interest exists on Binance
        # Futures -- confirmed via Binance's own docs and independently
        # by Tardis.dev (a professional data vendor) REST-polling this
        # exact endpoint themselves roughly every 6 seconds rather than
        # using a push stream, because there isn't one.
        return f"https://fapi.binance.com/fapi/v1/openInterest?symbol={asset.upper()}"

    def parse_open_interest(self, asset: str, raw: dict) -> List[dict]:
        # {"symbol":"BTCUSDT","openInterest":"10659.509","time":1625184323456}
        required = {"openInterest", "time"}
        if not required.issubset(raw):
            return []
        return [{
            "type":                "open_interest",
            "asset":               asset.upper(),
            "timestamp_ms":        int(raw["time"]),
            "open_interest":       float(raw["openInterest"]),
            "open_interest_value": None,  # Binance doesn't provide a USD value here
        }]


# ──────────────────────────────────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────────────────────────────────

_EXCHANGES = {
    "binance": BinanceExchange,
    "binance_futures": BinanceFuturesExchange,
    "coinbase": CoinbaseExchange,
    "okx": OKXExchange,
    "okx_swap": OKXSwapExchange,
    "kraken": KrakenExchange,
    "bybit": BybitExchange,
    "bybit_linear": BybitLinearExchange,
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