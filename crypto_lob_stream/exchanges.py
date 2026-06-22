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
# Registry
# ──────────────────────────────────────────────────────────────────────────

_EXCHANGES = {
    "binance": BinanceExchange,
    "coinbase": CoinbaseExchange,
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

