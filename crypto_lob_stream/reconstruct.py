"""
Reconstruct a live book from a snapshot and a stream of depth diffs.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import warnings

import pyarrow.parquet as pq

DOCS_URL = "https://github.com/Goodie-Goody/crypto_lob_stream_pypi#lob-reconstruction"

# Sensible default max_depth per exchange, matching what each exchange's
# subscription actually delivers (see README "Known limitations" for the
# reasoning). These fall into two genuinely different categories:
#
#   - Exchange-enforced hard ceilings: kraken (25) and bybit/bybit_linear
#     (50) are subscription parameters -- the exchange will never send a
#     level beyond that number, full stop.
#   - OKX/OKX swap's "books" channel is also a fixed, exchange-defined
#     ceiling (400), confirmed via OKX's own docs -- but it's not
#     something *we* chose, it's just what that channel is. Previously
#     mislabeled None ("unbounded") here, which was wrong: 400 is a real,
#     known number, not an absence of one.
#   - binance/binance_futures' 1000 is the opposite case: a depth *we*
#     requested in our REST snapshot call, not an exchange-enforced wall.
#     The live diff stream itself is not capped at 1000 -- without
#     pruning, the book can organically drift past 1000 over time, which
#     is the exact setup of the external 25-hour test referenced in the
#     README ("LOB reconstruction").
#
# None means "no known ceiling, don't prune by default" -- currently only
# true for coinbase, where no fixed maximum is documented anywhere.
DEFAULT_MAX_DEPTH: Dict[str, Optional[int]] = {
    "kraken": 25,
    "bybit": 50,
    "bybit_linear": 50,
    "binance": 1000,
    "binance_futures": 1000,
    "okx": 400,
    "okx_swap": 400,
    "coinbase": None,
}


def _format_depth_table() -> str:
    """Human-readable one-liner of DEFAULT_MAX_DEPTH, used in the
    over-request warning so the listed depths can never drift out of
    sync with the actual table."""
    parts = [
        f"{exch}={depth if depth is not None else 'unbounded'}"
        for exch, depth in DEFAULT_MAX_DEPTH.items()
    ]
    return ", ".join(parts)


class BookReconstructor:
    """
    Replays a snapshot and a stream of depth-diff rows into a live,
    correctly-pruned book. Pure in-memory state machine, no I/O -- use
    this directly if you already have snapshot/diff rows from your own
    query. Use reconstruct() below for the common case of reading
    straight from a LOBStreamer output directory.
    """

    def __init__(self, max_depth: Optional[int] = None, exchange: Optional[str] = None):
        self.max_depth = max_depth
        # Purely informational -- used only to make the over-request
        # warning in top() name the actual exchange. Not required when
        # using BookReconstructor directly with your own rows.
        self.exchange = exchange
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

    def load_snapshot(self, rows) -> None:
        """rows: iterable of dict-like rows with 'side', 'price', 'quantity'."""
        self.bids.clear()
        self.asks.clear()
        for r in rows:
            book = self.bids if r["side"] == "bid" else self.asks
            book[float(r["price"])] = float(r["quantity"])
        self._prune()

    def apply_diff(self, side: str, price: float, quantity: float) -> None:
        """Apply a single price-level update. quantity is the level's new
        total size (an absolute replacement), not a delta -- quantity 0
        means the exchange explicitly removed this exact price level."""
        book = self.bids if side == "bid" else self.asks
        if quantity == 0:
            book.pop(price, None)
        else:
            book[price] = quantity

    def apply_diffs(self, rows) -> None:
        """Apply a batch of diff rows (already sorted by last_update_id),
        then prune once at the end of the batch -- pruning after every
        single row instead of once per batch is unnecessary work and
        produces the identical final result either way."""
        for r in rows:
            self.apply_diff(r["side"], float(r["price"]), float(r["quantity"]))
        self._prune()

    def _prune(self) -> None:
        if self.max_depth is None:
            return
        if len(self.bids) > self.max_depth:
            top = sorted(self.bids.items(), reverse=True)[: self.max_depth]
            self.bids = dict(top)
        if len(self.asks) > self.max_depth:
            top = sorted(self.asks.items())[: self.max_depth]
            self.asks = dict(top)

    def top(
        self, n: Optional[int] = None
    ) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Return (bids, asks) as (price, quantity) pairs -- bids sorted
        highest price first, asks sorted lowest price first -- optionally
        limited to the top n levels per side.

        If n exceeds max_depth, a UserWarning is raised (not an exception
        -- this still returns the best max_depth levels available, since
        that's the most useful thing to do) explaining that the requested
        depth was never captured for this exchange, rather than silently
        handing back fewer rows than asked for with no explanation."""
        if n is not None and self.max_depth is not None and n > self.max_depth:
            exch = self.exchange or "this exchange"
            warnings.warn(
                f"Requested top {n} levels per side, but {exch} data was only "
                f"ever captured to depth {self.max_depth} -- returning "
                f"{self.max_depth} levels instead, not {n}. "
                f"Captured depth per exchange: {_format_depth_table()}. "
                f"See {DOCS_URL} for details.",
                UserWarning,
                stacklevel=2,
            )
        bids = sorted(self.bids.items(), reverse=True)
        asks = sorted(self.asks.items())
        if n is not None:
            bids = bids[:n]
            asks = asks[:n]
        return bids, asks


def reconstruct(
    output_dir: str,
    exchange: str,
    asset: str,
    max_depth=...,
) -> BookReconstructor:
    """
    Load the most recent snapshot and every depth diff after it from a
    LOBStreamer output directory (the same output_dir/exchange/asset you
    streamed into), and replay them into a correctly pruned
    BookReconstructor reflecting the book at the end of that data.

    Parameters
    ----------
    output_dir : the same output_dir you passed to LOBStreamer.
    exchange, asset : which captured stream to reconstruct, e.g.
        "binance", "BTCUSDT".
    max_depth : number of price levels to retain per side after each
        update. Defaults to DEFAULT_MAX_DEPTH for the given exchange.
        Pass None explicitly to disable pruning -- not recommended; see
        the module docstring for why. Requesting a depth deeper than what
        was actually subscribed to (e.g. >25 for kraken, >50 for
        bybit/bybit_linear) can't recover data that was never captured;
        it just returns whatever depth was actually available.

    Raises
    ------
    FileNotFoundError if no snapshot exists yet for that exchange/asset
    (LOBStreamer writes one on every connect, so this should only happen
    if it was never run, or output_dir/exchange/asset is wrong).
    """
    if max_depth is ...:
        max_depth = DEFAULT_MAX_DEPTH.get(exchange)

    base = Path(output_dir)
    snapshot_dir = base / "snapshots" / exchange / asset
    depth_dir = base / "depth" / exchange / asset

    snapshot_files = sorted(snapshot_dir.glob("*.parquet"))
    if not snapshot_files:
        raise FileNotFoundError(
            f"No snapshot files found in {snapshot_dir}. "
            f"Reconstruction needs at least one snapshot as a starting anchor."
        )

    # Snapshot filenames sort chronologically (YYYY-MM-DD-HHmmss), so the
    # last one is the most recent snapshot -- the anchor we replay diffs
    # forward from.
    snapshot_table = pq.read_table(str(snapshot_files[-1]))
    snapshot_rows = snapshot_table.to_pylist()
    if not snapshot_rows:
        raise ValueError(f"Snapshot file {snapshot_files[-1]} is empty.")
    snapshot_anchor_id = snapshot_rows[0]["last_update_id"]

    book = BookReconstructor(max_depth=max_depth, exchange=exchange)
    book.load_snapshot(snapshot_rows)

    depth_files = sorted(depth_dir.glob("*.parquet"))
    for f in depth_files:
        table = pq.read_table(str(f))
        rows = [
            r for r in table.to_pylist() if r["last_update_id"] > snapshot_anchor_id
        ]
        rows.sort(key=lambda r: r["last_update_id"])
        if rows:
            book.apply_diffs(rows)

    return book