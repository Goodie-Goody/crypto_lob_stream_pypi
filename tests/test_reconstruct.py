"""
Tests for the offline reconstruction module (reconstruct.py).

test_naive_replay_accumulates_ghost_levels and
test_pruned_replay_matches_real_top_n below directly reproduce the
failure mode described in Oliver Zehentleitner's article "Your Binance
L2 Order Book Can Be Gap-Free and Still Be Wrong" (dev.to) -- a price
level drifting outside a tracked depth window without ever receiving an
explicit quantity=0 removal, and the fix (active pruning after every
update) actually closing that gap. The rest of the file covers the more
mechanical behavior (snapshot loading, file I/O via reconstruct(), the
DEFAULT_MAX_DEPTH table).
"""

import json
import warnings
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_lob_stream.reconstruct import (
    DEFAULT_MAX_DEPTH,
    BookReconstructor,
    reconstruct,
)
from crypto_lob_stream.schemas import DEPTH_SCHEMA, SNAPSHOT_SCHEMA


# ── Core replay mechanics ───────────────────────────────────────────────────

def test_load_snapshot_populates_book():
    book = BookReconstructor()
    book.load_snapshot([
        {"side": "bid", "price": 100.0, "quantity": 1.0},
        {"side": "bid", "price": 99.0, "quantity": 2.0},
        {"side": "ask", "price": 101.0, "quantity": 1.5},
    ])
    bids, asks = book.top()
    assert bids == [(100.0, 1.0), (99.0, 2.0)]
    assert asks == [(101.0, 1.5)]


def test_apply_diff_quantity_zero_removes_level():
    book = BookReconstructor()
    book.load_snapshot([{"side": "bid", "price": 100.0, "quantity": 1.0}])
    book.apply_diff("bid", 100.0, 0.0)
    assert book.bids == {}


def test_apply_diff_replaces_not_subtracts():
    # quantity is an absolute replacement, not a delta -- this is the
    # one part of the mental model worth getting exactly right.
    book = BookReconstructor()
    book.load_snapshot([{"side": "bid", "price": 100.0, "quantity": 5.0}])
    book.apply_diff("bid", 100.0, 1.0)
    assert book.bids[100.0] == 1.0  # not 5.0 - 1.0 = 4.0


def test_top_n_respects_max_depth_already_applied():
    book = BookReconstructor(max_depth=2)
    book.load_snapshot([
        {"side": "bid", "price": p, "quantity": 1.0} for p in (100.0, 99.0, 98.0)
    ])
    bids, _ = book.top()
    assert len(bids) == 2
    assert bids == [(100.0, 1.0), (99.0, 1.0)]  # kept the two best, not worst


# ── The actual bug, reproduced and fixed ────────────────────────────────────

def test_naive_replay_accumulates_ghost_levels():
    """
    No pruning (max_depth=None): a price level that drifts out of the
    intended top-3 window by new orders landing closer to the market
    never gets removed, because nothing ever sends an explicit
    quantity=0 for it. This is the bug, reproduced directly.
    """
    book = BookReconstructor(max_depth=None)
    book.load_snapshot([
        {"side": "bid", "price": p, "quantity": 1.0} for p in (100.0, 99.0, 98.0)
    ])

    # New orders land closer to the market every round, pushing 98.0
    # further and further from the top -- but 98.0 itself never changes
    # and is never explicitly zeroed.
    for new_price in (99.1, 99.2, 99.3, 99.4, 99.5):
        book.apply_diffs([{"side": "bid", "price": new_price, "quantity": 1.0}])

    # 98.0 is still sitting in memory, even though no real top-3 view of
    # this book would ever include it any more.
    assert 98.0 in book.bids
    assert len(book.bids) == 8  # 3 original + 5 new = ghost levels piling up


def test_pruned_replay_does_not_accumulate_ghost_levels():
    """Same exact update sequence as above, just with max_depth=3 set --
    the fix is solely the active pruning, nothing else changes."""
    book = BookReconstructor(max_depth=3)
    book.load_snapshot([
        {"side": "bid", "price": p, "quantity": 1.0} for p in (100.0, 99.0, 98.0)
    ])

    for new_price in (99.1, 99.2, 99.3, 99.4, 99.5):
        book.apply_diffs([{"side": "bid", "price": new_price, "quantity": 1.0}])

    assert 98.0 not in book.bids  # correctly dropped once it fell out of top 3
    assert len(book.bids) == 3
    bids, _ = book.top()
    assert [p for p, _ in bids] == [100.0, 99.5, 99.4]  # the real top 3


def test_pruned_replay_matches_naive_replay_within_the_window():
    """Pruning never changes anything *inside* the tracked depth -- it
    only ever removes levels that have fallen outside it. The top of the
    book (what actually matters for spread/best-bid-ask) is identical
    whether or not pruning is enabled."""
    naive = BookReconstructor(max_depth=None)
    pruned = BookReconstructor(max_depth=2)
    snapshot = [{"side": "bid", "price": p, "quantity": 1.0} for p in (100.0, 99.0, 98.0)]
    naive.load_snapshot(snapshot)
    pruned.load_snapshot(snapshot)

    diffs = [{"side": "bid", "price": 100.0, "quantity": 0.5}]
    naive.apply_diffs(diffs)
    pruned.apply_diffs(diffs)

    naive_top, _ = naive.top(n=2)
    pruned_top, _ = pruned.top(n=2)
    assert naive_top == pruned_top == [(100.0, 0.5), (99.0, 1.0)]


# ── DEFAULT_MAX_DEPTH table ──────────────────────────────────────────────────

def test_default_max_depth_matches_known_subscribed_depths():
    # These mirror the exact depths subscribed in exchanges.py -- if one
    # of these ever changes there, it should change here too.
    assert DEFAULT_MAX_DEPTH["kraken"] == 25
    assert DEFAULT_MAX_DEPTH["bybit"] == 50
    assert DEFAULT_MAX_DEPTH["bybit_linear"] == 50
    # okx/okx_swap's "books" channel has a real, fixed, exchange-defined
    # ceiling of 400 -- confirmed via OKX's own docs. This is NOT the
    # same situation as coinbase below; it's a known number, not an
    # absence of one.
    assert DEFAULT_MAX_DEPTH["okx"] == 400
    assert DEFAULT_MAX_DEPTH["okx_swap"] == 400
    # coinbase is the one exchange with no documented ceiling at all.
    assert DEFAULT_MAX_DEPTH["coinbase"] is None


# ── Over-request warning ─────────────────────────────────────────────────────

def test_top_warns_when_n_exceeds_max_depth():
    book = BookReconstructor(max_depth=25, exchange="kraken")
    book.load_snapshot([
        {"side": "bid", "price": float(p), "quantity": 1.0} for p in range(100, 70, -1)
    ])
    with pytest.warns(UserWarning, match="kraken data was only ever captured to depth 25"):
        bids, _ = book.top(n=200)
    assert len(bids) == 25  # still returns the best it has, doesn't error


def test_top_warning_names_exchange_when_known():
    book = BookReconstructor(max_depth=10, exchange="bybit")
    book.load_snapshot([{"side": "bid", "price": 1.0, "quantity": 1.0}])
    with pytest.warns(UserWarning, match="bybit data"):
        book.top(n=999)


def test_top_warning_falls_back_gracefully_when_exchange_unknown():
    # BookReconstructor used directly (not via reconstruct()) has no
    # exchange name -- the warning should still fire, just with generic
    # wording instead of crashing on a missing name.
    book = BookReconstructor(max_depth=10)
    book.load_snapshot([{"side": "bid", "price": 1.0, "quantity": 1.0}])
    with pytest.warns(UserWarning, match="this exchange data"):
        book.top(n=999)


def test_top_warning_lists_all_exchange_depths():
    book = BookReconstructor(max_depth=25, exchange="kraken")
    book.load_snapshot([{"side": "bid", "price": 1.0, "quantity": 1.0}])
    with pytest.warns(UserWarning, match=r"bybit=50.*binance=1000.*okx=400"):
        book.top(n=999)


def test_top_no_warning_when_n_within_max_depth():
    book = BookReconstructor(max_depth=25, exchange="kraken")
    book.load_snapshot([
        {"side": "bid", "price": float(p), "quantity": 1.0} for p in range(100, 70, -1)
    ])
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any warning here fails the test
        book.top(n=10)  # within max_depth, should be silent


def test_top_no_warning_when_max_depth_unset():
    # max_depth=None means "no pruning configured" -- there's no ceiling
    # to have exceeded, so no warning makes sense regardless of n.
    # coinbase used here specifically because it's the one exchange
    # whose *real* default (via reconstruct()) is also None.
    book = BookReconstructor(max_depth=None, exchange="coinbase")
    book.load_snapshot([{"side": "bid", "price": 1.0, "quantity": 1.0}])
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        book.top(n=999999)


def test_reconstruct_passes_exchange_name_to_book(tmp_path):
    _write_snapshot(tmp_path, "kraken", "BTC/USD", "2026-06-01-000000", [
        {"timestamp_ms": 1, "exchange": "kraken", "asset": "BTC/USD",
         "side": "bid", "price": 100.0, "quantity": 1.0, "last_update_id": 10},
    ])
    book = reconstruct(str(tmp_path), "kraken", "BTC/USD")
    assert book.exchange == "kraken"
    with pytest.warns(UserWarning, match="kraken data"):
        book.top(n=999)


# ── File-based reconstruct() against a real LOBStreamer-shaped layout ───────

def _write_snapshot(base: Path, exchange: str, asset: str, ts_str: str, rows: list):
    out = base / "snapshots" / exchange / asset / f"{ts_str}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SNAPSHOT_SCHEMA), str(out))


def _write_depth(base: Path, exchange: str, asset: str, ts_str: str, rows: list):
    out = base / "depth" / exchange / asset / f"{ts_str}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=DEPTH_SCHEMA), str(out))


def test_reconstruct_reads_real_files_and_applies_default_depth(tmp_path):
    _write_snapshot(tmp_path, "kraken", "BTC/USD", "2026-06-01-000000", [
        {"timestamp_ms": 1, "exchange": "kraken", "asset": "BTC/USD",
         "side": "bid", "price": 100.0, "quantity": 1.0, "last_update_id": 10},
        {"timestamp_ms": 1, "exchange": "kraken", "asset": "BTC/USD",
         "side": "ask", "price": 101.0, "quantity": 1.0, "last_update_id": 10},
    ])
    _write_depth(tmp_path, "kraken", "BTC/USD", "2026-06-01-01", [
        {"timestamp_ms": 2, "exchange": "kraken", "asset": "BTC/USD",
         "side": "bid", "price": 99.5, "quantity": 0.5,
         "first_update_id": 11, "last_update_id": 11},
        {"timestamp_ms": 3, "exchange": "kraken", "asset": "BTC/USD",
         "side": "bid", "price": 100.0, "quantity": 0.0,  # explicit removal
         "first_update_id": 12, "last_update_id": 12},
    ])

    book = reconstruct(str(tmp_path), "kraken", "BTC/USD")
    assert book.max_depth == 25  # kraken's default applied automatically
    bids, asks = book.top()
    assert bids == [(99.5, 0.5)]  # 100.0 explicitly zeroed, 99.5 added
    assert asks == [(101.0, 1.0)]


def test_reconstruct_ignores_diffs_at_or_before_snapshot_anchor(tmp_path):
    # A diff file can legitimately contain rows from before the snapshot
    # was taken (e.g. it was flushed in the same hour) -- those must be
    # skipped, not double-applied on top of a snapshot that already
    # reflects them.
    _write_snapshot(tmp_path, "binance", "BTCUSDT", "2026-06-01-000000", [
        {"timestamp_ms": 1, "exchange": "binance", "asset": "BTCUSDT",
         "side": "bid", "price": 100.0, "quantity": 1.0, "last_update_id": 50},
    ])
    _write_depth(tmp_path, "binance", "BTCUSDT", "2026-06-01-00", [
        {"timestamp_ms": 0, "exchange": "binance", "asset": "BTCUSDT",
         "side": "bid", "price": 100.0, "quantity": 999.0,  # stale, pre-snapshot
         "first_update_id": 10, "last_update_id": 10},
        {"timestamp_ms": 5, "exchange": "binance", "asset": "BTCUSDT",
         "side": "bid", "price": 100.0, "quantity": 2.0,  # real, post-snapshot
         "first_update_id": 51, "last_update_id": 51},
    ])

    book = reconstruct(str(tmp_path), "binance", "BTCUSDT")
    bids, _ = book.top()
    assert bids == [(100.0, 2.0)]  # not 999.0


def test_reconstruct_raises_clearly_when_no_snapshot_exists(tmp_path):
    with pytest.raises(FileNotFoundError, match="No snapshot files found"):
        reconstruct(str(tmp_path), "binance", "BTCUSDT")
        