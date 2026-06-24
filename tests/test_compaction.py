"""Tests for the Parquet file-compaction utility (compaction.py)."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from crypto_lob_stream.compaction import compact, compact_tree


def _write(base: Path, name: str, rows: list):
    p = base / f"{name}.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), str(p))


def test_compact_merges_files_into_monthly_buckets(tmp_path):
    src = tmp_path / "source"
    for hour in ("2026-06-01-00", "2026-06-01-12", "2026-06-15-09"):
        _write(src, hour, [{"x": 1}])
    _write(src, "2026-07-01-00", [{"x": 1}])  # different month

    dest = tmp_path / "dest"
    summary = compact(str(src), str(dest), granularity="month")

    assert summary == {"2026-06": 3, "2026-07": 1}
    assert (dest / "2026-06.parquet").exists()
    assert (dest / "2026-07.parquet").exists()
    assert pq.read_table(str(dest / "2026-06.parquet")).num_rows == 3


def test_compact_merges_files_into_daily_buckets(tmp_path):
    src = tmp_path / "source"
    for hour in ("2026-06-01-00", "2026-06-01-12", "2026-06-01-23"):
        _write(src, hour, [{"x": 1}])
    _write(src, "2026-06-02-00", [{"x": 1}])

    dest = tmp_path / "dest"
    summary = compact(str(src), str(dest), granularity="day")

    assert summary == {"2026-06-01": 3, "2026-06-02": 1}


def test_compact_merges_files_into_yearly_buckets(tmp_path):
    src = tmp_path / "source"
    _write(src, "2026-01-05-00", [{"x": 1}])
    _write(src, "2026-11-20-00", [{"x": 1}])
    _write(src, "2027-01-01-00", [{"x": 1}])

    dest = tmp_path / "dest"
    summary = compact(str(src), str(dest), granularity="year")

    assert summary == {"2026": 2, "2027": 1}


def test_compact_merges_files_into_weekly_buckets(tmp_path):
    src = tmp_path / "source"
    # 2026-06-01 is a Monday -> ISO week 23; 2026-06-08 is the next Monday -> week 24
    _write(src, "2026-06-01-00", [{"x": 1}])
    _write(src, "2026-06-03-00", [{"x": 1}])
    _write(src, "2026-06-08-00", [{"x": 1}])

    dest = tmp_path / "dest"
    summary = compact(str(src), str(dest), granularity="week")

    assert len(summary) == 2
    assert sum(summary.values()) == 3


def test_compact_preserves_data_correctness_not_just_row_count(tmp_path):
    src = tmp_path / "source"
    _write(src, "2026-06-01-00", [{"price": 100.0, "qty": 1.0}])
    _write(src, "2026-06-01-12", [{"price": 101.0, "qty": 2.0}])

    dest = tmp_path / "dest"
    compact(str(src), str(dest), granularity="day")

    rows = pq.read_table(str(dest / "2026-06-01.parquet")).to_pylist()
    assert sorted(r["price"] for r in rows) == [100.0, 101.0]


def test_compact_delete_source_removes_originals_but_default_keeps_them(tmp_path):
    src = tmp_path / "source"
    f1 = src / "2026-06-01-00.parquet"
    _write(src, "2026-06-01-00", [{"x": 1}])
    dest = tmp_path / "dest"

    compact(str(src), str(dest), granularity="day", delete_source=False)
    assert f1.exists()  # default: source untouched

    compact(str(src), str(dest), granularity="day", delete_source=True)
    assert not f1.exists()


def test_compact_skips_unparseable_filenames_without_crashing(tmp_path):
    src = tmp_path / "source"
    src.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{"x": 1}]), str(src / "not-a-timestamp.parquet"))
    _write(src, "2026-06-01-00", [{"x": 2}])

    dest = tmp_path / "dest"
    summary = compact(str(src), str(dest), granularity="day")

    assert summary == {"2026-06-01": 1}  # only the parseable file counted


def test_compact_empty_source_returns_empty_summary(tmp_path):
    src = tmp_path / "empty"
    src.mkdir()
    dest = tmp_path / "dest"
    assert compact(str(src), str(dest)) == {}


def test_compact_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        compact(str(tmp_path / "nope"), str(tmp_path / "dest"))


def test_compact_invalid_granularity_raises(tmp_path):
    src = tmp_path / "source"
    _write(src, "2026-06-01-00", [{"x": 1}])
    with pytest.raises(ValueError, match="Unknown granularity"):
        compact(str(src), str(tmp_path / "dest"), granularity="fortnight")


# ── compact_tree: whole output_dir at once ──────────────────────────────────

def test_compact_tree_walks_exchange_asset_leaves(tmp_path):
    src_root = tmp_path / "lob_data"
    _write(src_root / "trades" / "binance" / "BTCUSDT", "2026-06-01-00", [{"x": 1}])
    _write(src_root / "trades" / "binance" / "BTCUSDT", "2026-06-01-01", [{"x": 1}])
    _write(src_root / "depth" / "kraken" / "BTC-USD", "2026-06-01-00", [{"x": 1}])

    dest_root = tmp_path / "lob_data_compacted"
    results = compact_tree(str(src_root), str(dest_root), granularity="day")

    assert "trades/binance/BTCUSDT" in results
    assert results["trades/binance/BTCUSDT"] == {"2026-06-01": 2}
    assert "depth/kraken/BTC-USD" in results
    assert (dest_root / "trades" / "binance" / "BTCUSDT" / "2026-06-01.parquet").exists()


def test_compact_tree_missing_root_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        compact_tree(str(tmp_path / "nope"), str(tmp_path / "dest"))
        