"""
Merge many small time-partitioned Parquet files (the files LOBStreamer
writes every flush_interval, 5 minutes by default) into fewer, larger
files at a chosen calendar granularity.

This is a pure file-consolidation operation -- it doesn't reconstruct,
prune, or modify any row of data, it just regroups existing rows by time
window. Useful regardless of whether you ever run book reconstruction
(see reconstruct.py): querying a year of 5-minute files (tens of
thousands of tiny files per exchange/asset) is much slower than querying
the same year compacted into ~12 monthly files, both because each small
file carries its own read overhead and because cloud storage in
particular has real per-file latency on top of that.

Schema evolution: files within one bucket may span a schema change (a
column added in a newer package version, e.g. exchange_ts in 0.9.0).
compact() merges these by filling the missing column with null in the
older rows, rather than raising -- this is what most pipelines actually
want when compacting a window that straddles an upgrade. A genuine type
conflict on a column present in both schemas still raises, as it should.
"""

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Literal

import pyarrow as pa
import pyarrow.parquet as pq

Granularity = Literal["day", "week", "month", "year"]

# Matches the leading "YYYY-MM-DD-HH" that every filename LOBStreamer
# writes starts with (trades/depth/gaps/checksums/funding use exactly
# this; snapshots append minutes/seconds after it, which this pattern
# simply ignores since it isn't anchored to the end of the string).
_FILENAME_TS_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})-(\d{2})")


def _bucket_key(dt: datetime, granularity: Granularity) -> str:
    if granularity == "day":
        return dt.strftime("%Y-%m-%d")
    if granularity == "week":
        iso_year, iso_week, _ = dt.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    if granularity == "month":
        return dt.strftime("%Y-%m")
    if granularity == "year":
        return dt.strftime("%Y")
    raise ValueError(
        f"Unknown granularity '{granularity}'. Use 'day', 'week', 'month', or 'year'."
    )


def compact(
    source_dir: str,
    dest_dir: str,
    granularity: Granularity = "month",
    delete_source: bool = False,
) -> Dict[str, int]:
    """
    Merge every Parquet file directly inside `source_dir` into one file
    per calendar bucket under `dest_dir`.

    Point this at a single (prefix, exchange, asset) leaf directory, e.g.:

        compact("lob_data/depth/binance/BTCUSDT",
                "lob_data_compacted/depth/binance/BTCUSDT",
                granularity="month")

    -> lob_data_compacted/depth/binance/BTCUSDT/2026-06.parquet, etc.

    Parameters
    ----------
    source_dir : directory containing the small input Parquet files.
    dest_dir : directory to write the merged files into. Created if it
        doesn't exist. Safe to be the same tree as source_dir as long as
        it's a different leaf directory -- never point dest_dir at the
        same directory as source_dir.
    granularity : "day", "week", "month", or "year".
    delete_source : if True, deletes each small input file once it has
        been folded into its merged output file. Off by default --
        run once, inspect the output, then re-run with this on (or just
        delete the source tree yourself) once you trust the result.

    Returns
    -------
    Dict mapping each bucket key (e.g. "2026-06") to the number of rows
    written for that bucket. Empty dict if source_dir has no matching files.
    """
    source = Path(source_dir)
    dest = Path(dest_dir)

    if not source.is_dir():
        raise FileNotFoundError(f"source_dir does not exist: {source_dir}")
    dest.mkdir(parents=True, exist_ok=True)

    files = sorted(source.glob("*.parquet"))
    if not files:
        return {}

    buckets: Dict[str, List[Path]] = {}
    skipped: List[Path] = []
    for f in files:
        m = _FILENAME_TS_RE.search(f.stem)
        if not m:
            skipped.append(f)
            continue
        year, month, day, hour = (int(x) for x in m.groups())
        dt = datetime(year, month, day, hour, tzinfo=timezone.utc)
        key = _bucket_key(dt, granularity)
        buckets.setdefault(key, []).append(f)

    summary: Dict[str, int] = {}
    for key, bucket_files in sorted(buckets.items()):
        tables = [pq.read_table(str(f)) for f in bucket_files]
        # promote_options="default" allows merging files whose schemas
        # differ only by column presence -- e.g. older files captured
        # before a field was added (exchange_ts in 0.9.0 is the first
        # real case of this). Missing columns are filled with null rather
        # than raising ArrowInvalid, which is what plain concat_tables()
        # does by default. This is deliberately permissive about *added*
        # columns; it does NOT paper over a genuine type conflict on a
        # column both schemas share (that still raises, as it should).
        merged = pa.concat_tables(tables, promote_options="default")
        out_path = dest / f"{key}.parquet"
        pq.write_table(merged, str(out_path), compression="snappy")
        summary[key] = merged.num_rows

        if delete_source:
            for f in bucket_files:
                f.unlink()

    return summary


def compact_tree(
    source_root: str,
    dest_root: str,
    granularity: Granularity = "month",
    delete_source: bool = False,
) -> Dict[str, Dict[str, int]]:
    """
    Convenience wrapper that walks every {prefix}/{exchange}/{asset} leaf
    directory under source_root (the same layout LOBStreamer writes,
    e.g. an entire output_dir) and compacts each one independently into
    the matching path under dest_root.

    Returns a dict mapping each leaf's relative path (as a string, e.g.
    "depth/binance/BTCUSDT") to that leaf's compact() summary.
    """
    source_root_path = Path(source_root)
    results: Dict[str, Dict[str, int]] = {}

    if not source_root_path.is_dir():
        raise FileNotFoundError(f"source_root does not exist: {source_root}")

    for leaf in sorted(source_root_path.glob("*/*/*")):
        if not leaf.is_dir():
            continue
        if not any(leaf.glob("*.parquet")):
            continue
        rel = leaf.relative_to(source_root_path)
        dest_leaf = Path(dest_root) / rel
        results[str(rel)] = compact(
            str(leaf), str(dest_leaf), granularity=granularity, delete_source=delete_source
        )

    return results