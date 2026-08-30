"""
Scans existing captured Parquet files for suspicious timestamp
clustering -- the signature the ts_str-overwrite bug (fixed in 0.9.2)
would have left behind: a file whose timestamp_ms values span only a
narrow band near the end of its naming window, rather than the full
window, because everything captured before the last flush of that
window was silently overwritten before this fix existed.

This isn't specific to that one bug, though -- narrow timestamp spans
are also the right signature for any future regression of the same
shape, or a genuinely misbehaving capture process, so it's worth running
as an ongoing spot-check, not just a one-time post-mortem.

    from crypto_lob_stream import audit_coverage
    results = audit_coverage("./lob_data/trades/binance/BTCUSDT")
    for r in results:
        if r.suspicious:
            print(r)
"""
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pyarrow.parquet as pq


@dataclass
class CoverageResult:
    path: str
    num_rows: int
    span_seconds: float
    window_seconds: float
    coverage_pct: float
    suspicious: bool

    def __str__(self):
        flag = " <-- SUSPICIOUS" if self.suspicious else ""
        return (
            f"{self.path}: {self.num_rows:,} rows, "
            f"{self.span_seconds:.0f}s span ({self.coverage_pct:.1f}% of "
            f"{self.window_seconds:.0f}s window){flag}"
        )


def audit_file(path: str, window_seconds: float = 3600, threshold_pct: float = 50.0) -> Optional[CoverageResult]:
    """Check one Parquet file's timestamp_ms coverage against its
    expected naming window. Returns None for an empty file (nothing to
    assess).

    window_seconds must match what a HEALTHY file is actually expected
    to cover, or this produces noisy, meaningless results:
      - Raw per-flush files (freshly written by LOBStreamer, one file
        per flush_interval as of 0.9.2): pass flush_interval here, e.g.
        60, not the default 3600. Each one legitimately only covers a
        single flush's worth of time, by design.
      - Compacted files (after compact()/compact_tree(), one file per
        day/week/month/year bucket): pass the compacted granularity in
        seconds, e.g. 86400 for "day". These genuinely are expected to
        span close to their full bucket, so the default assumption
        fits here.
      - Legacy hour-named files from before the 0.9.2 ts_str fix: the
        default 3600 is exactly right -- this is what the diagnostic
        was originally built to check.
    Using the default 3600 against fresh per-flush files will flag
    almost everything as "suspicious" even when nothing is wrong, since
    a single flush's worth of data will always look narrow next to a
    full hour.

    threshold_pct: coverage below this percentage is flagged suspicious.
    50% is a deliberately loose default -- genuine gaps, exchange
    downtime, or a process restart mid-window can all legitimately
    produce partial coverage. This is meant to catch a systematic
    problem (every file in a directory clustered near 5-10%), not to
    flag every individual short window as an error on its own.
    """
    table = pq.read_table(path, columns=["timestamp_ms"])
    if table.num_rows == 0:
        return None

    ts = table.column("timestamp_ms").to_pylist()
    span_seconds = (max(ts) - min(ts)) / 1000
    coverage_pct = span_seconds / window_seconds * 100

    return CoverageResult(
        path=str(path),
        num_rows=table.num_rows,
        span_seconds=span_seconds,
        window_seconds=window_seconds,
        coverage_pct=coverage_pct,
        suspicious=coverage_pct < threshold_pct,
    )


def audit_coverage(
    directory: str,
    window_seconds: float = 3600,
    threshold_pct: float = 50.0,
) -> List[CoverageResult]:
    """Audit every .parquet file under `directory` (recursive). Returns
    one CoverageResult per non-empty file, in the order found.

    Point this at any level of the tree -- a single (prefix, exchange,
    asset) leaf, a whole exchange, or the whole output directory -- it
    just walks everything it finds.
    """
    root = Path(directory)
    if not root.exists():
        raise FileNotFoundError(f"directory does not exist: {directory}")

    results = []
    for f in sorted(root.rglob("*.parquet")):
        r = audit_file(str(f), window_seconds=window_seconds, threshold_pct=threshold_pct)
        if r is not None:
            results.append(r)
    return results


def summarize(results: List[CoverageResult]) -> str:
    """One-line-per-file summary plus an overall suspicious-fraction
    line -- convenient for a quick CLI-style report."""
    if not results:
        return "No files found."
    lines = [str(r) for r in results]
    n_suspicious = sum(1 for r in results if r.suspicious)
    lines.append(
        f"\n{n_suspicious}/{len(results)} file(s) flagged suspicious "
        f"({n_suspicious / len(results) * 100:.1f}%)"
    )
    return "\n".join(lines)