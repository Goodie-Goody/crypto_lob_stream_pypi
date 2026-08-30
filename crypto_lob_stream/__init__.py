from .streamer import LOBStreamer
from .exchanges import available_exchanges, get_exchange
from .reconstruct import BookReconstructor, reconstruct, DEFAULT_MAX_DEPTH
from .compaction import compact, compact_tree
from .audit import audit_coverage, audit_file, CoverageResult, summarize

__all__ = [
    "LOBStreamer",
    "available_exchanges",
    "get_exchange",
    "BookReconstructor",
    "reconstruct",
    "DEFAULT_MAX_DEPTH",
    "compact",
    "compact_tree",
    "audit_coverage",
    "audit_file",
    "CoverageResult",
    "summarize",
]

from importlib.metadata import version as _pkg_version, PackageNotFoundError as _PkgNotFoundError

try:
    __version__ = _pkg_version("crypto-lob-stream")
except _PkgNotFoundError:
    __version__ = "unknown"