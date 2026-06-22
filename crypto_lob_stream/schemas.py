import pyarrow as pa

TRADE_SCHEMA = pa.schema([
    ("timestamp_ms", pa.int64()),
    ("exchange",     pa.string()),
    ("asset",        pa.string()),
    ("trade_id",     pa.int64()),
    ("price",        pa.float64()),
    ("quantity",     pa.float64()),
    ("buyer_maker",  pa.bool_()),
])

DEPTH_SCHEMA = pa.schema([
    ("timestamp_ms",    pa.int64()),
    ("exchange",        pa.string()),
    ("asset",           pa.string()),
    ("side",            pa.string()),
    ("price",           pa.float64()),
    ("quantity",        pa.float64()),
    ("first_update_id", pa.int64()),
    ("last_update_id",  pa.int64()),
])

SNAPSHOT_SCHEMA = pa.schema([
    ("timestamp_ms",   pa.int64()),
    ("exchange",       pa.string()),
    ("asset",          pa.string()),
    ("side",           pa.string()),
    ("price",          pa.float64()),
    ("quantity",       pa.float64()),
    ("last_update_id", pa.int64()),
])

# Emitted by LOBStreamer's gap detector (see streamer.py) when a depth
# message's first_update_id doesn't chain from the previous last_update_id.
# Only meaningful for exchanges with real sequence numbers
# (Exchange.has_sequence_ids = True); see README for which exchanges that
# covers today.
GAP_SCHEMA = pa.schema([
    ("timestamp_ms",       pa.int64()),
    ("exchange",           pa.string()),
    ("asset",              pa.string()),
    ("expected_update_id", pa.int64()),
    ("received_update_id", pa.int64()),
    ("gap_size",           pa.int64()),
])

# Emitted when verify_checksums=True and a live-maintained book mirror's
# CRC32 disagrees with the exchange-supplied checksum. Only successful
# *mismatches* are written here (matches are not persisted, to avoid
# writing a row for every single update); see KrakenExchange in
# exchanges.py for the checksum implementation.
CHECKSUM_SCHEMA = pa.schema([
    ("timestamp_ms", pa.int64()),
    ("exchange",     pa.string()),
    ("asset",        pa.string()),
    ("expected",     pa.int64()),
    ("received",     pa.int64()),
])

# Futures/perps only (currently BinanceFuturesExchange's markPrice stream).
FUNDING_SCHEMA = pa.schema([
    ("timestamp_ms",     pa.int64()),
    ("exchange",         pa.string()),
    ("asset",            pa.string()),
    ("mark_price",       pa.float64()),
    ("funding_rate",     pa.float64()),
    ("next_funding_ms",  pa.int64()),
])