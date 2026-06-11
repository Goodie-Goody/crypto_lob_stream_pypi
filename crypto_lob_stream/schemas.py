import pyarrow as pa

TRADE_SCHEMA = pa.schema([
    ("timestamp_ms", pa.int64()),
    ("asset",        pa.string()),
    ("trade_id",     pa.int64()),
    ("price",        pa.float64()),
    ("quantity",     pa.float64()),
    ("buyer_maker",  pa.bool_()),
])

DEPTH_SCHEMA = pa.schema([
    ("timestamp_ms",    pa.int64()),
    ("asset",           pa.string()),
    ("side",            pa.string()),
    ("price",           pa.float64()),
    ("quantity",        pa.float64()),
    ("first_update_id", pa.int64()),
    ("last_update_id",  pa.int64()),
])

SNAPSHOT_SCHEMA = pa.schema([
    ("timestamp_ms",   pa.int64()),
    ("asset",          pa.string()),
    ("side",           pa.string()),
    ("price",          pa.float64()),
    ("quantity",       pa.float64()),
    ("last_update_id", pa.int64()),
])
