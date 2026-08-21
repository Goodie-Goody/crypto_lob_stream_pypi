import pyarrow as pa

# timestamp_ms vs exchange_ts
# ---------------------------
# timestamp_ms is ALWAYS local receive time -- the moment this process saw
# the message (int(time.time() * 1000)) or, for REST-polled records, the
# moment the response was parsed. It is never populated from an
# exchange-supplied field, and it is present on every schema below
# (including gaps and checksums), so it's always safe to sort/join on.
#
# exchange_ts is the exchange's own event timestamp where one exists
# (e.g. Binance trade "T", Coinbase/Kraken's ISO message time, OKX/Bybit
# "ts"), kept as a *separate*, nullable field. It is None when the
# exchange genuinely doesn't supply one for that message (never silently
# backfilled with local time), and it's only added to schemas where an
# exchange-side event time is meaningful: trades, depth, funding,
# liquidations, and open interest. It is deliberately omitted from
# snapshots (REST/socket snapshots mostly carry no per-level exchange
# time -- Binance's REST snapshot has none at all) and from gaps/
# checksums, which are purely internal detection events with no
# exchange-side timestamp to speak of.
TRADE_SCHEMA = pa.schema([
    ("timestamp_ms", pa.int64()),
    ("exchange_ts",  pa.int64()),
    ("exchange",     pa.string()),
    ("asset",        pa.string()),
    ("trade_id",     pa.int64()),
    ("price",        pa.float64()),
    ("quantity",     pa.float64()),
    ("buyer_maker",  pa.bool_()),
])

DEPTH_SCHEMA = pa.schema([
    ("timestamp_ms",    pa.int64()),
    ("exchange_ts",     pa.int64()),
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
    ("exchange_ts",      pa.int64()),
    ("exchange",         pa.string()),
    ("asset",            pa.string()),
    ("mark_price",       pa.float64()),
    ("funding_rate",     pa.float64()),
    ("next_funding_ms",  pa.int64()),
])

# Futures/perps only. Every liquidation = a forced position closure, the
# clearest direct signal of leveraged positions getting stretched.
LIQUIDATION_SCHEMA = pa.schema([
    ("timestamp_ms", pa.int64()),
    ("exchange_ts",  pa.int64()),
    ("exchange",     pa.string()),
    ("asset",        pa.string()),
    ("side",         pa.string()),    # side of the liquidated position
    ("price",        pa.float64()),
    ("quantity",     pa.float64()),
])

# Futures/perps only. Total outstanding leveraged exposure -- the other
# half of the stress picture alongside liquidations: open_interest is the
# fuel, liquidations are the fire.
OPEN_INTEREST_SCHEMA = pa.schema([
    ("timestamp_ms",         pa.int64()),
    ("exchange_ts",          pa.int64()),
    ("exchange",             pa.string()),
    ("asset",                pa.string()),
    ("open_interest",        pa.float64()),  # in contracts/base currency
    ("open_interest_value",  pa.float64()),  # in quote currency (e.g. USD), where the exchange provides it
])