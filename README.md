# crypto-lob-stream

Stream Level 2 order book, trade, and funding-rate data from major crypto exchanges (spot and perpetual futures) to local disk or Google Cloud Storage, in analysis-ready Parquet.

**Supported exchanges:** Binance, Binance USDⓢ-M Futures, Coinbase, OKX, OKX swap, Kraken, Bybit, Bybit linear.

## Why this exists

High-quality, granular limit order book data is one of the biggest barriers to entry in high-frequency and microstructure research. For most markets the full depth-of-book feed is behind a paywall, licensed, time-limited, or never released publicly at all, which puts serious order-book research out of reach for many independent researchers, students, and small teams that cannot afford the resources required to surmount these challenges.

I experienced it as well, which is where the motivation to build this came from.

The focus on crypto is due to its unique exception compared to other asset classes: the major crypto exchanges publish full L2 order book and trade data over free, public WebSocket feeds. This package puts a single, consistent interface in front of several of them, so anyone can collect continuous, fully reconstructable order book data with one command (or several, concurrently, in one process) and store it in an open format. The goal is simple: lower the data barrier for high-frequency and market-microstructure research, at least in the one asset class where the raw feeds are genuinely open.

As a contribution back to the community, monthly snapshots of order book, trade, and futures data across all supported exchanges are published freely on [Hugging Face](https://huggingface.co/).

---

## Install

```bash
# Local output only
pip install crypto-lob-stream

# With Google Cloud Storage support
pip install crypto-lob-stream[gcs]
```

Requires Python 3.10+.

**Windows users:** if you see a warning that the `Scripts` directory is not on PATH, add it via System Settings > Environment Variables > Path. Until then, run with `python -m crypto_lob_stream.cli` instead of `crypto-lob-stream`.

---

## Quickstart

### Python

```python
from crypto_lob_stream import LOBStreamer

streamer = LOBStreamer(
    assets=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    exchange="binance",
    output="local",
    output_dir="./lob_data",
)
streamer.run()
```

### CLI

```bash
crypto-lob-stream --assets BTCUSDT,ETHUSDT --output-dir ./data
```

Press Ctrl+C to stop cleanly. Parquet files start landing within a few minutes.

### Real-time callbacks

Hook into every trade, depth, gap, or checksum-failure event as it arrives, for live monitoring or piping into your own sink:

```python
def on_trade(record):
    print(f"{record['exchange']} {record['asset']} {record['price']} x {record['quantity']}")

def on_gap(record):
    print(f"GAP on {record['exchange']}/{record['asset']}: missed {record['gap_size']} updates")

streamer = LOBStreamer(
    assets=["BTCUSDT"],
    exchange="binance",
    on_trade=on_trade,
    on_gap=on_gap,
)
streamer.run()
```

See "Supported exchanges & features" below for symbol formats, futures/perps, and running multiple exchanges concurrently.

---

## Supported exchanges & features

| Exchange | `exchange=` | Type | Symbol format | Snapshot source | Gap detection | Checksum verification |
|---|---|---|---|---|---|---|
| Binance | `binance` | Spot | `BTCUSDT` | REST | Yes | No |
| Binance USDⓢ-M Futures | `binance_futures` | Perpetual | `BTCUSDT` | REST | Yes | No |
| Coinbase | `coinbase` | Spot | `BTC-USD` | WebSocket | No | No |
| OKX | `okx` | Spot | `BTC-USDT` | WebSocket | Yes | No¹ |
| OKX swap | `okx_swap` | Perpetual | `BTC-USDT` | WebSocket | Yes | No¹ |
| Kraken | `kraken` | Spot | `BTC/USD` | WebSocket | No | Yes |
| Bybit | `bybit` | Spot | `BTCUSDT` | WebSocket | Yes | No |
| Bybit linear | `bybit_linear` | Perpetual | `BTCUSDT` | WebSocket | Yes | No |

¹ OKX is deprecating its `books`-channel checksum in favor of `seqId`/`prevSeqId` validation, which this package already implements as gap detection for `okx`/`okx_swap` — see "Data integrity" below.

Symbols are normalised per exchange, so `BTCUSDT`, `BTC-USDT`, and `BTC/USDT` all resolve to the correct native form for whichever exchange you choose. `okx_swap` and `bybit_linear` automatically format the perpetual-contract symbol (`BTC-USDT-SWAP`, plain `BTCUSDT`) from whatever you pass in.

### Picking an exchange

```python
LOBStreamer(assets=["BTC-USD"], exchange="coinbase", output_dir="./data").run()
LOBStreamer(assets=["BTC-USDT"], exchange="okx", output_dir="./data").run()
LOBStreamer(assets=["BTC/USD"], exchange="kraken", output_dir="./data").run()
LOBStreamer(assets=["BTCUSDT"], exchange="bybit", output_dir="./data").run()
```

### Futures / perpetuals

Three adapters cover USDT-margined (or USDⓢ-M) perpetual contracts: `binance_futures`, `okx_swap`, `bybit_linear`. Each adds three Parquet tables alongside the usual `trades/`/`depth/` ones: `funding/` (mark price, funding rate, next funding time), `liquidations/` (forced position closures), and `open_interest/` (total outstanding leveraged exposure) — together, the standard set for studying leverage and liquidity stress:

```python
LOBStreamer(assets=["BTCUSDT"], exchange="binance_futures", output_dir="./futures_data").run()
```

### Multiple exchanges in one process

Run several exchanges — spot, perpetual, or a mix — concurrently inside a single process and event loop, instead of launching one process per exchange:

```python
streamer = LOBStreamer(
    exchanges=[
        {"exchange": "binance", "assets": ["BTCUSDT", "ETHUSDT"]},
        {"exchange": "kraken", "assets": ["BTC/USD"]},
        {"exchange": "binance_futures", "assets": ["BTCUSDT"]},
    ],
    output_dir="./lob_data",
)
streamer.run()
```

Each exchange keeps its own independent WebSocket connection and reconnect loop — one exchange having connection trouble doesn't affect the others. Output is identical in shape to running each exchange as a separate single-exchange process; this is purely about process count, not data shape. Symbol formats differ per exchange, so each entry carries its own asset list rather than sharing one across exchanges.

CLI equivalent:

```bash
crypto-lob-stream --exchanges "binance:BTCUSDT,ETHUSDT;kraken:BTC/USD;binance_futures:BTCUSDT" --output-dir ./data
```

---

## Data integrity

Two independent integrity checks run alongside data capture.

### Gap detection

On by default (`detect_gaps=True`) for exchanges with real sequence numbers (see table above). After every depth message, the new message's `first_update_id` is checked against the previous message's `last_update_id`; a mismatch means updates were missed and is logged plus written to a `gaps/` table (also checked at the very first live message after a connection, against the snapshot's own update id). Not meaningful for Coinbase/Kraken, which lack a real per-message sequence number — skipped automatically there rather than producing meaningless rows.

When a gap is detected and `resync_on_gap=True` (default), a fresh snapshot is fetched automatically wherever a REST endpoint exists (Binance, Binance Futures). For socket-snapshot exchanges (OKX, Bybit family) there's no independent way to re-fetch just a snapshot without a full reconnect, so today this case logs and records the gap; the next automatic reconnect re-establishes a clean snapshot.

### Checksum verification (Kraken)

Off by default (`verify_checksums=True` to enable). For Kraken, a live top-of-book mirror is maintained per asset and verified against the CRC32 checksum Kraken sends on every `book` message. This is a correctness check on the book reconstruction logic itself, independent of and complementary to gap detection. Mismatches are logged and written to a `checksums/` table.

```python
streamer = LOBStreamer(
    assets=["BTC/USD"],
    exchange="kraken",
    verify_checksums=True,
    on_checksum_fail=lambda r: print(f"mismatch: {r}"),
)
streamer.run()
```

### Auditing captured data for coverage gaps

Separate from the two live checks above, `audit_coverage()` inspects Parquet files already on disk and flags hours whose data looks suspiciously thin relative to the file's intended time window — the signature left behind by, for example, two writes colliding on the same output path (fixed in 0.9.2; see "Known limitations" below and the [0.9.2 release notes](https://github.com/Goodie-Goody/crypto_lob_stream_pypi/releases/tag/v0.9.2) for the full story). Useful both as a one-time check on older data and as an ongoing spot-check against future regressions of the same shape.

```python
from crypto_lob_stream import audit_coverage, summarize

# window_seconds must match what a healthy file is actually expected to
# cover: 3600 for raw hour-named files, or your compact() granularity in
# seconds for already-compacted files (86400 for "day", etc.)
results = audit_coverage("./lob_data/trades/binance/BTCUSDT", window_seconds=3600)
print(summarize(results))
```

---

## Output structure

```
{output_dir}/
  trades/{exchange}/{asset}/YYYY-MM-DD-HHMMSS.parquet
  depth/{exchange}/{asset}/YYYY-MM-DD-HHMMSS.parquet
  snapshots/{exchange}/{asset}/YYYY-MM-DD-HHMMSS.parquet
  gaps/{exchange}/{asset}/YYYY-MM-DD-HHMMSS.parquet            # only written when a gap is detected
  checksums/{exchange}/{asset}/YYYY-MM-DD-HHMMSS.parquet       # only written on a mismatch
  funding/{exchange}/{asset}/YYYY-MM-DD-HHMMSS.parquet         # futures/perps only
  liquidations/{exchange}/{asset}/YYYY-MM-DD-HHMMSS.parquet    # futures/perps only
  open_interest/{exchange}/{asset}/YYYY-MM-DD-HHMMSS.parquet   # futures/perps only
```

All files are Snappy-compressed Parquet, flushed every 5 minutes by default (configurable with `--flush-interval`). Output is always partitioned by exchange first, then asset, so running multiple exchanges never collides, even for the same symbol on two different exchanges.

> **Note for users of pre-0.7.0 versions:** every Parquet table now carries an explicit `exchange` column, and paths are nested one level deeper (`{prefix}/{exchange}/{asset}/...` instead of `{prefix}/{asset}/...`). Existing data will sit alongside the new layout rather than merge into it — move it under an `{exchange}/` subfolder manually if you want one unified tree.

> **Note for users of pre-0.9.2 versions:** filenames changed from hour precision (`YYYY-MM-DD-HH.parquet`) to full second precision (`YYYY-MM-DD-HHMMSS.parquet`) for every table except `snapshots` (which already used second precision). This fixes a real issue where multiple flushes within the same hour could silently overwrite one another — see "Known limitations" below and the [0.9.2 release notes](https://github.com/Goodie-Goody/crypto_lob_stream_pypi/releases/tag/v0.9.2) for the full explanation. Existing pre-0.9.2 files aren't renamed automatically; new files simply use the corrected format going forward.

---

## Schemas

Every table's `timestamp_ms` is local receive time; several also carry a separate, nullable `exchange_ts` — see "Timestamps" below for the distinction and why it matters.

### trades

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Local receive time (Unix ms) — see "Timestamps" below |
| exchange_ts | int64 (nullable) | The exchange's own event time, where provided — see "Timestamps" below |
| exchange | string | e.g. `binance` |
| asset | string | Native symbol, e.g. BTCUSDT |
| trade_id | int64 | Exchange trade ID (UUID-based IDs are hashed to a stable int) |
| price | float64 | |
| quantity | float64 | |
| buyer_maker | bool | True if the buyer was the maker |

### depth (diff events)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Local receive time (Unix ms) — see "Timestamps" below |
| exchange_ts | int64 (nullable) | The exchange's own event time, where provided — see "Timestamps" below |
| exchange | string | |
| asset | string | |
| side | string | bid or ask |
| price | float64 | |
| quantity | float64 | 0.0 means the level was removed |
| first_update_id | int64 | Sequence start (exchange-specific; falls back to timestamp where no sequence id exists) |
| last_update_id | int64 | Sequence number for replay ordering |

### snapshots

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Snapshot time (Unix ms) |
| exchange | string | |
| asset | string | |
| side | string | bid or ask |
| price | float64 | |
| quantity | float64 | |
| last_update_id | int64 | Snapshot anchor for diff replay |

A snapshot is captured at the start of every connection (and after reconnects), and again automatically whenever a gap is detected and `resync_on_gap=True`. Binance fetches it via REST; the other exchanges deliver it over the WebSocket.

### gaps

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | When the gap was detected (Unix ms) |
| exchange | string | |
| asset | string | |
| expected_update_id | int64 | What the next message's first_update_id should have been |
| received_update_id | int64 | What it actually was |
| gap_size | int64 | Number of missed update ids |

### checksums

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | When the mismatch was detected (Unix ms) |
| exchange | string | Currently only `kraken` |
| asset | string | |
| expected | int64 | CRC32 computed from the live book mirror |
| received | int64 | Checksum the exchange sent |

Matches are not written (that would be a row per update); only mismatches are persisted, since they're the actionable signal.

### funding (futures/perps only)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Local receive time (Unix ms) — see "Timestamps" below |
| exchange_ts | int64 (nullable) | The exchange's own event time, where provided — see "Timestamps" below |
| exchange | string | |
| asset | string | |
| mark_price | float64 | `None` for `okx_swap` — see "Known limitations" below |
| funding_rate | float64 | |
| next_funding_ms | int64 | Next funding settlement time (Unix ms) |

### liquidations (futures/perps only)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Local receive time (Unix ms) — see "Timestamps" below |
| exchange_ts | int64 (nullable) | The exchange's own event time, where provided — see "Timestamps" below |
| exchange | string | |
| asset | string | |
| side | string | Side of the liquidated position |
| price | float64 | |
| quantity | float64 | |

Every row is a forced position closure — the most direct real-time signal of leveraged positions getting stretched, the actual stress event rather than just a precursor to one.

### open_interest (futures/perps only)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Local receive/poll time (Unix ms) — see "Timestamps" below |
| exchange_ts | int64 (nullable) | The exchange's own event time, where provided — see "Timestamps" below |
| exchange | string | |
| asset | string | |
| open_interest | float64 | Total outstanding contracts/base currency |
| open_interest_value | float64 | In quote currency (e.g. USD), where the exchange provides it. `None` for `binance_futures` — see "Known limitations" |

Liquidations are the fire; open interest is the fuel — the two are meant to be read together for stress research, not separately.

---

## Timestamps: `timestamp_ms` vs `exchange_ts`

Every trade, depth, funding, liquidation, and open-interest record carries two independent timestamps:

- **`timestamp_ms`** — always this process's local receive time (or, for REST-polled data like `binance_futures` open interest, local parse time). Never populated from anything the exchange sends. Consistent across every exchange and record type, which makes it the right field to sort or join on when you need a single trustworthy clock across sources.
- **`exchange_ts`** — the exchange's own event timestamp, kept separately, where the exchange actually provides one. Honestly `null` when it doesn't — never silently backfilled with local time, so a `null` here is a real signal ("this exchange/message didn't carry one"), not missing data.

Keeping these separate — rather than blending them into one ambiguous `timestamp_ms`, as earlier versions of this package did for some record types — means `timestamp_ms` is reliable for ordering and joining across exchanges, while `exchange_ts` stays available for anything that specifically needs the exchange's own clock: cross-checking against exchange-side event logs, or measuring `timestamp_ms - exchange_ts` as an approximate per-exchange, per-message-type capture latency (network transit plus local processing) — a research angle this split makes newly possible.

Coinbase and Kraken have no real exchange-assigned sequence number for `first_update_id`/`last_update_id`, so those two fields fall back to local receive time specifically when the exchange's own timestamp fails to parse — a separate, deliberate design choice from `exchange_ts`, which stays `null` in that same case rather than being backfilled. `first_update_id`/`last_update_id` need *some* usable ordering value on every row; `exchange_ts` doesn't carry that obligation.

> **Note for users of pre-0.9.0 versions:** `timestamp_ms` semantics changed. Previously it was inconsistent — some record types on some exchanges (e.g. Binance trades) carried the exchange's own timestamp, others carried local receive time, with no way to tell which from the data alone. From 0.9.0 forward it's always local receive time, and `exchange_ts` is new. Data captured before this version may have `timestamp_ms` values that are actually exchange time for some exchange/record-type combinations — treat pre-0.9.0 `timestamp_ms` values with that in mind if precise local-vs-exchange timing matters for your analysis.

---

## LOB reconstruction

A snapshot is the full book frozen at one instant. A diff is one change since the last message: a price level's size went up, went down, or the level disappeared entirely. Replaying a snapshot plus every diff after it, in order, rebuilds the book at any point in between — that's the entire idea. Diffs are used instead of repeatedly re-sending a full snapshot because almost every update only touches one or two price levels; re-sending hundreds of unchanged levels every time would be enormously wasteful.

Reconstruct each exchange's book separately — never merge order books across exchanges. Binance's BTC/USDT and Kraken's BTC/USD are different markets with different participants and can briefly disagree on price; there's no single combined ladder to merge them into. Cross-exchange analysis means comparing books side by side, not collapsing them into one.

### The ghost-level problem

Every exchange in this package signals removal only one way: a diff explicitly setting a price level's quantity to `0`. None of them tell you when a level has simply drifted outside whatever depth you intend to track — it just stops sending updates for that price, and "nothing changed" looks identical to "this fell out of scope." A naive replay (apply every diff, remove only on `quantity == 0`) accumulates these **ghost levels** indefinitely: price levels that are no longer real but were never explicitly zeroed.

This isn't a quirk of one exchange. [An independent 25-hour test against Binance](https://dev.to/oliverzehentleitner) (gap-free, sequence-validated, no pruning) ended with 20,758 tracked bid levels against a real ~1,000-level book — only 24% still matched reality. The fix in both his case and ours is the same: after every update, actively prune the book back to the depth you intend to maintain, rather than waiting for the exchange to tell you when a level should be dropped — for a level that's merely fallen out of scope, it never will. This is the exact bug already found and fixed in this package's own Kraken checksum mirror (see "Data integrity" above); `reconstruct.py` applies the identical fix generally, for anyone reconstructing from any exchange's data.

How exposed each exchange is depends on how narrow its tracked window is — narrower means drift happens more often, not "doesn't happen." Worth separating two genuinely different situations here: some of these numbers are hard ceilings the exchange itself enforces; others are just a depth this package happened to choose, which the underlying live stream isn't actually restricted to.

| Exchange | What's actually captured | Is the ceiling exchange-enforced? |
|---|---|---|
| Kraken | `depth: 25` | Yes — a subscription parameter; the exchange will never send level 26 |
| Bybit / Bybit linear | `orderbook.50` | Yes — same idea, baked into the subscribed topic name |
| OKX / OKX swap | `books` channel | Yes — confirmed via OKX's own docs: the channel itself is a fixed 400 levels, not something we requested |
| Binance / Binance Futures | REST snapshot requested at 1000 | **No** — 1000 is a depth *we* chose for the snapshot call; the live diff stream itself isn't capped, so without pruning the book can genuinely drift past 1000 over time |
| Coinbase | `level2_batch`, full book | No known ceiling documented anywhere — there's no number to enforce |

This applies identically across spot and futures — it's a property of the snapshot-plus-diff protocol shape every adapter shares, not something tied to market type.

### Using the built-in reconstruction helper

```python
from crypto_lob_stream import reconstruct

book = reconstruct("./lob_data", exchange="kraken", asset="BTC/USD")
bids, asks = book.top(n=10)   # best 10 levels per side, correctly pruned
```

`reconstruct()` loads the most recent snapshot plus every depth diff after it from your output directory, replays them, and returns a `BookReconstructor` already pruned to a sensible default depth for that exchange (the table above). Pass `max_depth=` to override it; pass `max_depth=None` to disable pruning entirely (not recommended, for the reasons above). Requesting a depth deeper than what was actually captured — e.g. more than 25 for Kraken, more than 50 for Bybit/Bybit linear — can't recover data that was never subscribed to; `book.top(n=...)` returns whatever depth is actually available in that case, and raises a `UserWarning` naming the exchange, its real captured depth, and a link back to this section, rather than silently handing back fewer rows than you asked for with no explanation.

This operates entirely offline, on Parquet files already on disk. It doesn't run live or connect to any exchange — that's a deliberate scope boundary, not an oversight: a live, continuously-maintained in-memory book is a different job (one ccxt.pro already does), and this package is about capturing the event stream, not maintaining a live one.

### Manual reconstruction

If you're not using Python, or want full control over the replay:

1. Load the nearest `snapshots/` file whose `timestamp_ms` precedes your target window. Its `last_update_id` is the anchor.
2. Discard depth diff rows at or before the snapshot anchor.
3. Apply remaining diffs in ascending `last_update_id` order. Each diff's `quantity` is the level's new total size, not a delta — set it directly, and remove the level when `quantity` is `0.0`.
4. **After every update, prune back to your intended depth.** This step is the fix for the ghost-level problem above — without it, you will reproduce it.
5. Cross-check against `gaps/` for that exchange/asset — any gap row covering your window means a contiguous diff sequence isn't available across that point. Treat the gap as a hard reset rather than reconstructing across it; a fresh snapshot is written immediately after every detected gap where `resync_on_gap=True` and a REST endpoint exists.

For exchanges with true sequence ids, the update ids give exact ordering and gap detection. For Coinbase and Kraken, the message timestamp is the ordering key, and Kraken's checksum table (if run with `verify_checksums=True`) is the integrity signal to check instead.

---

## Compacting Parquet files

By default, every `flush_interval` (5 minutes) produces a new small Parquet file. Over months of continuous capture that becomes tens of thousands of tiny files per exchange/asset — slow to query and slow to even list, especially in cloud storage where each file open carries real latency. This is a separate concern from reconstruction above: it's pure file consolidation, doesn't touch a single row of data, and is worth doing whether or not you ever reconstruct anything.

```python
from crypto_lob_stream import compact, compact_tree

# One (prefix, exchange, asset) leaf at a time:
compact("lob_data/depth/binance/BTCUSDT",
        "lob_data_compacted/depth/binance/BTCUSDT",
        granularity="month")

# Or the whole output_dir at once:
compact_tree("lob_data", "lob_data_compacted", granularity="month")
```

`granularity` is `"day"`, `"week"`, `"month"`, or `"year"` — pick whatever matches how you'll actually query the data later. Pass `delete_source=True` once you trust the result, to remove the small originals after merging; it defaults to `False` so the first run is non-destructive. `compact()`/`compact_tree()` tolerate a bucket spanning a schema change (e.g. a field added in a newer package version) by filling the missing column with `null` in the older rows, rather than raising.

---

## Auditing existing data for coverage gaps

Version 0.9.2 fixed a bug where, at any `flush_interval` under 3600 seconds (the default is 300), files named with hour-only precision could get silently overwritten by a later flush within the same hour — see the [0.9.2 release notes](https://github.com/Goodie-Goody/crypto_lob_stream_pypi/releases/tag/v0.9.2) for the full explanation. `audit_coverage()` checks existing captured files for the timestamp-clustering signature that bug leaves behind: real data spread thin across a whole hour looks very different from a burst crammed into a few minutes with silence either side.

```python
from crypto_lob_stream import audit_coverage, summarize

# Point this at a single (prefix, exchange, asset) leaf, or a whole tree
results = audit_coverage("./lob_data/trades/binance/BTCUSDT", window_seconds=3600)
print(summarize(results))
```

`window_seconds` must match what a healthy file is actually expected to cover, or this produces noisy, meaningless results:

- **Raw, uncompacted files** (one file per `flush_interval`, the default output shape): pass `flush_interval` itself, e.g. `60` — each file only ever covers one flush's worth of time by design, so comparing it against a full hour will flag almost everything as suspicious even when nothing is wrong.
- **Compacted files** (after `compact()`/`compact_tree()`, one file per day/week/month/year bucket): pass that granularity in seconds, e.g. `86400` for `"day"` — these genuinely are expected to span close to their full bucket.
- **Legacy hour-named files from before 0.9.2**: the default `3600` is exactly right — this is what the tool was originally built to check.

`snapshots/` files are a special case worth knowing about: a snapshot is captured once per WebSocket connection, not spread continuously through an hour, so `audit_coverage()` will flag even a perfectly healthy snapshot file as "narrow" if pointed at it directly. Exclude `snapshots/` paths from this check entirely rather than trying to interpret its results there.

Useful both as a one-time check on older data and as an ongoing spot-check against future regressions of the same shape.

---

## Advanced: streaming to Google Cloud Storage

```bash
pip install crypto-lob-stream[gcs]
crypto-lob-stream setup   # one-time wizard, saves bucket + service-account key path
crypto-lob-stream --assets BTCUSDT --exchange binance --output gcs
```

Config is saved to `~/.crypto_lob_stream/config.json`, so credentials apply automatically on later runs. Check it with `crypto-lob-stream config`, or override per run with `--bucket` and `--credentials`.

---

## LOBStreamer parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| assets | list[str] | None | Symbols for the single-exchange form; normalised per exchange. Required unless `exchanges` is given. |
| exchange | str | `"binance"` | Single-exchange form — see the exchange table above for valid values. |
| exchanges | list[dict] | None | Multi-exchange form: `[{"exchange": ..., "assets": [...]}, ...]`. Overrides `assets`/`exchange` when given. |
| output | str | `"local"` | `"local"` or `"gcs"` |
| output_dir | str | `"./lob_data"` | Base directory for local output |
| bucket | str | None | GCS bucket (required when output="gcs") |
| flush_interval | int | `300` | Seconds between buffer flushes |
| open_interest_poll_interval | int | `30` | Seconds between open-interest polls, for exchanges with no WebSocket push for it (currently only `binance_futures`; has no effect for `okx_swap`/`bybit_linear`, which get it via their existing WebSocket channels) |
| on_trade | callable | None | Optional callback per trade record |
| on_depth | callable | None | Optional callback per depth record |
| on_gap | callable | None | Optional callback per detected gap |
| on_checksum_fail | callable | None | Optional callback per checksum mismatch |
| detect_gaps | bool | `True` | See "Data integrity" above |
| resync_on_gap | bool | `True` | Auto-resnapshot on gap, where a REST endpoint is available |
| verify_checksums | bool | `False` | See "Data integrity" above |
| log_dir | str | `"./logs"` | Directory for rotating log files |

---

## Known limitations

- Binance depth files collected before **2026-06-03 14:38:49 UTC** do not contain sequence ids and cannot be used for full reconstruction.
- Files written before **package version 0.9.2** may have coverage gaps in some hours, due to a filename-collision issue that has since been fixed (unique per-second filenames plus a collision guard on every write). See the [0.9.2 release notes](https://github.com/Goodie-Goody/crypto_lob_stream_pypi/releases/tag/v0.9.2) for the full explanation, and `audit_coverage()` (see "Data integrity" above) to check your own older data.
- `okx_swap` funding records have `mark_price=None` by design — a real value requires also subscribing to OKX's separate `mark-price` channel, which isn't wired up yet.
- `binance_futures` open-interest records have `open_interest_value=None` — Binance's REST endpoint provides a contract count but not a USD value (OKX and Bybit linear both provide one, since their open-interest data already carries it).
- OKX swap's `open-interest` channel payload fields (`oi`, `oiUsd`) are assumed to mirror OKX's confirmed REST response shape, per OKX's general consistency between REST and WS field naming — the WS push itself wasn't separately confirmed against a live connection. If `oiUsd` is ever absent in practice, `open_interest_value` will just come through as `None` rather than raising, but this is one to watch on first live run.
- `binance_futures` open interest is REST-polled (default every 30s, see `open_interest_poll_interval`), not pushed over WebSocket — confirmed there is no such push stream at all on Binance Futures, not just an oversight here (even Tardis.dev, a professional data vendor, polls the identical REST endpoint themselves).
- `reconstruct()` is opt-in, not automatic — the raw depth data is captured faithfully as-is, and nothing prunes it for you unless you call `reconstruct()` (or implement the pruning step yourself; see "LOB reconstruction" above). Querying raw `depth/` files directly without reconstruction risks exactly the ghost-level problem described there.
- Auto-resync-on-gap only works for REST-snapshot exchanges (Binance, Binance Futures). OKX/Bybit-family exchanges log and record the gap but need a full reconnect to get a fresh socket-delivered snapshot.
- A genuine exchange-side gap hasn't yet been observed in live testing; the detection logic itself is validated via synthetic/mocked sequence breaks (`tests/test_integration_mocked.py`).
- Multi-exchange mode (`exchanges=`) runs each exchange as an independent task in one process/event loop. One exchange's connection trouble doesn't affect the others, but a process-level crash takes every feed down together. For full fault isolation, separate processes per exchange are still the safer choice.
- Binance's WebSocket infrastructure is mid-migration to routed `/public`/`/market`/`/private` paths; further changes there could affect the Binance and Binance Futures adapters.
- Dated/quarterly futures contracts aren't supported — only perpetuals, for the three futures-capable exchanges.
- **Kraken Futures and Coinbase's institutional perpetuals (Coinbase International Exchange) are not covered.** Both are architecturally separate platforms from the spot APIs already integrated here — different WebSocket host, different auth, different protocol entirely — rather than a different instrument category on the same API the way Binance/OKX/Bybit futures are. Coinbase's perpetuals additionally require institutional eligibility and onboarding, independent of any engineering effort. Adding either would be comparable in scope to integrating a new exchange from scratch.

---

## Hugging Face dataset

Monthly snapshots of order book, trade, and (for perpetual futures venues) funding, liquidation, and open-interest data — across all eight supported exchanges — are published freely on Hugging Face as a contribution to the open-source research community. See the dataset card for current coverage and known limitations.

---

## Roadmap

- OKX mark price for `okx_swap` funding records
- Live confirmation of OKX swap's `open-interest` WS push field names (currently assumed from REST, see "Known limitations")
- Auto-resync-on-gap for socket-snapshot exchanges (OKX, Bybit) without requiring a full reconnect
- Dated/quarterly futures contracts
- Additional spot venues
- AWS S3 output target
- FX and equity venues, where data licensing permits (note: unlike crypto, full depth-of-book equity data is generally licensed and cannot be freely redistributed, so coverage there will be limited)

---

## Contributing

Issues and pull requests are welcome. Adding a new exchange means implementing one adapter class against the `Exchange` interface in `exchanges.py`; please include tests. The existing adapters are worked examples.

---

## License

MIT