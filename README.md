# crypto-lob-stream

Stream Level 2 order book and trade data from major crypto exchanges to local disk or Google Cloud Storage, in analysis-ready Parquet.

**Supported exchanges:** Binance, Binance USDⓢ-M Futures, Coinbase, OKX, OKX swap, Kraken, Bybit, Bybit linear.

## Why this exists

High-quality, granular limit order book data is one of the biggest barriers to entry in high-frequency and microstructure research. For most markets the full depth-of-book feed is paywalled, licensed, time-limited, or never released publicly at all, which puts serious order-book research out of reach for independent researchers, students, and small teams.

Crypto is the exception. The major crypto exchanges publish full L2 order book and trade data over free, public WebSocket feeds. This package puts a single, consistent interface in front of several of them, so anyone can collect continuous, fully reconstructable order book data with one command (or several, concurrently, in one process) and store it in an open format. The goal is simple: lower the data barrier for high-frequency and market-microstructure research, at least in the one asset class where the raw feeds are genuinely open.

As a contribution back to the community, monthly snapshots of BTC, ETH, and SOL order book data (collected from Binance) are published freely on [Hugging Face](https://huggingface.co/).

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

### Stream to local disk

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

### Pick an exchange

Each exchange uses its own native symbol format, but the package normalises automatically, so you can pass any common form and it converts to the right one.

```python
# Coinbase  (BTC-USD)
LOBStreamer(assets=["BTC-USD"], exchange="coinbase", output_dir="./data").run()

# OKX  (BTC-USDT)
LOBStreamer(assets=["BTC-USDT"], exchange="okx", output_dir="./data").run()

# Kraken  (BTC/USD)
LOBStreamer(assets=["BTC/USD"], exchange="kraken", output_dir="./data").run()

# Bybit  (BTCUSDT)
LOBStreamer(assets=["BTCUSDT"], exchange="bybit", output_dir="./data").run()

# Binance USDⓢ-M Futures / perpetuals  (BTCUSDT) -- see "Futures / perps" below
LOBStreamer(assets=["BTCUSDT"], exchange="binance_futures", output_dir="./data").run()
```

### Multiple exchanges in one process

Run several exchanges concurrently inside a single process and event loop, instead of launching one process per exchange:

```python
streamer = LOBStreamer(
    exchanges=[
        {"exchange": "binance", "assets": ["BTCUSDT", "ETHUSDT"]},
        {"exchange": "kraken", "assets": ["BTC/USD"]},
        {"exchange": "okx", "assets": ["BTC-USDT"]},
    ],
    output="local",
    output_dir="./lob_data",
)
streamer.run()
```

Each exchange keeps its own independent WebSocket connection and reconnect loop; one exchange having connection trouble doesn't affect the others. Output is identical in shape to running each exchange as a separate single-exchange process -- this is purely about process count, not data shape. Symbol formats differ per exchange, so each entry carries its own asset list rather than sharing one across exchanges.

### CLI

```bash
# Binance (default)
crypto-lob-stream --assets BTCUSDT,ETHUSDT --output-dir ./data

# Another exchange
crypto-lob-stream --assets BTC-USD --exchange coinbase --output-dir ./data

# Multiple exchanges in one process
crypto-lob-stream --exchanges "binance:BTCUSDT,ETHUSDT;kraken:BTC/USD;okx:BTC-USDT" --output-dir ./data

# Custom flush interval (seconds)
crypto-lob-stream --assets BTCUSDT --exchange bybit --flush-interval 60 --output-dir ./data
```

Press Ctrl+C to stop cleanly. Parquet files start landing within a few minutes.

### Real-time callback

Hook into every trade, depth, gap, or checksum-failure event as it arrives, for live monitoring or piping into your own sink:

```python
def on_trade(record):
    print(f"{record['exchange']} {record['asset']} {record['price']} x {record['quantity']}")

def on_gap(record):
    print(f"GAP on {record['exchange']}/{record['asset']}: missed {record['gap_size']} updates")

streamer = LOBStreamer(
    assets=["BTCUSDT"],
    exchange="binance",
    output="local",
    on_trade=on_trade,
    on_gap=on_gap,
)
streamer.run()
```

---

## Supported exchanges

| Exchange | `exchange=` | Symbol format | Snapshot source | Real sequence ids (gap detection) | Checksum verification |
|---|---|---|---|---|---|
| Binance | `binance` | `BTCUSDT` | REST | Yes | No |
| Binance USDⓢ-M Futures | `binance_futures` | `BTCUSDT` | REST | Yes | No |
| Coinbase | `coinbase` | `BTC-USD` | WebSocket | No (timestamp ordering only) | No |
| OKX | `okx` | `BTC-USDT` | WebSocket | Yes | Deprecated by OKX -- see note below |
| OKX swap (perpetuals) | `okx_swap` | `BTC-USDT` | WebSocket | Yes | Deprecated by OKX -- see note below |
| Kraken | `kraken` | `BTC/USD` | WebSocket | No (timestamp ordering only) | Yes |
| Bybit | `bybit` | `BTCUSDT` | WebSocket | Yes | No |
| Bybit linear (USDT perpetuals) | `bybit_linear` | `BTCUSDT` | WebSocket | Yes | No |

Symbols are normalised per exchange, so `BTCUSDT`, `BTC-USDT`, and `BTC/USDT` all resolve to the correct native form for whichever exchange you choose. `okx_swap` and `bybit_linear` automatically format the perpetual-contract symbol (`BTC-USDT-SWAP`, plain `BTCUSDT`) from whatever you pass in.

**On OKX's checksum:** OKX is deprecating the `books` channel checksum entirely (production cutover June 23, 2026) in favor of `seqId`/`prevSeqId` validation -- which this package already implements as gap detection for OKX and OKX swap. OKX's own stated reasoning is that this provides equivalent-or-stronger integrity protection than the checksum did, so there's no integrity-checking gap here, just a different (and, per the exchange itself, better) mechanism.

---

## Futures / perps

Three futures/perps adapters are available, covering the three exchanges' USDT-margined (or USDⓢ-M) perpetual contracts: `binance_futures`, `okx_swap`, `bybit_linear`. All three add a `funding/` Parquet table (FUNDING_SCHEMA: timestamp_ms, exchange, asset, mark_price, funding_rate, next_funding_ms) alongside the usual `trades/`/`depth/` tables.

```python
streamer = LOBStreamer(
    exchanges=[
        {"exchange": "binance_futures", "assets": ["BTCUSDT"]},
        {"exchange": "okx_swap", "assets": ["BTC-USDT"]},
        {"exchange": "bybit_linear", "assets": ["BTCUSDT"]},
    ],
    output_dir="./futures_data",
).run()
```

### Binance USDⓢ-M Futures (`binance_futures`)

**Live-tested.** Depth and trades worked on the first live run. The funding stream initially shipped broken in a way that produced *zero errors* -- Binance recently split its WebSocket infrastructure into `/public`, `/market`, and `/private` routed paths, and a connection without one of those paths in the URL silently only receives `/public` stream data. `markPrice` is a `/market` stream, so it was dropped with no error, no warning, nothing -- depth and trades kept flowing normally while funding just never arrived. Caught by noticing the flush line had no `funding:` count after a live run. Fixed by routing `markPrice` through a second, `/market`-routed connection (`aux_ws_url` on the `Exchange` base class) that runs concurrently with the main one. Re-tested live and funding records now flow correctly alongside trades.

Remaining caveats: gap detection uses the `pu` field per Binance's futures docs but a real futures gap hasn't been observed live (only synthetic ones); only perpetual contracts are handled, not dated/quarterly futures (different symbol format); if Binance changes stream categorization again or decommissions the legacy unrouted connection depth/trade currently use, this would need revisiting.

### OKX swap (`okx_swap`)

**Live-tested, clean on the first run.** Connected, subscribed, and streamed trades/depth/funding with no errors, no gap warnings. Structurally a much smaller change than Binance Futures -- same public WebSocket endpoint as spot OKX (no second connection needed), just a different `instId` (`BTC-USDT-SWAP`) and an added `funding-rate` channel. `funding_rate`/`next_funding_ms` field names are confirmed via OKX's own SDK source. `mark_price` is deliberately left `None`: getting a real value would mean also subscribing to OKX's separate `mark-price` channel, and its exact push field name wasn't confirmed from documentation in the time available -- rather than guess (which is exactly how today's Kraken and Binance bugs started), this is left as a documented gap instead of a third unverified assumption.

### Bybit linear (`bybit_linear`)

**Live-tested, and it caught a real bug -- in `bybit` (spot) too, not just this adapter.** Trades, depth, and funding all worked correctly on the first live run. Gap detection did not: it reported thousands of gaps per minute, each claiming many billions of missed updates, which is the signature of a detector comparing the wrong numbers rather than a real venue-side problem. The root cause: Bybit's orderbook messages carry two different counters -- `u` (a real per-symbol update id, incrementing by roughly 1 per message) and `seq` (a "cross sequence" value Bybit's own docs describe as being for comparing data freshness *across different sources*, e.g. REST vs WebSocket, not a chainable per-message id, and on a wildly different numeric scale -- Bybit's own sample payloads show `u` around 10⁸ and `seq` around 10¹⁰). The original adapter used `seq` for `first_update_id` and `u` for `last_update_id`, so the gap detector was permanently comparing two unrelated counters. Fixed by using `u` for both. This bug existed in plain `bybit` (spot) too -- it just hadn't been gap-detection-live-tested before today, since gap detection itself only shipped today and `bybit` specifically was never run with it on until this was found via `bybit_linear`. Re-tested live after the fix: zero gap warnings across a clean run.

Bybit's own `tickers` channel field names (`markPrice`, `fundingRate`, `nextFundingTime`) were confirmed correct on the first live run -- no issues there.

### Why not Coinbase or Kraken futures?

The three adapters above were all tractable in a single session because each exchange's futures product is just a different *instrument category* on the **same public API** the spot adapter already talks to -- same WebSocket host (or close to it), same message framework, different symbol/path. Coinbase and Kraken don't work that way:

- **Kraken Futures** runs on a completely separate platform (`futures.kraken.com`, not `ws.kraken.com`), with its own authentication, its own WebSocket protocol, and its own API keys generated on a different site. Kraken's own docs describe Spot and Futures as genuinely separate API ecosystems. Adding it would mean writing a new adapter from scratch -- the same scope as adding an entirely new exchange (e.g. Deribit), not a subclass tweak like `okx_swap`/`bybit_linear` were.
- **Coinbase's perpetuals** live on **Coinbase International Exchange**, a separate institutional-only platform (non-US clients, eligibility application and onboarding required) -- not on the consumer Coinbase Exchange this package already integrates with. Even setting aside engineering effort, this one is gated behind business eligibility, not just code.

So this isn't an oversight -- it's that "futures" means two structurally different things depending on the exchange, and only three of the five futures-capable venues here happen to make it cheap.

---

## Output structure

```
{output_dir}/
  trades/{exchange}/{asset}/YYYY-MM-DD-HH.parquet
  depth/{exchange}/{asset}/YYYY-MM-DD-HH.parquet
  snapshots/{exchange}/{asset}/YYYY-MM-DD-HHmmss.parquet
  gaps/{exchange}/{asset}/YYYY-MM-DD-HH.parquet        # only written when a gap is detected
  checksums/{exchange}/{asset}/YYYY-MM-DD-HH.parquet   # only written on a mismatch
  funding/{exchange}/{asset}/YYYY-MM-DD-HH.parquet     # futures/perps only
```

All files are Snappy-compressed Parquet, flushed every 5 minutes by default (configurable with `--flush-interval`). Output is always partitioned by exchange first, then asset, so running multiple exchanges (whether as separate processes or together via `exchanges=`) never collides, even for the same symbol on two different exchanges.

> **Note on this layout vs earlier releases:** every Parquet table now also carries an explicit `exchange` column, and paths are nested one level deeper than before (`{prefix}/{exchange}/{asset}/...` instead of `{prefix}/{asset}/...`). If you have existing data collected with an earlier version, it will sit alongside the new layout rather than merge into it -- move it under an `{exchange}/` subfolder manually if you want one unified tree.

---

## Schemas

### trades

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Event time (Unix ms) |
| exchange | string | e.g. `binance` |
| asset | string | Native symbol, e.g. BTCUSDT |
| trade_id | int64 | Exchange trade ID (UUID-based IDs are hashed to a stable int) |
| price | float64 | |
| quantity | float64 | |
| buyer_maker | bool | True if the buyer was the maker |

### depth (diff events)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Event/receipt time (Unix ms) |
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

A snapshot is captured at the start of every connection (and after reconnects), and again automatically whenever a gap is detected and `resync_on_gap=True`. Binance fetches it via REST; the other exchanges deliver it over the WebSocket. Either way it gives a valid anchor point for book reconstruction.

### gaps (written only when `detect_gaps=True`, default, and a gap is actually detected)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | When the gap was detected (Unix ms) |
| exchange | string | |
| asset | string | |
| expected_update_id | int64 | What the next message's first_update_id should have been |
| received_update_id | int64 | What it actually was |
| gap_size | int64 | Number of missed update ids |

Only meaningful for exchanges with real sequence numbers (see table above). For Coinbase and Kraken, `first_update_id`/`last_update_id` in the depth schema are both the message timestamp, not a real sequence id, so there's no continuity invariant to check -- gap detection is automatically skipped for those rather than producing meaningless rows.

### checksums (written only when `verify_checksums=True`, off by default, and a mismatch is detected)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | When the mismatch was detected (Unix ms) |
| exchange | string | Currently only `kraken` |
| asset | string | |
| expected | int64 | CRC32 computed from the live book mirror |
| received | int64 | Checksum the exchange sent |

Matches are not written (that would be a row per update); only mismatches are persisted, since they're the actionable signal.

### funding (futures/perps only -- currently `binance_futures`)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Event time (Unix ms) |
| exchange | string | |
| asset | string | |
| mark_price | float64 | |
| funding_rate | float64 | |
| next_funding_ms | int64 | Next funding settlement time (Unix ms) |

---

## LOB reconstruction

To reconstruct the full order book at a point in time:

1. Load the nearest `snapshots/` file whose `timestamp_ms` precedes your target window. Its `last_update_id` is the anchor.
2. Discard depth diff rows at or before the snapshot anchor.
3. Apply remaining diffs in ascending `last_update_id` order. For each price level, set the quantity to the diff value, and remove the level when quantity is 0.0.
4. Cross-check against `gaps/` for that exchange/asset -- any gap row covering your window means a contiguous diff sequence isn't available across that point. Treat the gap as a hard reset: don't reconstruct across it. A fresh snapshot is written immediately after every detected gap (when `resync_on_gap=True`, the default, and the exchange has a REST snapshot endpoint), so you can resume reconstruction from there.

For exchanges that expose true sequence ids (Binance, Binance Futures, OKX, Bybit), the update ids give exact ordering and gap detection. For exchanges that don't (Coinbase, Kraken), the message timestamp is used as the ordering key, and Kraken's checksum table (if you ran with `verify_checksums=True`) is the integrity signal to check instead.

---

## Gap detection

On by default (`detect_gaps=True`) for exchanges with real sequence numbers. After every depth message, the next message's `first_update_id` is checked against the previous message's `last_update_id`; a mismatch means updates were missed (dropped connection, slow consumer, exchange-side hiccup) and is logged plus written to the `gaps/` table. The very first live message after a (re)connection is checked too, against the snapshot's own update id -- that boundary, where a REST fetch races the live stream, is one of the likelier places to actually miss something.

When a gap is detected and `resync_on_gap=True` (default), a fresh snapshot is fetched automatically for the affected asset if the exchange has a REST endpoint (Binance, Binance Futures today). For socket-snapshot exchanges (OKX, Bybit) there's no independent way to re-fetch just a snapshot without a full reconnect, so today this case only logs and records the gap -- a full reconnect (which already happens automatically on the next stream error) is what re-establishes a clean socket-delivered snapshot for those. Closing that gap automatically too is on the roadmap.

This has been validated against synthetic/mocked sequence breaks (see `tests/test_integration_mocked.py`) and run live against binance, binance_futures, kraken, bybit_linear (post-fix), and okx_swap without false positives. It was *also* run live against bybit_linear pre-fix, which turned up thousands of false-positive gaps per minute -- not a real venue-side problem, but a bug in the `bybit`/`bybit_linear` adapter's continuity-id mapping (see "Bybit linear" under "Futures / perps" for the full story). That's worth noting either way it cuts: the detector did exactly its job by surfacing a real discrepancy, even though the discrepancy turned out to be in this package rather than the exchange. A *genuine* exchange-side gap still hasn't been observed live on any exchange -- live runs so far have simply had nothing real to detect.

---

## Checksum verification (Kraken)

Off by default (`verify_checksums=True` to enable). When on, for exchanges that implement it -- currently only Kraken -- a live top-of-book mirror is maintained per asset and verified against the CRC32 checksum Kraken sends on every `book` snapshot/update message. This is a correctness check on the book itself, independent of and complementary to gap detection: a connection can be perfectly contiguous (no gaps) and still disagree with the exchange's own book state if something in level-application logic is subtly wrong.

**Implementation status:** live-tested and confirmed working. The first live run against real Kraken data hit a 100% mismatch rate -- every single message, with computed and received checksums both internally consistent but never matching. That signature (rather than occasional/random mismatches) pointed away from the string-formatting algorithm and toward something structural, which a targeted depth-10 diagnostic run confirmed: the formatting and CRC32 logic were correct, but the local book mirror wasn't being truncated to the subscribed depth (Kraken's docs are explicit that levels falling out of your subscribed depth -- 25 by default here -- don't get an explicit `qty: 0` removal; you're expected to truncate locally, which this package wasn't doing). Fixed by truncating the mirror to the subscribed depth after every update. Re-tested live for several minutes across tens of thousands of depth events with zero mismatches afterward.

```python
streamer = LOBStreamer(
    assets=["BTC/USD"],
    exchange="kraken",
    verify_checksums=True,
    on_checksum_fail=lambda r: print(f"mismatch: {r}"),
)
streamer.run()
```

OKX's checksum is being deprecated by the exchange itself (see "Supported exchanges" above) in favor of `seqId`/`prevSeqId` -- which this package already covers via gap detection for OKX/OKX swap -- so there's no OKX checksum work planned.

---

## Advanced: streaming to Google Cloud Storage

For long-running collection on a server you can stream directly to a GCS bucket. This is optional and needs the `gcs` extra:

```bash
pip install crypto-lob-stream[gcs]
```

Run the one-time setup wizard, which saves your bucket and service-account key path:

```bash
crypto-lob-stream setup
```

Then stream to the cloud:

```bash
crypto-lob-stream --assets BTCUSDT --exchange binance --output gcs
```

Config is saved to `~/.crypto_lob_stream/config.json`, so credentials apply automatically on later runs. Check it with `crypto-lob-stream config`, or override per run with `--bucket` and `--credentials`.

---

## LOBStreamer parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| assets | list[str] | None | Symbols for the single-exchange form; normalised per exchange. Required unless `exchanges` is given. |
| exchange | str | `"binance"` | Single-exchange form: binance, binance_futures, coinbase, okx, kraken, or bybit. |
| exchanges | list[dict] | None | Multi-exchange form: `[{"exchange": ..., "assets": [...]}, ...]`. Overrides `assets`/`exchange` when given. |
| output | str | `"local"` | `"local"` or `"gcs"` |
| output_dir | str | `"./lob_data"` | Base directory for local output |
| bucket | str | None | GCS bucket (required when output="gcs") |
| flush_interval | int | `300` | Seconds between buffer flushes |
| on_trade | callable | None | Optional callback per trade record |
| on_depth | callable | None | Optional callback per depth record |
| on_gap | callable | None | Optional callback per detected gap |
| on_checksum_fail | callable | None | Optional callback per checksum mismatch |
| detect_gaps | bool | `True` | See "Gap detection" above |
| resync_on_gap | bool | `True` | Auto-resnapshot on gap, where a REST endpoint is available |
| verify_checksums | bool | `False` | See "Checksum verification" above |
| log_dir | str | `"./logs"` | Directory for rotating log files |

---

## Data limitations

- Binance depth files collected before **2026-06-03 14:38:49 UTC** do not contain sequence ids and cannot be used for full reconstruction. Files from that point onward, and all data from the other spot exchanges, are fully reconstructable.
- All 8 exchange adapters, gap detection, Kraken checksum verification, and multi-exchange concurrency have been run against live connections and are working as of this release. Three real bugs were caught and fixed this way, not by review: Kraken's checksum (missing local-book truncation to the subscribed depth), `binance_futures`' funding stream (Binance's `/public`/`/market`/`/private` WebSocket routing split silently dropping `markPrice`), and `bybit`/`bybit_linear`'s gap detection (using the wrong counter -- `seq` instead of `u` -- for continuity, which predates today's futures work and affected plain `bybit` too). See the relevant sections above for each. A *genuine* exchange-side gap specifically hasn't been observed live yet, so that one trigger path is still only validated synthetically (see "Gap detection" above).
- OKX's `mark_price` for `okx_swap` funding records is `None` by design, not a bug -- see "Futures / perps" above for why.
- Multi-exchange (`exchanges=`) runs each exchange as an independent task in one event loop; one exchange's connection trouble doesn't block the others, but a Python-level crash in the process still takes every feed down together. For maximum isolation (e.g. one exchange's outage shouldn't risk another's data at all), separate processes are still the safer choice -- multi-exchange mode is about convenience and resource sharing, not fault isolation.
- Binance's WebSocket infrastructure is mid-migration to routed `/public`/`/market`/`/private` paths (see "Futures / perps" above); if they change stream categorization again or fully decommission the legacy unrouted connection, the spot/futures adapters here would need a corresponding update.
- Kraken Futures and Coinbase's institutional perpetuals (Coinbase International Exchange) are not covered and aren't simple additions -- both are architecturally separate platforms from the spot APIs this package already integrates with, not just a different instrument category on the same one. See "Why not Coinbase or Kraken futures?" above.

---

## Hugging Face dataset

Monthly snapshots of BTC, ETH, and SOL order book and trade data, collected from Binance with this package, are published freely on Hugging Face as a contribution to the open-source research community. Multi-exchange dataset releases are planned.

---

## Roadmap

- OKX mark price for `okx_swap` funding records (currently `None` -- see "Futures / perps" above for why)
- Auto-resync-on-gap for socket-snapshot exchanges (OKX, Bybit) without requiring a full reconnect
- Dated/quarterly futures contracts (currently only perpetuals are handled for `binance_futures`/`okx_swap`/`bybit_linear`)
- Additional spot venues
- AWS S3 output target
- FX and equity venues are being explored, where data licensing permits (note: unlike crypto, full depth-of-book equity data is generally licensed and cannot be freely redistributed, so coverage there will be limited)

---

## Contributing

Issues and pull requests are welcome. Adding a new exchange means implementing one adapter class against the `Exchange` interface in `exchanges.py`; please include tests. The existing adapters are worked examples.

---

## License

MIT