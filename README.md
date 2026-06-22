# crypto-lob-stream

Stream Level 2 order book, trade, and funding-rate data from major crypto exchanges (spot and perpetual futures) to local disk or Google Cloud Storage, in analysis-ready Parquet.

**Supported exchanges:** Binance, Binance USDⓢ-M Futures, Coinbase, OKX, OKX swap, Kraken, Bybit, Bybit linear.

## Why this exists

High-quality, granular limit order book data is one of the biggest barriers to entry in high-frequency and microstructure research. For most markets the full depth-of-book feed is behind a paywall, licensed, time-limited, or never released publicly at all, which puts serious order-book research out of reach for many independent researchers, students, and small teams that cannot afford the resources required to surmount these challenges.

I experienced it as well, which is where the motivation to build this came from.

The focus on crypto is due to its unique exception compared to other asset classes: the major crypto exchanges publish full L2 order book and trade data over free, public WebSocket feeds. This package puts a single, consistent interface in front of several of them, so anyone can collect continuous, fully reconstructable order book data with one command (or several, concurrently, in one process) and store it in an open format. The goal is simple: lower the data barrier for high-frequency and market-microstructure research, at least in the one asset class where the raw feeds are genuinely open.

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

Three adapters cover USDT-margined (or USDⓢ-M) perpetual contracts: `binance_futures`, `okx_swap`, `bybit_linear`. Each adds a `funding/` Parquet table (mark price, funding rate, next funding time) alongside the usual `trades/`/`depth/` tables:

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

All files are Snappy-compressed Parquet, flushed every 5 minutes by default (configurable with `--flush-interval`). Output is always partitioned by exchange first, then asset, so running multiple exchanges never collides, even for the same symbol on two different exchanges.

> **Note for users of pre-0.7.0 versions:** every Parquet table now carries an explicit `exchange` column, and paths are nested one level deeper (`{prefix}/{exchange}/{asset}/...` instead of `{prefix}/{asset}/...`). Existing data will sit alongside the new layout rather than merge into it — move it under an `{exchange}/` subfolder manually if you want one unified tree.

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
| timestamp_ms | int64 | Event time (Unix ms) |
| exchange | string | |
| asset | string | |
| mark_price | float64 | `None` for `okx_swap` — see "Known limitations" below |
| funding_rate | float64 | |
| next_funding_ms | int64 | Next funding settlement time (Unix ms) |

---

## LOB reconstruction

To reconstruct the full order book at a point in time:

1. Load the nearest `snapshots/` file whose `timestamp_ms` precedes your target window. Its `last_update_id` is the anchor.
2. Discard depth diff rows at or before the snapshot anchor.
3. Apply remaining diffs in ascending `last_update_id` order. For each price level, set the quantity to the diff value, and remove the level when quantity is 0.0.
4. Cross-check against `gaps/` for that exchange/asset — any gap row covering your window means a contiguous diff sequence isn't available across that point. Treat the gap as a hard reset rather than reconstructing across it; a fresh snapshot is written immediately after every detected gap where `resync_on_gap=True` and a REST endpoint exists.

For exchanges with true sequence ids, the update ids give exact ordering and gap detection. For Coinbase and Kraken, the message timestamp is the ordering key, and Kraken's checksum table (if run with `verify_checksums=True`) is the integrity signal to check instead.

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

- `okx_swap` funding records have `mark_price=None` by design — a real value requires also subscribing to OKX's separate `mark-price` channel, which isn't wired up yet.
- Auto-resync-on-gap only works for REST-snapshot exchanges (Binance, Binance Futures). OKX/Bybit-family exchanges log and record the gap but need a full reconnect to get a fresh socket-delivered snapshot.
- A genuine exchange-side gap hasn't yet been observed in live testing; the detection logic itself is validated via synthetic/mocked sequence breaks (`tests/test_integration_mocked.py`).
- Multi-exchange mode (`exchanges=`) runs each exchange as an independent task in one process/event loop. One exchange's connection trouble doesn't affect the others, but a process-level crash takes every feed down together. For full fault isolation, separate processes per exchange are still the safer choice.
- Binance's WebSocket infrastructure is mid-migration to routed `/public`/`/market`/`/private` paths; further changes there could affect the Binance and Binance Futures adapters.
- Dated/quarterly futures contracts aren't supported — only perpetuals, for the three futures-capable exchanges.
- **Kraken Futures and Coinbase's institutional perpetuals (Coinbase International Exchange) are not covered.** Both are architecturally separate platforms from the spot APIs already integrated here — different WebSocket host, different auth, different protocol entirely — rather than a different instrument category on the same API the way Binance/OKX/Bybit futures are. Coinbase's perpetuals additionally require institutional eligibility and onboarding, independent of any engineering effort. Adding either would be comparable in scope to integrating a new exchange from scratch.

---

## Hugging Face dataset

Monthly snapshots of BTC, ETH, and SOL order book and trade data, collected from Binance with this package, are published freely on Hugging Face as a contribution to the open-source research community. Multi-exchange dataset releases are planned.

---

## Roadmap

- OKX mark price for `okx_swap` funding records
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
