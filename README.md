# crypto-lob-stream

Stream Level 2 order book and trade data from major crypto exchanges to local disk or Google Cloud Storage, in analysis-ready Parquet.

**Supported exchanges:** Binance, Coinbase, OKX, Kraken, Bybit.

## Why this exists

High-quality, granular limit order book data is one of the biggest barriers to entry in high-frequency and microstructure research. For most markets the full depth-of-book feed is paywalled, licensed, time-limited, or never released publicly at all, which puts serious order-book research out of reach for independent researchers, students, and small teams.

Crypto is the exception. The major crypto exchanges publish full L2 order book and trade data over free, public WebSocket feeds. This package puts a single, consistent interface in front of five of them, so anyone can collect continuous, fully reconstructable order book data with one command and store it in an open format. The goal is simple: lower the data barrier for high-frequency and market-microstructure research, at least in the one asset class where the raw feeds are genuinely open.

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
```

### CLI

```bash
# Binance (default)
crypto-lob-stream --assets BTCUSDT,ETHUSDT --output-dir ./data

# Another exchange
crypto-lob-stream --assets BTC-USD --exchange coinbase --output-dir ./data

# Custom flush interval (seconds)
crypto-lob-stream --assets BTCUSDT --exchange bybit --flush-interval 60 --output-dir ./data
```

Press Ctrl+C to stop cleanly. Parquet files start landing within a few minutes.

### Real-time callback

Hook into every trade or depth event as it arrives, for live monitoring or piping into your own sink:

```python
def on_trade(record):
    print(f"{record['asset']} {record['price']} x {record['quantity']}")

streamer = LOBStreamer(
    assets=["BTCUSDT"],
    exchange="binance",
    output="local",
    on_trade=on_trade,
)
streamer.run()
```

---

## Supported exchanges

| Exchange | `exchange=` | Symbol format | Snapshot source |
|---|---|---|---|
| Binance | `binance` | `BTCUSDT` | REST |
| Coinbase | `coinbase` | `BTC-USD` | WebSocket |
| OKX | `okx` | `BTC-USDT` | WebSocket |
| Kraken | `kraken` | `BTC/USD` | WebSocket |
| Bybit | `bybit` | `BTCUSDT` | WebSocket |

Symbols are normalised per exchange, so `BTCUSDT`, `BTC-USDT`, and `BTC/USDT` all resolve to the correct native form for whichever exchange you choose.

---

## Output structure

```
{output_dir}/
  trades/{asset}/YYYY-MM-DD-HH.parquet
  depth/{asset}/YYYY-MM-DD-HH.parquet
  snapshots/{asset}/YYYY-MM-DD-HHmmss.parquet
```

All files are Snappy-compressed Parquet, flushed every 5 minutes by default (configurable with `--flush-interval`). Each exchange keeps its native symbol format in the folder path, so streaming the same pair from multiple exchanges never collides.

---

## Schemas

### trades

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Event time (Unix ms) |
| asset | string | Native symbol, e.g. BTCUSDT |
| trade_id | int64 | Exchange trade ID (UUID-based IDs are hashed to a stable int) |
| price | float64 | |
| quantity | float64 | |
| buyer_maker | bool | True if the buyer was the maker |

### depth (diff events)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Event/receipt time (Unix ms) |
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
| asset | string | |
| side | string | bid or ask |
| price | float64 | |
| quantity | float64 | |
| last_update_id | int64 | Snapshot anchor for diff replay |

A snapshot is captured at the start of every connection (and after reconnects). Binance fetches it via REST; the other exchanges deliver it over the WebSocket. Either way it gives a valid anchor point for book reconstruction.

---

## LOB reconstruction

To reconstruct the full order book at a point in time:

1. Load the nearest `snapshots/` file whose `timestamp_ms` precedes your target window. Its `last_update_id` is the anchor.
2. Discard depth diff rows at or before the snapshot anchor.
3. Apply remaining diffs in ascending `last_update_id` order. For each price level, set the quantity to the diff value, and remove the level when quantity is 0.0.

For exchanges that expose true sequence ids (Binance, OKX, Bybit), the update ids give exact ordering and gap detection. For exchanges that don't (Coinbase, Kraken), the message timestamp is used as the ordering key.

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
| assets | list[str] | required | Symbols; normalised per exchange |
| exchange | str | `"binance"` | binance, coinbase, okx, kraken, or bybit |
| output | str | `"local"` | `"local"` or `"gcs"` |
| output_dir | str | `"./lob_data"` | Base directory for local output |
| bucket | str | None | GCS bucket (required when output="gcs") |
| flush_interval | int | `300` | Seconds between buffer flushes |
| on_trade | callable | None | Optional callback per trade record |
| on_depth | callable | None | Optional callback per depth record |
| log_dir | str | `"./logs"` | Directory for rotating log files |

---

## Data limitations

Binance depth files collected before **2026-06-03 14:38:49 UTC** do not contain sequence ids and cannot be used for full reconstruction. Files from that point onward, and all data from the other exchanges, are fully reconstructable.

---

## Hugging Face dataset

Monthly snapshots of BTC, ETH, and SOL order book and trade data, collected from Binance with this package, are published freely on Hugging Face as a contribution to the open-source research community. Multi-exchange dataset releases are planned.

---

## Roadmap

- Additional crypto venues
- AWS S3 output target
- Multiple exchanges within a single streamer process
- FX and equity venues are being explored, where data licensing permits (note: unlike crypto, full depth-of-book equity data is generally licensed and cannot be freely redistributed, so coverage there will be limited)

---

## Contributing

Issues and pull requests are welcome. Adding a new exchange means implementing one adapter class against the `Exchange` interface in `exchanges.py`; please include tests. The existing five adapters are worked examples.

---

## License

MIT