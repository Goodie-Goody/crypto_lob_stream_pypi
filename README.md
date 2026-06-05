# crypto-lob-stream

Stream Binance Level 2 order book and trade data to local disk or Google Cloud Storage.

High-quality, granular LOB data is difficult to access openly. Most sources are paywalled, time-limited, or never publicly released. This package makes it easy for anyone to collect continuous, fully reconstructable order book and trade data for any Binance pair and store it in an open, analysis-ready format. Monthly snapshots are published freely on [Hugging Face](https://huggingface.co/) for the research community.

---

## Install

```bash
# Local output only
pip install crypto-lob-stream

# With Google Cloud Storage support
pip install crypto-lob-stream[gcs]
```

Requires Python 3.10+.

---

## Quickstart

### Stream to local disk

```python
from crypto_lob_stream import LOBStreamer

streamer = LOBStreamer(
    assets=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    output="local",
    output_dir="./lob_data",
)
streamer.run()
```

### Stream to GCS

```python
streamer = LOBStreamer(
    assets=["BTCUSDT"],
    output="gcs",
    bucket="my-gcs-bucket",
)
streamer.run()
```

### Real-time callback

```python
def on_trade(record):
    print(f"{record['asset']} {record['price']} x {record['quantity']}")

streamer = LOBStreamer(
    assets=["BTCUSDT"],
    output="local",
    on_trade=on_trade,
)
streamer.run()
```

### CLI

```bash
# Local
crypto-lob-stream --assets BTCUSDT,ETHUSDT --output local --output-dir ./data

# GCS (after running setup)
crypto-lob-stream --assets BTCUSDT,ETHUSDT,SOLUSDT --output gcs

# Custom flush interval (seconds)
crypto-lob-stream --assets BTCUSDT --output local --flush-interval 60
```

---

## GCS setup

If you want to stream to Google Cloud Storage, run the setup wizard once before your first stream. It saves your credentials so you never have to configure them again.

### Step 1 -- Create a GCS bucket

Go to [console.cloud.google.com](https://console.cloud.google.com), create a project, and create a bucket in your preferred region.

### Step 2 -- Create a service account

In the GCP Console, go to IAM & Admin > Service Accounts. Create a new service account, assign it the **Storage Object Admin** role on your bucket, and download the JSON key file.

### Step 3 -- Run the setup wizard

```bash
crypto-lob-stream setup
```

You will be prompted for your bucket name and the path to your JSON key file:

```
crypto-lob-stream -- GCS Setup
----------------------------------
GCS bucket name: my-bucket
Path to service account JSON key: ~/Downloads/my-key.json

Saved config to ~/.crypto_lob_stream/config.json
Testing connection to gs://my-bucket ... OK

Setup complete. You can now run:
  crypto-lob-stream --assets BTCUSDT,ETHUSDT --output gcs
```

Config is saved to `~/.crypto_lob_stream/config.json`. From this point on, just run with `--output gcs` and credentials are applied automatically.

### Check saved config

```bash
crypto-lob-stream config
```

### Override credentials per run

```bash
crypto-lob-stream --assets BTCUSDT --output gcs --bucket other-bucket --credentials /path/to/other-key.json
```

---

## Output structure

```
{output_dir}/
  trades/{asset}/YYYY-MM-DD-HH.parquet
  depth/{asset}/YYYY-MM-DD-HH.parquet
  snapshots/{asset}/YYYY-MM-DD-HHmmss.parquet
```

All files are Snappy-compressed Parquet. Files are flushed to disk or uploaded to GCS every 5 minutes by default.

---

## Schemas

### trades

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Event time (Unix ms) |
| asset | string | e.g. BTCUSDT |
| trade_id | int64 | Binance trade ID |
| price | float64 | |
| quantity | float64 | |
| buyer_maker | bool | True if the buyer is the market maker |

### depth (diff events)

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Receipt time (Unix ms) |
| asset | string | |
| side | string | bid or ask |
| price | float64 | |
| quantity | float64 | 0.0 means the level was removed |
| first_update_id | int64 | `U` field from Binance diff event |
| last_update_id | int64 | `u` field -- sequence number for replay ordering |

### snapshots

| Field | Type | Notes |
|---|---|---|
| timestamp_ms | int64 | Fetch time (Unix ms) |
| asset | string | |
| side | string | bid or ask |
| price | float64 | |
| quantity | float64 | |
| last_update_id | int64 | REST snapshot `lastUpdateId` -- anchor for diff replay |

A REST snapshot is fetched from Binance (1000 levels per side) immediately after every WebSocket connection, including reconnects. This ensures there is always a valid anchor point for book reconstruction.

---

## LOB reconstruction

To reconstruct the full order book at any point in time from stored data:

1. Load the nearest `snapshots/` file whose `timestamp_ms` precedes your target window. The `last_update_id` column is the snapshot anchor.
2. Discard all depth diff rows where `last_update_id <= snapshot_last_update_id`.
3. Verify the first retained diff satisfies `first_update_id <= snapshot_last_update_id + 1`. If not, a gap exists -- use the next available snapshot.
4. Apply diffs in ascending `last_update_id` order. For each price level, set quantity to the diff value. Remove the level if `quantity == 0.0`.

To detect the schema version programmatically:

```python
import pyarrow.parquet as pq

schema = pq.read_schema("depth/btcusdt/2026-06-03-15.parquet")
is_reconstructable = "last_update_id" in schema.names
```

---

## LOBStreamer parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| assets | list[str] | required | Binance symbols, e.g. `["BTCUSDT", "ETHUSDT"]` |
| output | str | `"local"` | `"local"` or `"gcs"` |
| output_dir | str | `"./lob_data"` | Base directory for local output |
| bucket | str | None | GCS bucket name (required for GCS output if not saved via setup) |
| fallback_dir | str | `"./lob_fallback"` | Local directory for failed GCS uploads |
| flush_interval | int | `300` | Seconds between buffer flushes |
| on_trade | callable | None | Optional callback invoked per trade record |
| on_depth | callable | None | Optional callback invoked per depth record |
| log_dir | str | `"./logs"` | Directory for rotating log files |

---

## Data limitations

Depth files collected before **2026-06-03 14:38:49 UTC** do not contain `first_update_id` or `last_update_id` and cannot be used for LOB reconstruction. They may still be useful for analysing the distribution of price-level updates and trade activity. Files from that timestamp onward are fully reconstructable.

---

## Hugging Face dataset

Monthly snapshots of data collected by this package are published at [Hugging Face](https://huggingface.co/) for free public use. Each release includes a dataset card noting the schema cutoff date and any known gaps.

---

## Contributing

Issues and pull requests are welcome. If you add support for a new exchange or output target, please include tests.

---

## License

MIT