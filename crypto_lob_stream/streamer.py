import asyncio
import json
import logging
import logging.handlers
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Literal, Optional

import aiohttp
import websockets

from .schemas import DEPTH_SCHEMA, SNAPSHOT_SCHEMA, TRADE_SCHEMA
from .writers import retry_gcs_fallbacks, write_gcs, write_local

logger = logging.getLogger(__name__)

OutputType = Literal["local", "gcs"]


class LOBStreamer:
    """Stream Binance order book and trade data to local disk or GCS.

    Parameters
    ----------
    assets : list of str
        Binance trading pair symbols, e.g. ["BTCUSDT", "ETHUSDT"].
        Case-insensitive; stored internally as lowercase.
    output : "local" or "gcs"
        Where to write Parquet files.
    output_dir : str, optional
        Base directory for local output. Required when output="local".
        Defaults to "./lob_data".
    bucket : str, optional
        GCS bucket name. Required when output="gcs".
    fallback_dir : str, optional
        Local fallback directory used when GCS uploads fail.
        Defaults to "./lob_fallback" when output="gcs".
    flush_interval : int
        Seconds between buffer flushes. Default 300 (5 minutes).
    on_trade : callable, optional
        Optional callback invoked for every trade record dict before
        it is buffered. Useful for real-time monitoring or custom sinks.
    on_depth : callable, optional
        Optional callback invoked for every depth record dict.
    log_dir : str
        Directory for rotating log files. Default "./logs".
    """

    def __init__(
        self,
        assets: List[str],
        output: OutputType = "local",
        output_dir: str = "./lob_data",
        bucket: Optional[str] = None,
        fallback_dir: str = "./lob_fallback",
        flush_interval: int = 300,
        on_trade: Optional[Callable] = None,
        on_depth: Optional[Callable] = None,
        log_dir: str = "./logs",
    ):
        if not assets:
            raise ValueError("At least one asset must be specified.")
        if output == "gcs" and not bucket:
            raise ValueError("bucket is required when output='gcs'.")
        if output not in ("local", "gcs"):
            raise ValueError("output must be 'local' or 'gcs'.")

        self.assets = [a.lower() for a in assets]
        self.output = output
        self.output_dir = output_dir
        self.bucket = bucket
        self.fallback_dir = fallback_dir
        self.flush_interval = flush_interval
        self.on_trade = on_trade
        self.on_depth = on_depth

        self._trade_buffer: dict = defaultdict(list)
        self._depth_buffer: dict = defaultdict(list)
        self._last_flush: float = time.time()

        self._setup_logging(log_dir)

    # ── Logging ───────────────────────────────────────────────────────────────

    def _setup_logging(self, log_dir: str):
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)

        if not logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s %(levelname)s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            fh = logging.handlers.RotatingFileHandler(
                log_path / "streamer.log", maxBytes=10 * 1024 * 1024, backupCount=5
            )
            fh.setFormatter(formatter)
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            logger.addHandler(fh)
            logger.addHandler(ch)
            logger.setLevel(logging.INFO)

    # ── Stream URL ────────────────────────────────────────────────────────────

    def _get_stream_url(self) -> str:
        streams = []
        for asset in self.assets:
            streams.append(f"{asset}@trade")
            streams.append(f"{asset}@depth@100ms")
        return f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

    # ── Snapshot fetch ────────────────────────────────────────────────────────

    async def _fetch_snapshot(self, asset: str):
        url = f"https://api.binance.com/api/v3/depth?symbol={asset.upper()}&limit=1000"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.error(f"Snapshot HTTP {resp.status} for {asset}")
                        return
                    snap = await resp.json()
        except Exception as e:
            logger.error(f"Snapshot fetch failed for {asset}: {e}")
            return

        ts = int(time.time() * 1000)
        last_uid = int(snap["lastUpdateId"])
        records = []
        for price, qty in snap.get("bids", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "bid",
                "price":          float(price),
                "quantity":       float(qty),
                "last_update_id": last_uid,
            })
        for price, qty in snap.get("asks", []):
            records.append({
                "timestamp_ms":   ts,
                "asset":          asset.upper(),
                "side":           "ask",
                "price":          float(price),
                "quantity":       float(qty),
                "last_update_id": last_uid,
            })

        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
        self._write(records, SNAPSHOT_SCHEMA, "snapshots", asset, ts_str)
        logger.info(
            f"Snapshot stored for {asset.upper()} | "
            f"lastUpdateId={last_uid} | levels={len(records)}"
        )

    # ── Message handlers ──────────────────────────────────────────────────────

    def _handle_trade(self, asset: str, data: dict):
        required = {"T", "t", "p", "q", "m"}
        if not required.issubset(data):
            logger.warning(f"Malformed trade message for {asset}: {data}")
            return
        try:
            record = {
                "timestamp_ms": int(data["T"]),
                "asset":        asset.upper(),
                "trade_id":     int(data["t"]),
                "price":        float(data["p"]),
                "quantity":     float(data["q"]),
                "buyer_maker":  bool(data["m"]),
            }
            if self.on_trade:
                self.on_trade(record)
            self._trade_buffer[asset].append(record)
        except (ValueError, TypeError) as e:
            logger.warning(f"Trade parse error for {asset}: {e} | raw: {data}")

    def _handle_depth(self, asset: str, data: dict):
        required = {"U", "u", "b", "a"}
        if not required.issubset(data):
            logger.warning(
                f"Malformed depth message for {asset} "
                f"(missing fields): {list(data.keys())}"
            )
            return
        try:
            ts = int(time.time() * 1000)
            first_uid = int(data["U"])
            last_uid  = int(data["u"])
            for price, qty in data["b"]:
                record = {
                    "timestamp_ms":    ts,
                    "asset":           asset.upper(),
                    "side":            "bid",
                    "price":           float(price),
                    "quantity":        float(qty),
                    "first_update_id": first_uid,
                    "last_update_id":  last_uid,
                }
                if self.on_depth:
                    self.on_depth(record)
                self._depth_buffer[asset].append(record)
            for price, qty in data["a"]:
                record = {
                    "timestamp_ms":    ts,
                    "asset":           asset.upper(),
                    "side":            "ask",
                    "price":           float(price),
                    "quantity":        float(qty),
                    "first_update_id": first_uid,
                    "last_update_id":  last_uid,
                }
                if self.on_depth:
                    self.on_depth(record)
                self._depth_buffer[asset].append(record)
        except (ValueError, TypeError) as e:
            logger.warning(f"Depth parse error for {asset}: {e}")

    # ── Write dispatch ────────────────────────────────────────────────────────

    def _write(self, records, schema, prefix, asset, ts_str):
        if self.output == "local":
            write_local(records, schema, self.output_dir, prefix, asset, ts_str)
        else:
            write_gcs(
                records, schema, self.bucket, prefix, asset, ts_str,
                fallback_dir=self.fallback_dir,
            )

    # ── Flush ─────────────────────────────────────────────────────────────────

    def _flush(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_flush) < self.flush_interval:
            return

        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
        flush_time = datetime.now(timezone.utc).strftime("%H:%M UTC")

        for asset in self.assets:
            trades_saved = 0
            depth_saved = 0

            if self._trade_buffer[asset]:
                records = self._trade_buffer[asset][:]
                success = True
                if self.output == "local":
                    success = write_local(
                        records, TRADE_SCHEMA, self.output_dir, "trades", asset, ts_str
                    )
                else:
                    success = write_gcs(
                        records, TRADE_SCHEMA, self.bucket, "trades", asset, ts_str,
                        fallback_dir=self.fallback_dir,
                    )
                if success:
                    trades_saved = len(records)
                    self._trade_buffer[asset] = []

            if self._depth_buffer[asset]:
                records = self._depth_buffer[asset][:]
                success = True
                if self.output == "local":
                    success = write_local(
                        records, DEPTH_SCHEMA, self.output_dir, "depth", asset, ts_str
                    )
                else:
                    success = write_gcs(
                        records, DEPTH_SCHEMA, self.bucket, "depth", asset, ts_str,
                        fallback_dir=self.fallback_dir,
                    )
                if success:
                    depth_saved = len(records)
                    self._depth_buffer[asset] = []

            if trades_saved or depth_saved:
                print(
                    f"[{flush_time}] {asset.upper():10s} "
                    f"trades: {trades_saved:>7,} | "
                    f"depth events: {depth_saved:>9,} | saved"
                )

        self._last_flush = now

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _stream(self):
        url = self._get_stream_url()
        reconnect_delay = 5

        if self.output == "gcs":
            retry_gcs_fallbacks(self.fallback_dir, self.bucket)

        while True:
            heartbeat_task = None
            try:
                logger.info("Connecting to Binance WebSocket...")
                async with websockets.connect(url, ping_interval=180) as ws:
                    logger.info(
                        f"Connected. Streaming: {[a.upper() for a in self.assets]}"
                    )
                    reconnect_delay = 5

                    logger.info("Fetching order book snapshots...")
                    await asyncio.gather(
                        *[self._fetch_snapshot(a) for a in self.assets]
                    )
                    logger.info("Snapshots done. Processing live stream.")

                    async def heartbeat():
                        while True:
                            await asyncio.sleep(self.flush_interval)
                            self._flush()

                    heartbeat_task = asyncio.create_task(heartbeat())

                    async for message in ws:
                        data = json.loads(message)
                        stream_name = data.get("stream", "")
                        payload = data.get("data", {})
                        asset = stream_name.split("@")[0]

                        if "@trade" in stream_name:
                            self._handle_trade(asset, payload)
                        elif "@depth" in stream_name:
                            self._handle_depth(asset, payload)

            except Exception as e:
                logger.error(f"Stream error: {e}. Reconnecting in {reconnect_delay}s...")
            finally:
                if heartbeat_task and not heartbeat_task.done():
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass
                self._flush(force=True)

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

    def run(self):
        """Start the streamer. Blocks until interrupted."""
        logger.info(f"Starting LOBStreamer")
        logger.info(f"Assets  : {[a.upper() for a in self.assets]}")
        logger.info(f"Output  : {self.output}")
        if self.output == "local":
            logger.info(f"Dir     : {self.output_dir}")
        else:
            logger.info(f"Bucket  : {self.bucket}")
        logger.info(f"Flush   : {self.flush_interval}s")
        try:
            asyncio.run(self._stream())
        except KeyboardInterrupt:
            print("\nStopped. Final buffers flushed.")