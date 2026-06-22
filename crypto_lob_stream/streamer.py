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

from .exchanges import get_exchange
from .schemas import DEPTH_SCHEMA, SNAPSHOT_SCHEMA, TRADE_SCHEMA
from .writers import retry_gcs_fallbacks, write_gcs, write_local

logger = logging.getLogger(__name__)

OutputType = Literal["local", "gcs"]


class LOBStreamer:
    """Stream order book and trade data from a crypto exchange to local disk or GCS.

    Parameters
    ----------
    assets : list of str
        Trading pair symbols. Format is normalised per exchange, so you can
        pass "BTCUSDT" or "BTC-USD" and the adapter converts as needed.
    exchange : str
        Exchange to stream from. Default "binance". Also supports "coinbase".
    output : "local" or "gcs"
        Where to write Parquet files.
    output_dir : str, optional
        Base directory for local output. Defaults to "./lob_data".
    bucket : str, optional
        GCS bucket name. Required when output="gcs".
    fallback_dir : str, optional
        Local fallback directory used when GCS uploads fail.
    flush_interval : int
        Seconds between buffer flushes. Default 300 (5 minutes).
    on_trade : callable, optional
        Optional callback invoked for every normalised trade record.
    on_depth : callable, optional
        Optional callback invoked for every normalised depth record.
    log_dir : str
        Directory for rotating log files. Default "./logs".
    """

    def __init__(
        self,
        assets: List[str],
        exchange: str = "binance",
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

        # Resolve the exchange adapter (raises ValueError on unknown name)
        self.exchange = get_exchange(exchange)

        # Normalise each asset to the exchange's native symbol format
        self.assets = [self.exchange.normalize_symbol(a) for a in assets]

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

        # Tracks which assets have had their initial snapshot captured,
        # for exchanges that deliver the snapshot over the socket.
        self._snapshot_taken: set = set()

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

    # ── Snapshot fetch (REST-based exchanges, e.g. Binance) ─────────────────────

    async def _fetch_snapshot(self, asset: str):
        url = self.exchange.snapshot_url(asset)
        if not url:
            # Exchange delivers its snapshot over the socket; nothing to fetch.
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.error(f"Snapshot HTTP {resp.status} for {asset}")
                        return
                    raw = await resp.json()
        except Exception as e:
            logger.error(f"Snapshot fetch failed for {asset}: {e}")
            return

        records = self.exchange.parse_snapshot(asset, raw)
        if not records:
            return
        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
        self._write(records, SNAPSHOT_SCHEMA, "snapshots", asset, ts_str)
        last_uid = records[0].get("last_update_id", 0)
        logger.info(
            f"Snapshot stored for {asset.upper()} | "
            f"lastUpdateId={last_uid} | levels={len(records)}"
        )

    def _store_socket_snapshot(self, asset: str, raw: dict):
        """Store a snapshot delivered over the WebSocket (e.g. Coinbase)."""
        records = self.exchange.parse_snapshot(asset, raw)
        if not records:
            return
        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
        self._write(records, SNAPSHOT_SCHEMA, "snapshots", asset, ts_str)
        logger.info(
            f"Snapshot stored for {asset.upper()} | levels={len(records)}"
        )

    # ── Normalised record dispatch ──────────────────────────────────────────────

    def _ingest(self, record: dict):
        rtype = record.get("type")
        asset = record["asset"]
        if rtype == "trade":
            clean = {
                "timestamp_ms": record["timestamp_ms"],
                "asset":        asset,
                "trade_id":     record["trade_id"],
                "price":        record["price"],
                "quantity":     record["quantity"],
                "buyer_maker":  record["buyer_maker"],
            }
            if self.on_trade:
                self.on_trade(clean)
            self._trade_buffer[asset].append(clean)
        elif rtype == "depth":
            clean = {
                "timestamp_ms":    record["timestamp_ms"],
                "asset":           asset,
                "side":            record["side"],
                "price":           record["price"],
                "quantity":        record["quantity"],
                "first_update_id": record["first_update_id"],
                "last_update_id":  record["last_update_id"],
            }
            if self.on_depth:
                self.on_depth(clean)
            self._depth_buffer[asset].append(clean)

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
                    f"[{flush_time}] {asset.upper():12s} "
                    f"trades: {trades_saved:>7,} | "
                    f"depth events: {depth_saved:>9,} | saved"
                )

        self._last_flush = now

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _stream(self):
        url = self.exchange.ws_url(self.assets)
        subscribe_msgs = self.exchange.subscribe_messages(self.assets)
        reconnect_delay = 5

        if self.output == "gcs":
            retry_gcs_fallbacks(self.fallback_dir, self.bucket)

        while True:
            heartbeat_task = None
            try:
                logger.info(f"Connecting to {self.exchange.name} WebSocket...")
                async with websockets.connect(
                    url, ping_interval=180, max_size=self.exchange.ws_max_size
                ) as ws:
                    logger.info(
                        f"Connected. Streaming: {[a.upper() for a in self.assets]}"
                    )
                    reconnect_delay = 5

                    # Send subscription messages if the exchange needs them
                    for msg in subscribe_msgs:
                        await ws.send(json.dumps(msg))

                    # REST snapshots (no-op for socket-snapshot exchanges)
                    snapshot_urls = [self.exchange.snapshot_url(a) for a in self.assets]
                    if any(snapshot_urls):
                        logger.info("Fetching order book snapshots...")
                        await asyncio.gather(
                            *[self._fetch_snapshot(a) for a in self.assets]
                        )
                        logger.info("Snapshots done. Processing live stream.")
                    else:
                        logger.info("Processing live stream (snapshot via socket).")

                    async def heartbeat():
                        while True:
                            await asyncio.sleep(self.flush_interval)
                            self._flush()

                    heartbeat_task = asyncio.create_task(heartbeat())

                    async for message in ws:
                        # Some exchanges (OKX) send a literal "pong" or expect
                        # "ping" text frames; ignore non-JSON control frames.
                        if message == "pong" or message == "ping":
                            continue
                        try:
                            raw = json.loads(message)
                        except (ValueError, TypeError):
                            continue

                        # Ignore subscription acks / errors / control frames
                        if isinstance(raw, dict):
                            # OKX-style event acks
                            if raw.get("event") in ("subscribe", "error"):
                                continue
                            # Kraken-style method acks (subscribe/unsubscribe)
                            if "method" in raw and "data" not in raw:
                                continue
                            # Kraken heartbeat/status channels carry no book/trade data
                            if raw.get("channel") in ("heartbeat", "status"):
                                continue

                        # Handle socket-delivered snapshots (Coinbase, OKX)
                        if self.exchange.is_socket_snapshot(raw):
                            asset = self.exchange.socket_snapshot_asset(raw)
                            if asset and asset not in self._snapshot_taken:
                                self._store_socket_snapshot(asset, raw)
                                self._snapshot_taken.add(asset)
                            continue

                        for record in self.exchange.parse_message(raw):
                            self._ingest(record)

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
                # Allow snapshots to be re-taken on reconnect
                self._snapshot_taken.clear()

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

    def run(self):
        """Start the streamer. Blocks until interrupted."""
        logger.info(f"Starting LOBStreamer")
        logger.info(f"Exchange: {self.exchange.name}")
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