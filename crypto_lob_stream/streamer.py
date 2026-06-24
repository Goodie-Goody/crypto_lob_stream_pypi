import asyncio
import json
import logging
import logging.handlers
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Literal, Optional

import aiohttp
import websockets

from .exchanges import Exchange, get_exchange
from .schemas import (
    CHECKSUM_SCHEMA,
    DEPTH_SCHEMA,
    FUNDING_SCHEMA,
    GAP_SCHEMA,
    LIQUIDATION_SCHEMA,
    OPEN_INTEREST_SCHEMA,
    SNAPSHOT_SCHEMA,
    TRADE_SCHEMA,
)
from .writers import retry_gcs_fallbacks, write_gcs, write_local

logger = logging.getLogger(__name__)

OutputType = Literal["local", "gcs"]


@dataclass
class _Feed:
    """One exchange + its asset list, run as an independent task inside the
    same asyncio loop and process as every other feed."""
    exchange: Exchange
    assets: List[str]


class LOBStreamer:
    """Stream order book and trade data from one or more crypto exchanges to
    local disk or GCS, all within a single process and asyncio event loop.

    Parameters
    ----------
    assets : list of str, optional
        Trading pair symbols for a single exchange. Format is normalised
        per exchange, so you can pass "BTCUSDT" or "BTC-USD" and the
        adapter converts as needed. Ignored if `exchanges` is given.
    exchange : str
        Exchange to stream from when using the single-exchange form.
        Default "binance". Ignored if `exchanges` is given.
    exchanges : list of dict, optional
        Run multiple exchanges concurrently in this one process/event loop.
        Each entry: {"exchange": "binance", "assets": ["BTCUSDT", ...]}.
        Symbol formats differ per exchange, so each entry carries its own
        asset list rather than sharing one across exchanges. When given,
        `assets`/`exchange` above are ignored. Output paths and buffers are
        always keyed by (exchange, asset), so running 5 exchanges in one
        process produces the same on-disk layout as running 5 separate
        single-exchange processes -- this is purely about process count,
        not data shape.
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
    open_interest_poll_interval : int
        Seconds between open-interest polls, for exchanges with no
        WebSocket push for it (currently only binance_futures -- OKX
        swap and Bybit linear deliver open interest over their existing
        WebSocket channels, no polling involved). Default 30. Has no
        effect at all for exchanges that don't need REST polling for this.
    on_trade : callable, optional
        Optional callback invoked for every normalised trade record.
    on_depth : callable, optional
        Optional callback invoked for every normalised depth record.
    on_gap : callable, optional
        Optional callback invoked when a sequence gap is detected (see
        `detect_gaps`). Receives the same dict that's written to the gaps
        Parquet table.
    on_checksum_fail : callable, optional
        Optional callback invoked when a live book checksum mismatch is
        detected (see `verify_checksums`).
    detect_gaps : bool
        Default True. For exchanges with real sequence numbers (currently
        Binance, Bybit, OKX -- see Exchange.has_sequence_ids), checks that
        each depth message's first_update_id chains from the previous
        message's last_update_id, including the gap between the initial
        snapshot and the first live update. Gaps are logged and written to
        a "gaps" Parquet table. No-op for exchanges without real sequence
        numbers (Coinbase, Kraken).
    resync_on_gap : bool
        Default True. When a gap is detected on an exchange with a REST
        snapshot endpoint (currently Binance/Binance Futures), automatically
        re-fetches a fresh snapshot for the affected asset so downstream
        consumers have a known-good resync point. For socket-snapshot
        exchanges (OKX, Bybit) there's no independent re-fetch available
        without a full reconnect, so this currently only logs/records the
        gap for those -- see README for the caveat.
    verify_checksums : bool
        Default False. For exchanges that implement it (currently only
        Kraken -- see Exchange.supports_checksum), maintains a live
        top-of-book mirror and verifies it against the exchange-supplied
        CRC32 checksum on every book message. Mismatches are logged and
        written to a "checksums" Parquet table. This never blocks or drops
        data; it's purely an integrity signal layered on top.
    log_dir : str
        Directory for rotating log files. Default "./logs".
    """

    def __init__(
        self,
        assets: Optional[List[str]] = None,
        exchange: str = "binance",
        exchanges: Optional[List[dict]] = None,
        output: OutputType = "local",
        output_dir: str = "./lob_data",
        bucket: Optional[str] = None,
        fallback_dir: str = "./lob_fallback",
        flush_interval: int = 300,
        open_interest_poll_interval: int = 30,
        on_trade: Optional[Callable] = None,
        on_depth: Optional[Callable] = None,
        on_gap: Optional[Callable] = None,
        on_checksum_fail: Optional[Callable] = None,
        detect_gaps: bool = True,
        resync_on_gap: bool = True,
        verify_checksums: bool = False,
        log_dir: str = "./logs",
    ):
        if output == "gcs" and not bucket:
            raise ValueError("bucket is required when output='gcs'.")
        if output not in ("local", "gcs"):
            raise ValueError("output must be 'local' or 'gcs'.")

        self.feeds: List[_Feed] = []
        if exchanges:
            for spec in exchanges:
                exch_name = spec["exchange"]
                spec_assets = spec.get("assets") or []
                if not spec_assets:
                    raise ValueError(
                        f"exchanges entry for '{exch_name}' has no assets."
                    )
                exch = get_exchange(exch_name)
                self.feeds.append(_Feed(
                    exchange=exch,
                    assets=[exch.normalize_symbol(a) for a in spec_assets],
                ))
            if not self.feeds:
                raise ValueError("exchanges must contain at least one entry.")
        else:
            if not assets:
                raise ValueError("At least one asset must be specified.")
            exch = get_exchange(exchange)
            self.feeds.append(_Feed(
                exchange=exch,
                assets=[exch.normalize_symbol(a) for a in assets],
            ))

        # Backward-compatible single-exchange attributes (always reflect the
        # first/only feed; meaningful whenever there's exactly one).
        self.exchange = self.feeds[0].exchange
        self.assets = self.feeds[0].assets

        self._exchanges_by_name = {f.exchange.name: f.exchange for f in self.feeds}

        self.output = output
        self.output_dir = output_dir
        self.bucket = bucket
        self.fallback_dir = fallback_dir
        self.flush_interval = flush_interval
        self.open_interest_poll_interval = open_interest_poll_interval
        self.on_trade = on_trade
        self.on_depth = on_depth
        self.on_gap = on_gap
        self.on_checksum_fail = on_checksum_fail
        self.detect_gaps = detect_gaps
        self.resync_on_gap = resync_on_gap
        self.verify_checksums = verify_checksums

        # Buffers are keyed by "{exchange}:{asset}" so the same symbol on
        # two different exchanges (e.g. BTCUSDT on both Binance and Bybit)
        # never collides.
        self._trade_buffer: dict = defaultdict(list)
        self._depth_buffer: dict = defaultdict(list)
        self._gap_buffer: dict = defaultdict(list)
        self._checksum_buffer: dict = defaultdict(list)
        self._funding_buffer: dict = defaultdict(list)
        self._liquidation_buffer: dict = defaultdict(list)
        self._open_interest_buffer: dict = defaultdict(list)
        self._last_flush: float = time.time()

        # Gap detection state, keyed the same way.
        self._last_update_id: dict = {}
        self._last_batch_seen: dict = {}

        # Tracks which (exchange, asset) keys have had their initial
        # snapshot captured, for exchanges that deliver it over the socket.
        self._snapshot_taken: set = set()

        self._setup_logging(log_dir)

    # ── Key helper ───────────────────────────────────────────────────────────

    @staticmethod
    def _key(exchange_name: str, asset: str) -> str:
        return f"{exchange_name}:{asset}"

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

    async def _fetch_snapshot(self, exch: Exchange, asset: str):
        url = exch.snapshot_url(asset)
        if not url:
            # Exchange delivers its snapshot over the socket; nothing to fetch.
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.error(f"Snapshot HTTP {resp.status} for {exch.name}/{asset}")
                        return
                    raw = await resp.json()
        except Exception as e:
            logger.error(f"Snapshot fetch failed for {exch.name}/{asset}: {e}")
            return

        records = exch.parse_snapshot(asset, raw)
        if not records:
            return
        for r in records:
            r["exchange"] = exch.name
        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
        self._write(records, SNAPSHOT_SCHEMA, "snapshots", exch.name, asset, ts_str)
        last_uid = records[0].get("last_update_id", 0)
        logger.info(
            f"Snapshot stored for {exch.name}/{asset.upper()} | "
            f"lastUpdateId={last_uid} | levels={len(records)}"
        )
        # Seed gap detection from the snapshot's own update id, so the very
        # first live message after the snapshot is checked for continuity
        # too -- that boundary is the single most likely place to miss
        # updates (REST fetch races the live stream).
        if self.detect_gaps and exch.has_sequence_ids:
            key = self._key(exch.name, asset)
            self._last_update_id[key] = last_uid
            self._last_batch_seen.pop(key, None)

    def _store_socket_snapshot(self, exch: Exchange, asset: str, raw: dict):
        """Store a snapshot delivered over the WebSocket (e.g. Coinbase)."""
        records = exch.parse_snapshot(asset, raw)
        if not records:
            return
        for r in records:
            r["exchange"] = exch.name
        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
        self._write(records, SNAPSHOT_SCHEMA, "snapshots", exch.name, asset, ts_str)
        logger.info(
            f"Snapshot stored for {exch.name}/{asset.upper()} | levels={len(records)}"
        )
        if self.detect_gaps and exch.has_sequence_ids:
            last_uid = records[0].get("last_update_id", 0)
            key = self._key(exch.name, asset)
            self._last_update_id[key] = last_uid
            self._last_batch_seen.pop(key, None)

    # ── Gap detection ────────────────────────────────────────────────────────

    def _check_gap(self, exch: Exchange, asset: str, first_update_id: int, last_update_id: int):
        key = self._key(exch.name, asset)
        batch = (first_update_id, last_update_id)
        if self._last_batch_seen.get(key) == batch:
            # Same message's other side of the book -- already checked.
            return
        self._last_batch_seen[key] = batch

        last_seen = self._last_update_id.get(key)
        if last_seen is not None and first_update_id > last_seen + 1:
            gap_size = first_update_id - last_seen - 1
            ts = int(time.time() * 1000)
            record = {
                "timestamp_ms":        ts,
                "exchange":            exch.name,
                "asset":               asset,
                "expected_update_id":  last_seen + 1,
                "received_update_id":  first_update_id,
                "gap_size":            gap_size,
            }
            self._gap_buffer[key].append(record)
            if self.on_gap:
                self.on_gap(record)
            logger.warning(
                f"GAP {exch.name}/{asset}: expected update_id "
                f"{last_seen + 1}, got {first_update_id} (missed {gap_size})"
            )
            if self.resync_on_gap and exch.snapshot_url(asset):
                logger.info(f"Resyncing {exch.name}/{asset} via fresh snapshot after gap...")
                asyncio.create_task(self._fetch_snapshot(exch, asset))
            elif self.resync_on_gap:
                logger.info(
                    f"{exch.name}/{asset} has no REST snapshot endpoint; "
                    f"gap recorded but not auto-resynced. A reconnect "
                    f"(automatic on the next stream error, or manual) is "
                    f"the way to get a fresh socket-delivered snapshot."
                )

        self._last_update_id[key] = last_update_id

    # ── Normalised record dispatch ──────────────────────────────────────────────

    def _ingest(self, exchange_name: str, exch: Exchange, record: dict):
        rtype = record.get("type")
        asset = record["asset"]
        key = self._key(exchange_name, asset)

        if rtype == "trade":
            clean = {
                "timestamp_ms": record["timestamp_ms"],
                "exchange":     exchange_name,
                "asset":        asset,
                "trade_id":     record["trade_id"],
                "price":        record["price"],
                "quantity":     record["quantity"],
                "buyer_maker":  record["buyer_maker"],
            }
            if self.on_trade:
                self.on_trade(clean)
            self._trade_buffer[key].append(clean)

        elif rtype == "depth":
            clean = {
                "timestamp_ms":    record["timestamp_ms"],
                "exchange":        exchange_name,
                "asset":           asset,
                "side":            record["side"],
                "price":           record["price"],
                "quantity":        record["quantity"],
                "first_update_id": record["first_update_id"],
                "last_update_id":  record["last_update_id"],
            }
            if self.detect_gaps and exch.has_sequence_ids:
                self._check_gap(exch, asset, record["first_update_id"], record["last_update_id"])
            if self.on_depth:
                self.on_depth(clean)
            self._depth_buffer[key].append(clean)

        elif rtype == "funding":
            clean = {
                "timestamp_ms":     record["timestamp_ms"],
                "exchange":         exchange_name,
                "asset":            asset,
                "mark_price":       record["mark_price"],
                "funding_rate":     record["funding_rate"],
                "next_funding_ms":  record["next_funding_ms"],
            }
            self._funding_buffer[key].append(clean)

        elif rtype == "liquidation":
            clean = {
                "timestamp_ms": record["timestamp_ms"],
                "exchange":     exchange_name,
                "asset":        asset,
                "side":         record["side"],
                "price":        record["price"],
                "quantity":     record["quantity"],
            }
            self._liquidation_buffer[key].append(clean)

        elif rtype == "open_interest":
            clean = {
                "timestamp_ms":         record["timestamp_ms"],
                "exchange":             exchange_name,
                "asset":                asset,
                "open_interest":        record["open_interest"],
                "open_interest_value":  record["open_interest_value"],
            }
            self._open_interest_buffer[key].append(clean)

    # ── Checksum verification (Kraken-style live book mirror) ───────────────

    def _maybe_verify_checksum(self, exch: Exchange, raw: dict, msg_type: str):
        if not (self.verify_checksums and getattr(exch, "supports_checksum", False)):
            return
        if not hasattr(exch, "update_book_and_checksum"):
            return
        for book in raw.get("data", []):
            asset, match, expected, received = exch.update_book_and_checksum(
                book, is_snapshot=(msg_type == "snapshot")
            )
            if match is None or asset is None:
                continue
            if match:
                logger.debug(f"Checksum OK {exch.name}/{asset} ({expected})")
                continue
            ts = int(time.time() * 1000)
            record = {
                "timestamp_ms": ts,
                "exchange":     exch.name,
                "asset":        asset,
                "expected":     expected,
                "received":     received,
            }
            key = self._key(exch.name, asset)
            self._checksum_buffer[key].append(record)
            if self.on_checksum_fail:
                self.on_checksum_fail(record)
            logger.warning(
                f"CHECKSUM MISMATCH {exch.name}/{asset}: "
                f"computed {expected}, exchange sent {received}"
            )

    # ── Write dispatch ────────────────────────────────────────────────────────

    def _write(self, records, schema, prefix, exchange_name, asset, ts_str):
        if self.output == "local":
            write_local(records, schema, self.output_dir, prefix, exchange_name, asset, ts_str)
        else:
            write_gcs(
                records, schema, self.bucket, prefix, exchange_name, asset, ts_str,
                fallback_dir=self.fallback_dir,
            )

    # ── Flush ─────────────────────────────────────────────────────────────────

    # (buffer, schema, prefix) -- iterated generically so adding a new
    # record type (as happened with gaps/checksums/funding) doesn't require
    # repeating the same flush block again.
    def _flush_targets(self):
        return [
            (self._trade_buffer,    TRADE_SCHEMA,    "trades"),
            (self._depth_buffer,    DEPTH_SCHEMA,    "depth"),
            (self._gap_buffer,      GAP_SCHEMA,      "gaps"),
            (self._checksum_buffer, CHECKSUM_SCHEMA, "checksums"),
            (self._funding_buffer,  FUNDING_SCHEMA,  "funding"),
            (self._liquidation_buffer,   LIQUIDATION_SCHEMA,   "liquidations"),
            (self._open_interest_buffer, OPEN_INTEREST_SCHEMA, "open_interest"),
        ]

    def _flush(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_flush) < self.flush_interval:
            return

        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
        flush_time = datetime.now(timezone.utc).strftime("%H:%M UTC")

        counts: dict = defaultdict(lambda: defaultdict(int))

        for buffer, schema, prefix in self._flush_targets():
            for key in list(buffer.keys()):
                records = buffer[key]
                if not records:
                    continue
                exchange_name, asset = key.split(":", 1)
                records_copy = records[:]
                if self.output == "local":
                    success = write_local(
                        records_copy, schema, self.output_dir, prefix,
                        exchange_name, asset, ts_str,
                    )
                else:
                    success = write_gcs(
                        records_copy, schema, self.bucket, prefix,
                        exchange_name, asset, ts_str,
                        fallback_dir=self.fallback_dir,
                    )
                if success:
                    counts[key][prefix] = len(records_copy)
                    buffer[key] = []

        for key, by_prefix in counts.items():
            exchange_name, asset = key.split(":", 1)
            parts = ", ".join(f"{p}: {n:,}" for p, n in by_prefix.items())
            print(f"[{flush_time}] {exchange_name}/{asset.upper():12s} {parts} | saved")

        self._last_flush = now

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _stream_feed(self, feed: _Feed):
        """Run one exchange's connection (covering all of that feed's
        assets) with its own independent reconnect loop. Multiple feeds run
        concurrently as sibling tasks inside the same event loop."""
        exch = feed.exchange
        assets = feed.assets
        url = exch.ws_url(assets)
        subscribe_msgs = exch.subscribe_messages(assets)
        reconnect_delay = 5

        while True:
            try:
                logger.info(f"Connecting to {exch.name} WebSocket...")
                async with websockets.connect(
                    url, ping_interval=180, max_size=exch.ws_max_size
                ) as ws:
                    logger.info(
                        f"[{exch.name}] Connected. Streaming: {[a.upper() for a in assets]}"
                    )
                    reconnect_delay = 5

                    for msg in subscribe_msgs:
                        await ws.send(json.dumps(msg))

                    snapshot_urls = [exch.snapshot_url(a) for a in assets]
                    if any(snapshot_urls):
                        logger.info(f"[{exch.name}] Fetching order book snapshots...")
                        await asyncio.gather(
                            *[self._fetch_snapshot(exch, a) for a in assets]
                        )
                        logger.info(f"[{exch.name}] Snapshots done. Processing live stream.")
                    else:
                        logger.info(f"[{exch.name}] Processing live stream (snapshot via socket).")

                    async for message in ws:
                        if message == "pong" or message == "ping":
                            continue
                        try:
                            raw = json.loads(message, parse_float=str)
                        except (ValueError, TypeError):
                            continue

                        if isinstance(raw, dict):
                            if raw.get("event") in ("subscribe", "error"):
                                continue
                            if "method" in raw and "data" not in raw:
                                continue
                            if raw.get("channel") in ("heartbeat", "status"):
                                continue

                        # Optional checksum verification runs on book
                        # snapshot/update messages regardless of which
                        # branch below ultimately consumes them -- it's a
                        # side observation, not part of the data pipeline.
                        if (
                            self.verify_checksums
                            and isinstance(raw, dict)
                            and raw.get("channel") == "book"
                            and raw.get("type") in ("snapshot", "update")
                        ):
                            self._maybe_verify_checksum(exch, raw, raw.get("type"))

                        if exch.is_socket_snapshot(raw):
                            asset = exch.socket_snapshot_asset(raw)
                            key = self._key(exch.name, asset)
                            if asset and key not in self._snapshot_taken:
                                self._store_socket_snapshot(exch, asset, raw)
                                self._snapshot_taken.add(key)
                            continue

                        for record in exch.parse_message(raw):
                            self._ingest(exch.name, exch, record)

            except Exception as e:
                logger.error(f"[{exch.name}] Stream error: {e}. Reconnecting in {reconnect_delay}s...")
            finally:
                # Allow snapshots to be re-taken on reconnect, for this feed only.
                self._snapshot_taken = {
                    k for k in self._snapshot_taken if not k.startswith(f"{exch.name}:")
                }

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

    async def _stream_aux_feed(self, feed: _Feed):
        """Run an exchange's optional second connection (e.g. Binance
        Futures' /market-routed markPrice stream -- see aux_ws_url() on
        Exchange). Same independent reconnect loop as the main feed, but
        simpler: no snapshots, no checksum/gap hooks, just parse-and-ingest."""
        exch = feed.exchange
        assets = feed.assets
        url = exch.aux_ws_url(assets)
        if not url:
            return
        subscribe_msgs = exch.aux_subscribe_messages(assets)
        reconnect_delay = 5

        while True:
            try:
                logger.info(f"[{exch.name}] Connecting to auxiliary WebSocket...")
                async with websockets.connect(
                    url, ping_interval=180, max_size=exch.ws_max_size
                ) as ws:
                    logger.info(f"[{exch.name}] Auxiliary connection established.")
                    reconnect_delay = 5

                    for msg in subscribe_msgs:
                        await ws.send(json.dumps(msg))

                    async for message in ws:
                        if message == "pong" or message == "ping":
                            continue
                        try:
                            raw = json.loads(message, parse_float=str)
                        except (ValueError, TypeError):
                            continue
                        if isinstance(raw, dict) and raw.get("event") in ("subscribe", "error"):
                            continue
                        for record in exch.parse_aux_message(raw):
                            self._ingest(exch.name, exch, record)

            except Exception as e:
                logger.error(
                    f"[{exch.name}] Auxiliary stream error: {e}. "
                    f"Reconnecting in {reconnect_delay}s..."
                )

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

    async def _poll_open_interest(self, exch: Exchange, asset: str):
        """Fetch and ingest one open-interest reading via REST, for
        exchanges with no WebSocket push for it (see Exchange.open_interest_url
        docstring -- currently only binance_futures uses this path)."""
        url = exch.open_interest_url(asset)
        if not url:
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.error(f"Open interest HTTP {resp.status} for {exch.name}/{asset}")
                        return
                    raw = await resp.json()
        except Exception as e:
            logger.error(f"Open interest fetch failed for {exch.name}/{asset}: {e}")
            return

        for record in exch.parse_open_interest(asset, raw):
            self._ingest(exch.name, exch, record)

    async def _poll_open_interest_loop(self, feed: _Feed):
        """Repeatedly poll open interest for every asset in this feed that
        the exchange doesn't push over WebSocket. No-ops entirely (never
        sleeps in a tight loop) for exchanges where open_interest_url()
        returns "" for all assets, since open interest for those arrives
        via parse_message/parse_aux_message instead."""
        exch = feed.exchange
        polled_assets = [a for a in feed.assets if exch.open_interest_url(a)]
        if not polled_assets:
            return
        while True:
            await asyncio.gather(*[self._poll_open_interest(exch, a) for a in polled_assets])
            await asyncio.sleep(self.open_interest_poll_interval)

    async def _heartbeat(self):
        while True:
            await asyncio.sleep(self.flush_interval)
            self._flush()

    async def _stream(self):
        if self.output == "gcs":
            retry_gcs_fallbacks(self.fallback_dir, self.bucket)

        tasks = [self._stream_feed(f) for f in self.feeds]
        for f in self.feeds:
            if f.exchange.aux_ws_url(f.assets):
                tasks.append(self._stream_aux_feed(f))
            if any(f.exchange.open_interest_url(a) for a in f.assets):
                tasks.append(self._poll_open_interest_loop(f))

        heartbeat_task = asyncio.create_task(self._heartbeat())
        try:
            await asyncio.gather(*tasks)
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            self._flush(force=True)

    def run(self):
        """Start the streamer. Blocks until interrupted."""
        logger.info(f"Starting LOBStreamer")
        for feed in self.feeds:
            logger.info(f"Exchange: {feed.exchange.name} | Assets: {[a.upper() for a in feed.assets]}")
        logger.info(f"Output  : {self.output}")
        if self.output == "local":
            logger.info(f"Dir     : {self.output_dir}")
        else:
            logger.info(f"Bucket  : {self.bucket}")
        logger.info(f"Flush   : {self.flush_interval}s")
        logger.info(f"Gap detection : {self.detect_gaps} (resync_on_gap={self.resync_on_gap})")
        logger.info(f"Checksum verify: {self.verify_checksums}")
        try:
            asyncio.run(self._stream())
        except KeyboardInterrupt:
            print("\nStopped. Final buffers flushed.")