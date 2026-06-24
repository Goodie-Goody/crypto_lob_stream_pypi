import asyncio

import pytest
from crypto_lob_stream import LOBStreamer


def test_instantiation_local():
    s = LOBStreamer(assets=["BTCUSDT"], output="local")
    assert s.assets == ["BTCUSDT"]
    assert s.output == "local"


def test_instantiation_gcs():
    s = LOBStreamer(assets=["BTCUSDT"], output="gcs", bucket="test-bucket")
    assert s.bucket == "test-bucket"


def test_default_exchange_is_binance():
    s = LOBStreamer(assets=["BTCUSDT"])
    assert s.exchange.name == "binance"


def test_coinbase_exchange():
    s = LOBStreamer(assets=["BTC-USD"], exchange="coinbase")
    assert s.exchange.name == "coinbase"
    assert s.assets == ["BTC-USD"]


def test_unknown_exchange_raises():
    with pytest.raises(ValueError, match="Unknown exchange"):
        LOBStreamer(assets=["BTCUSDT"], exchange="ftx")


def test_no_assets_raises():
    with pytest.raises(ValueError, match="At least one asset"):
        LOBStreamer(assets=[])


def test_gcs_without_bucket_raises():
    with pytest.raises(ValueError, match="bucket is required"):
        LOBStreamer(assets=["BTCUSDT"], output="gcs")


def test_invalid_output_raises():
    with pytest.raises(ValueError, match="output must be"):
        LOBStreamer(assets=["BTCUSDT"], output="s3")


def test_assets_normalized_binance():
    s = LOBStreamer(assets=["btcusdt", "BTC-USDT"])
    # Both normalise to Binance concatenated uppercase
    assert s.assets == ["BTCUSDT", "BTCUSDT"]


def test_ingest_trade():
    s = LOBStreamer(assets=["BTCUSDT"], output="local")
    s._ingest("binance", s.exchange, {
        "type": "trade", "asset": "BTCUSDT", "timestamp_ms": 1700000000000,
        "trade_id": 123456, "price": 65000.0, "quantity": 0.01, "buyer_maker": False,
    })
    assert len(s._trade_buffer["binance:BTCUSDT"]) == 1
    assert s._trade_buffer["binance:BTCUSDT"][0]["price"] == 65000.0
    assert s._trade_buffer["binance:BTCUSDT"][0]["exchange"] == "binance"


def test_ingest_depth():
    s = LOBStreamer(assets=["BTCUSDT"], output="local")
    s._ingest("binance", s.exchange, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 1700000000000,
        "side": "bid", "price": 65000.0, "quantity": 0.5,
        "first_update_id": 1000, "last_update_id": 1005,
    })
    assert len(s._depth_buffer["binance:BTCUSDT"]) == 1
    assert s._depth_buffer["binance:BTCUSDT"][0]["last_update_id"] == 1005


def test_on_trade_callback():
    received = []
    s = LOBStreamer(assets=["BTCUSDT"], output="local", on_trade=received.append)
    s._ingest("binance", s.exchange, {
        "type": "trade", "asset": "BTCUSDT", "timestamp_ms": 1700000000000,
        "trade_id": 1, "price": 100.0, "quantity": 1.0, "buyer_maker": True,
    })
    assert len(received) == 1
    assert received[0]["price"] == 100.0


def test_on_depth_callback():
    received = []
    s = LOBStreamer(assets=["BTCUSDT"], output="local", on_depth=received.append)
    s._ingest("binance", s.exchange, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 1700000000000,
        "side": "bid", "price": 100.0, "quantity": 1.0,
        "first_update_id": 1, "last_update_id": 2,
    })
    assert len(received) == 1
    assert received[0]["side"] == "bid"


# ── Multi-exchange feeds ─────────────────────────────────────────────────────

def test_multi_exchange_feeds_built():
    s = LOBStreamer(
        exchanges=[
            {"exchange": "binance", "assets": ["BTCUSDT", "ETHUSDT"]},
            {"exchange": "kraken", "assets": ["BTC/USD"]},
        ],
        output="local",
    )
    assert len(s.feeds) == 2
    names = sorted(f.exchange.name for f in s.feeds)
    assert names == ["binance", "kraken"]
    binance_feed = next(f for f in s.feeds if f.exchange.name == "binance")
    assert binance_feed.assets == ["BTCUSDT", "ETHUSDT"]
    # Backward-compat single-exchange attrs reflect the first feed
    assert s.exchange.name == "binance"
    assert s.assets == ["BTCUSDT", "ETHUSDT"]


def test_multi_exchange_requires_assets_per_entry():
    with pytest.raises(ValueError, match="no assets"):
        LOBStreamer(exchanges=[{"exchange": "binance", "assets": []}], output="local")


def test_multi_exchange_buffers_dont_collide():
    s = LOBStreamer(
        exchanges=[
            {"exchange": "binance", "assets": ["BTCUSDT"]},
            {"exchange": "bybit", "assets": ["BTCUSDT"]},
        ],
        output="local",
    )
    binance_exch = next(f.exchange for f in s.feeds if f.exchange.name == "binance")
    bybit_exch = next(f.exchange for f in s.feeds if f.exchange.name == "bybit")
    s._ingest("binance", binance_exch, {
        "type": "trade", "asset": "BTCUSDT", "timestamp_ms": 1,
        "trade_id": 1, "price": 1.0, "quantity": 1.0, "buyer_maker": False,
    })
    s._ingest("bybit", bybit_exch, {
        "type": "trade", "asset": "BTCUSDT", "timestamp_ms": 1,
        "trade_id": 2, "price": 2.0, "quantity": 1.0, "buyer_maker": False,
    })
    assert len(s._trade_buffer["binance:BTCUSDT"]) == 1
    assert len(s._trade_buffer["bybit:BTCUSDT"]) == 1
    assert s._trade_buffer["binance:BTCUSDT"][0]["price"] == 1.0
    assert s._trade_buffer["bybit:BTCUSDT"][0]["price"] == 2.0


# ── Gap detection ────────────────────────────────────────────────────────────

def test_gap_detected_on_sequence_break():
    s = LOBStreamer(assets=["BTCUSDT"], output="local", resync_on_gap=False)
    exch = s.exchange  # binance, has_sequence_ids=True
    s._ingest("binance", exch, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 1,
        "side": "bid", "price": 1.0, "quantity": 1.0,
        "first_update_id": 100, "last_update_id": 110,
    })
    # Next message should start at 111; it starts at 115 instead -> gap of 4
    s._ingest("binance", exch, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 2,
        "side": "ask", "price": 2.0, "quantity": 1.0,
        "first_update_id": 115, "last_update_id": 120,
    })
    gaps = s._gap_buffer["binance:BTCUSDT"]
    assert len(gaps) == 1
    assert gaps[0]["expected_update_id"] == 111
    assert gaps[0]["received_update_id"] == 115
    assert gaps[0]["gap_size"] == 4


def test_no_gap_when_contiguous():
    s = LOBStreamer(assets=["BTCUSDT"], output="local", resync_on_gap=False)
    exch = s.exchange
    s._ingest("binance", exch, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 1,
        "side": "bid", "price": 1.0, "quantity": 1.0,
        "first_update_id": 100, "last_update_id": 110,
    })
    s._ingest("binance", exch, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 2,
        "side": "bid", "price": 1.0, "quantity": 1.0,
        "first_update_id": 111, "last_update_id": 120,
    })
    assert s._gap_buffer["binance:BTCUSDT"] == []


def test_same_batch_not_double_checked():
    # Two rows (bid + ask) from the *same* message share first/last_update_id
    # and must not be treated as two separate batches.
    s = LOBStreamer(assets=["BTCUSDT"], output="local", resync_on_gap=False)
    exch = s.exchange
    s._ingest("binance", exch, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 1,
        "side": "bid", "price": 1.0, "quantity": 1.0,
        "first_update_id": 100, "last_update_id": 110,
    })
    s._ingest("binance", exch, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 1,
        "side": "ask", "price": 2.0, "quantity": 1.0,
        "first_update_id": 100, "last_update_id": 110,
    })
    assert s._gap_buffer["binance:BTCUSDT"] == []


def test_gap_detection_noop_for_exchanges_without_sequence_ids():
    s = LOBStreamer(assets=["BTC-USD"], exchange="coinbase", output="local", resync_on_gap=False)
    exch = s.exchange
    assert exch.has_sequence_ids is False
    s._ingest("coinbase", exch, {
        "type": "depth", "asset": "BTC-USD", "timestamp_ms": 1,
        "side": "bid", "price": 1.0, "quantity": 1.0,
        "first_update_id": 1, "last_update_id": 1,
    })
    s._ingest("coinbase", exch, {
        "type": "depth", "asset": "BTC-USD", "timestamp_ms": 9999,
        "side": "bid", "price": 1.0, "quantity": 1.0,
        "first_update_id": 9999, "last_update_id": 9999,
    })
    assert s._gap_buffer["coinbase:BTC-USD"] == []


def test_on_gap_callback():
    received = []
    s = LOBStreamer(
        assets=["BTCUSDT"], output="local", resync_on_gap=False, on_gap=received.append
    )
    exch = s.exchange
    s._ingest("binance", exch, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 1,
        "side": "bid", "price": 1.0, "quantity": 1.0,
        "first_update_id": 100, "last_update_id": 110,
    })
    s._ingest("binance", exch, {
        "type": "depth", "asset": "BTCUSDT", "timestamp_ms": 2,
        "side": "bid", "price": 1.0, "quantity": 1.0,
        "first_update_id": 120, "last_update_id": 130,
    })
    assert len(received) == 1
    assert received[0]["gap_size"] == 9


# ── Kraken checksum verification (LOBStreamer-level wiring) ─────────────────

def test_maybe_verify_checksum_logs_mismatch():
    s = LOBStreamer(assets=["BTC/USD"], exchange="kraken", output="local", verify_checksums=True)
    exch = s.exchange
    raw = {
        "channel": "book",
        "type": "snapshot",
        "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": "65000.0", "qty": "0.5"}],
            "asks": [{"price": "65001.0", "qty": "0.3"}],
            "checksum": 1,  # deliberately wrong
        }],
    }
    s._maybe_verify_checksum(exch, raw, "snapshot")
    failures = s._checksum_buffer["kraken:BTC/USD"]
    assert len(failures) == 1
    assert failures[0]["received"] == 1
    assert failures[0]["expected"] != 1


def test_maybe_verify_checksum_noop_when_disabled():
    s = LOBStreamer(assets=["BTC/USD"], exchange="kraken", output="local", verify_checksums=False)
    exch = s.exchange
    raw = {
        "channel": "book", "type": "snapshot",
        "data": [{"symbol": "BTC/USD", "bids": [], "asks": [], "checksum": 1}],
    }
    s._maybe_verify_checksum(exch, raw, "snapshot")
    assert s._checksum_buffer == {}


def test_maybe_verify_checksum_noop_for_unsupported_exchange():
    s = LOBStreamer(assets=["BTCUSDT"], exchange="binance", output="local", verify_checksums=True)
    exch = s.exchange
    assert exch.supports_checksum is False
    raw = {
        "channel": "book", "type": "snapshot",
        "data": [{"symbol": "BTCUSDT", "bids": [], "asks": [], "checksum": 1}],
    }
    s._maybe_verify_checksum(exch, raw, "snapshot")
    assert s._checksum_buffer == {}


# ── Futures / funding records ───────────────────────────────────────────────

def test_futures_exchange_registered():
    s = LOBStreamer(assets=["BTCUSDT"], exchange="binance_futures", output="local")
    assert s.exchange.name == "binance_futures"
    assert "fapi.binance.com" in s.exchange.snapshot_url("BTCUSDT")


def test_ingest_funding_record():
    s = LOBStreamer(assets=["BTCUSDT"], exchange="binance_futures", output="local")
    s._ingest("binance_futures", s.exchange, {
        "type": "funding", "asset": "BTCUSDT", "timestamp_ms": 1700000000000,
        "mark_price": 65010.5, "funding_rate": 0.0001, "next_funding_ms": 1700001800000,
    })
    funding = s._funding_buffer["binance_futures:BTCUSDT"]
    assert len(funding) == 1
    assert funding[0]["funding_rate"] == 0.0001
    assert funding[0]["exchange"] == "binance_futures"


# ── Config tests ──────────────────────────────────────────────────────────────

import json
import tempfile
import os
from crypto_lob_stream.config import (
    apply_credentials,
    load_config,
    save_config,
    check_gcs_connection,
)


def test_save_and_load_config(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "crypto_lob_stream.config.CONFIG_FILE",
        tmp_path / "config.json"
    )
    monkeypatch.setattr(
        "crypto_lob_stream.config.CONFIG_DIR",
        tmp_path
    )
    save_config({"gcs_bucket": "test-bucket", "gcs_credentials_path": "/tmp/key.json"})
    cfg = load_config()
    assert cfg["gcs_bucket"] == "test-bucket"
    assert cfg["gcs_credentials_path"] == "/tmp/key.json"


def test_load_config_missing_file(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "crypto_lob_stream.config.CONFIG_FILE",
        tmp_path / "nonexistent.json"
    )
    assert load_config() == {}


def test_apply_credentials_sets_env(tmp_path, monkeypatch):
    # Create a fake credentials file
    creds_file = tmp_path / "key.json"
    creds_file.write_text("{}")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    apply_credentials(str(creds_file))
    assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == str(creds_file)


def test_apply_credentials_missing_file(tmp_path, monkeypatch):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    import pytest
    with pytest.raises(FileNotFoundError):
        apply_credentials(str(tmp_path / "nonexistent.json"))


def test_apply_credentials_env_already_set(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/already/set.json")
    # Should not raise even though file doesn't exist -- env takes precedence
    apply_credentials("/some/other/path.json")
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "/already/set.json"


def test_gcs_connection_no_library(monkeypatch):
    import builtins
    real_import = builtins.__import__
    def mock_import(name, *args, **kwargs):
        if name == "google.cloud.storage" or name == "google.cloud":
            raise ImportError("mocked")
        return real_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", mock_import)
    ok, msg = check_gcs_connection("any-bucket")
    assert not ok
    assert "not installed" in msg.lower() or "import" in msg.lower() or isinstance(msg, str)

def test_bybit_consecutive_real_messages_not_falsely_flagged_as_gap():
    # Regression test for a real bug found via live testing: using
    # Bybit's "seq" (cross sequence) for first_update_id instead of "u"
    # (the real per-symbol counter) made every consecutive live message
    # look like a gap of billions of missed updates, since the two
    # counters are on completely different numeric scales. Uses the
    # streamer's actual gap-detection path (not just the exchange parser)
    # with realistic u/seq values taken from Bybit's own sample payloads.
    s = LOBStreamer(exchange="bybit_linear", assets=["BTCUSDT"], output="local", resync_on_gap=False)
    exch = s.exchange

    msg1 = {"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": 1,
            "data": {"s": "BTCUSDT", "u": 18521288, "seq": 7961638724,
                       "b": [["1.0", "1.0"]], "a": []}}
    msg2 = {"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": 2,
            "data": {"s": "BTCUSDT", "u": 18521289, "seq": 7961638999,
                       "b": [["1.0", "1.0"]], "a": []}}

    for record in exch.parse_message(msg1):
        s._ingest(exch.name, exch, record)
    for record in exch.parse_message(msg2):
        s._ingest(exch.name, exch, record)

    assert s._gap_buffer.get("bybit_linear:BTCUSDT", []) == []


# ── Liquidations & open interest ─────────────────────────────────────────────

def test_ingest_liquidation_record():
    s = LOBStreamer(exchange="binance_futures", assets=["BTCUSDT"], output="local")
    s._ingest("binance_futures", s.exchange, {
        "type": "liquidation", "asset": "BTCUSDT", "timestamp_ms": 1,
        "side": "sell", "price": 65000.0, "quantity": 0.5,
    })
    liqs = s._liquidation_buffer["binance_futures:BTCUSDT"]
    assert len(liqs) == 1
    assert liqs[0]["side"] == "sell"
    assert liqs[0]["exchange"] == "binance_futures"


def test_ingest_open_interest_record():
    s = LOBStreamer(exchange="binance_futures", assets=["BTCUSDT"], output="local")
    s._ingest("binance_futures", s.exchange, {
        "type": "open_interest", "asset": "BTCUSDT", "timestamp_ms": 1,
        "open_interest": 10000.0, "open_interest_value": None,
    })
    oi = s._open_interest_buffer["binance_futures:BTCUSDT"]
    assert len(oi) == 1
    assert oi[0]["open_interest"] == 10000.0
    assert oi[0]["open_interest_value"] is None


def test_liquidation_and_open_interest_flush_to_parquet(tmp_path):
    s = LOBStreamer(exchange="bybit_linear", assets=["BTCUSDT"], output="local",
                     output_dir=str(tmp_path), flush_interval=9999)
    s._ingest("bybit_linear", s.exchange, {
        "type": "liquidation", "asset": "BTCUSDT", "timestamp_ms": 1,
        "side": "buy", "price": 1.0, "quantity": 1.0,
    })
    s._ingest("bybit_linear", s.exchange, {
        "type": "open_interest", "asset": "BTCUSDT", "timestamp_ms": 1,
        "open_interest": 1.0, "open_interest_value": 2.0,
    })
    s._flush(force=True)

    import pyarrow.parquet as pq
    liq_files = list((tmp_path / "liquidations" / "bybit_linear" / "BTCUSDT").glob("*.parquet"))
    oi_files = list((tmp_path / "open_interest" / "bybit_linear" / "BTCUSDT").glob("*.parquet"))
    assert len(liq_files) == 1
    assert len(oi_files) == 1
    assert pq.read_table(str(liq_files[0])).num_rows == 1
    assert pq.read_table(str(oi_files[0])).num_rows == 1


def test_poll_open_interest_loop_noops_for_exchanges_without_rest_polling():
    # okx_swap and bybit_linear get open interest via WebSocket, not
    # REST -- the poll loop should return immediately rather than
    # spin forever doing nothing.
    s = LOBStreamer(exchange="okx_swap", assets=["BTC-USDT"], output="local")
    result = asyncio.run(
        asyncio.wait_for(s._poll_open_interest_loop(s.feeds[0]), timeout=1.0)
    )
    assert result is None  # returned (didn't hang) because nothing needs polling


def test_poll_open_interest_loop_polls_binance_futures(monkeypatch):
    s = LOBStreamer(
        exchange="binance_futures", assets=["BTCUSDT"], output="local",
        open_interest_poll_interval=0,
    )

    calls = []

    async def fake_poll(exch, asset):
        calls.append(asset)
        if len(calls) >= 2:
            raise asyncio.CancelledError()  # stop the infinite loop cleanly

    monkeypatch.setattr(s, "_poll_open_interest", fake_poll)

    async def run():
        try:
            await s._poll_open_interest_loop(s.feeds[0])
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert calls == ["BTCUSDT", "BTCUSDT"]


def test_poll_open_interest_fetches_and_ingests(monkeypatch):
    s = LOBStreamer(exchange="binance_futures", assets=["BTCUSDT"], output="local")

    class FakeResp:
        status = 200
        async def json(self):
            return {"symbol": "BTCUSDT", "openInterest": "123.45", "time": 1}
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False

    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        def get(self, url, timeout=None):
            return FakeResp()

    monkeypatch.setattr(
        "crypto_lob_stream.streamer.aiohttp.ClientSession", lambda: FakeSession()
    )

    asyncio.run(s._poll_open_interest(s.exchange, "BTCUSDT"))

    oi = s._open_interest_buffer["binance_futures:BTCUSDT"]
    assert len(oi) == 1
    assert oi[0]["open_interest"] == 123.45