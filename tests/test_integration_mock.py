"""
End-to-end smoke test of LOBStreamer._stream_feed with WebSocket and HTTP
mocked out -- exercises multi-exchange concurrency, gap detection (with
auto-resync), and checksum verification together, the way a real run would.

This is the closest thing to a live test that can run in CI/sandboxes
without real exchange connectivity. It does NOT replace validating against
the actual exchanges -- in particular it can't catch real protocol drift
or confirm the Kraken checksum format is exactly right, since the checksum
in the fake message is just made up. It DOES confirm the plumbing -- that
gap detection fires on a real sequence break, that a detected gap triggers
exactly one resync fetch, that checksum mismatches are computed and routed
to the right buffer, and that two feeds running concurrently in one event
loop never cross-contaminate each other's buffers.
"""

import asyncio
import json
from unittest.mock import patch

from crypto_lob_stream.streamer import LOBStreamer


class _FakeWS:
    def __init__(self, messages):
        self._messages = list(messages)

    async def send(self, msg):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            # Park forever rather than ending the stream -- the test cancels
            # the task explicitly once it's seen what it needs to see.
            await asyncio.sleep(3600)
        await asyncio.sleep(0)
        return self._messages.pop(0)


class _FakeConnectCtx:
    def __init__(self, ws):
        self.ws = ws

    async def __aenter__(self):
        return self.ws

    async def __aexit__(self, *a):
        return False


class _FakeResp:
    def __init__(self, payload, status=200):
        self.status = status
        self._payload = payload

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.get_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def get(self, url, timeout=None):
        self.get_calls += 1
        return _FakeResp(self.payload)


BINANCE_SNAPSHOT = {"lastUpdateId": 100, "bids": [["65000.0", "0.5"]], "asks": [["65001.0", "0.3"]]}

BINANCE_MSGS = [
    json.dumps({  # intentionally skips ahead (101-104 missing) to create a gap
        "stream": "btcusdt@depth@100ms",
        "data": {"U": 105, "u": 110, "b": [["65000.0", "0.4"]], "a": []},
    }),
    json.dumps({
        "stream": "btcusdt@trade",
        "data": {"T": 1, "t": 1, "p": "65000.0", "q": "0.1", "m": False},
    }),
]

KRAKEN_MSGS = [
    json.dumps({
        "channel": "book", "type": "snapshot",
        "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": 65000.0, "qty": 0.5}],
            "asks": [{"price": 65001.0, "qty": 0.3}],
            "checksum": 1,  # deliberately wrong, to exercise the mismatch path
        }],
    }),
    json.dumps({
        "channel": "trade", "type": "update",
        "data": [{"symbol": "BTC/USD", "side": "buy", "price": 65000.0,
                   "qty": 0.1, "trade_id": 1, "timestamp": "2026-06-12T11:05:57.123456Z"}],
    }),
]


def test_multi_exchange_gap_and_checksum_pipeline(tmp_path):
    asyncio.run(_run_multi_exchange_gap_and_checksum_pipeline(tmp_path))


async def _run_multi_exchange_gap_and_checksum_pipeline(tmp_path):
    s = LOBStreamer(
        exchanges=[
            {"exchange": "binance", "assets": ["BTCUSDT"]},
            {"exchange": "kraken", "assets": ["BTC/USD"]},
        ],
        output="local",
        output_dir=str(tmp_path),
        flush_interval=9999,
        verify_checksums=True,
        detect_gaps=True,
        resync_on_gap=True,
    )

    fake_session = _FakeSession(BINANCE_SNAPSHOT)

    def fake_connect(url, **kwargs):
        if "binance.com" in url:
            return _FakeConnectCtx(_FakeWS(list(BINANCE_MSGS)))
        return _FakeConnectCtx(_FakeWS(list(KRAKEN_MSGS)))

    with patch("crypto_lob_stream.streamer.websockets.connect", side_effect=fake_connect), \
         patch("crypto_lob_stream.streamer.aiohttp.ClientSession", return_value=fake_session):

        tasks = [asyncio.create_task(s._stream_feed(f)) for f in s.feeds]
        await asyncio.sleep(1.0)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    # Both feeds ran concurrently without cross-contaminating buffers.
    assert "binance:BTCUSDT" in s._trade_buffer
    assert "kraken:BTC/USD" in s._trade_buffer
    assert "binance:BTCUSDT" in s._depth_buffer

    # Gap detected exactly where the fake messages put it.
    gaps = s._gap_buffer["binance:BTCUSDT"]
    assert len(gaps) == 1
    assert gaps[0]["expected_update_id"] == 101
    assert gaps[0]["received_update_id"] == 105
    assert gaps[0]["gap_size"] == 4

    # Checksum mismatch detected and routed correctly.
    failures = s._checksum_buffer["kraken:BTC/USD"]
    assert len(failures) == 1
    assert failures[0]["received"] == 1

    # Initial snapshot fetch + exactly one resync-after-gap fetch.
    assert fake_session.get_calls == 2


# ── Binance Futures dual-connection (main + aux) ────────────────────────────

def test_binance_futures_aux_connection_delivers_funding(tmp_path):
    asyncio.run(_run_binance_futures_aux_connection(tmp_path))


async def _run_binance_futures_aux_connection(tmp_path):
    aux_msgs = [json.dumps({
        "stream": "btcusdt@markPrice@1s",
        "data": {"E": 1700000000000, "p": "65010.5", "r": "0.0001", "T": 1700001800000},
    })]
    main_msgs = [json.dumps({
        "stream": "btcusdt@trade",
        "data": {"T": 1, "t": 1, "p": "1", "q": "1", "m": False},
    })]

    def fake_connect(url, **kwargs):
        if "/market/" in url:
            return _FakeConnectCtx(_FakeWS(list(aux_msgs)))
        return _FakeConnectCtx(_FakeWS(list(main_msgs)))

    s = LOBStreamer(
        exchange="binance_futures", assets=["BTCUSDT"],
        output="local", output_dir=str(tmp_path),
    )
    feed = s.feeds[0]

    # Snapshot fetch will fail (no real network) -- that's fine, this test
    # is only about the main/aux WebSocket split, not the REST snapshot.
    with patch("crypto_lob_stream.streamer.websockets.connect", side_effect=fake_connect):
        tasks = [
            asyncio.create_task(s._stream_feed(feed)),
            asyncio.create_task(s._stream_aux_feed(feed)),
        ]
        await asyncio.sleep(0.5)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    assert s._trade_buffer["binance_futures:BTCUSDT"][0]["price"] == 1.0
    funding = s._funding_buffer["binance_futures:BTCUSDT"]
    assert len(funding) == 1
    assert funding[0]["funding_rate"] == 0.0001