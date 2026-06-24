import pytest
from crypto_lob_stream.exchanges import (
    BinanceExchange,
    CoinbaseExchange,
    get_exchange,
    available_exchanges,
)


# ── Registry ──────────────────────────────────────────────────────────────

def test_available_exchanges():
    ex = available_exchanges()
    assert "binance" in ex
    assert "coinbase" in ex


def test_get_exchange_binance():
    assert get_exchange("binance").name == "binance"
    assert get_exchange("BINANCE").name == "binance"


def test_get_exchange_coinbase():
    assert get_exchange("coinbase").name == "coinbase"


def test_get_exchange_unknown():
    with pytest.raises(ValueError, match="Unknown exchange"):
        get_exchange("ftx")


# ── Binance symbol normalisation ────────────────────────────────────────────

def test_binance_normalize():
    b = BinanceExchange()
    assert b.normalize_symbol("btcusdt") == "BTCUSDT"
    assert b.normalize_symbol("BTC-USDT") == "BTCUSDT"
    assert b.normalize_symbol("BTC/USDT") == "BTCUSDT"


def test_binance_ws_url():
    b = BinanceExchange()
    url = b.ws_url(["BTCUSDT", "ETHUSDT"])
    assert "btcusdt@trade" in url
    assert "btcusdt@depth@100ms" in url
    assert "ethusdt@trade" in url
    assert url.startswith("wss://stream.binance.com")


def test_binance_no_subscribe_messages():
    assert BinanceExchange().subscribe_messages(["BTCUSDT"]) == []


def test_binance_parse_trade():
    b = BinanceExchange()
    raw = {
        "stream": "btcusdt@trade",
        "data": {"T": 1700000000000, "t": 123, "p": "65000.0", "q": "0.5", "m": False},
    }
    out = b.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "trade"
    assert out[0]["asset"] == "BTCUSDT"
    assert out[0]["price"] == 65000.0
    assert out[0]["buyer_maker"] is False


def test_binance_parse_depth():
    b = BinanceExchange()
    raw = {
        "stream": "btcusdt@depth@100ms",
        "data": {
            "U": 1000, "u": 1005,
            "b": [["65000.0", "0.5"], ["64999.0", "1.0"]],
            "a": [["65001.0", "0.3"]],
        },
    }
    out = b.parse_message(raw)
    assert len(out) == 3
    bids = [r for r in out if r["side"] == "bid"]
    asks = [r for r in out if r["side"] == "ask"]
    assert len(bids) == 2
    assert len(asks) == 1
    assert all(r["first_update_id"] == 1000 for r in out)
    assert all(r["last_update_id"] == 1005 for r in out)


def test_binance_parse_malformed():
    b = BinanceExchange()
    assert b.parse_message({"stream": "btcusdt@trade", "data": {"T": 1}}) == []
    assert b.parse_message({}) == []


def test_binance_snapshot_url():
    b = BinanceExchange()
    assert "api.binance.com" in b.snapshot_url("BTCUSDT")
    assert "BTCUSDT" in b.snapshot_url("btcusdt")


def test_binance_parse_snapshot():
    b = BinanceExchange()
    raw = {
        "lastUpdateId": 9999,
        "bids": [["65000.0", "0.5"]],
        "asks": [["65001.0", "0.3"]],
    }
    recs = b.parse_snapshot("BTCUSDT", raw)
    assert len(recs) == 2
    assert all(r["last_update_id"] == 9999 for r in recs)


# ── Coinbase ────────────────────────────────────────────────────────────────

def test_coinbase_normalize():
    c = CoinbaseExchange()
    assert c.normalize_symbol("BTC-USD") == "BTC-USD"
    assert c.normalize_symbol("btc/usd") == "BTC-USD"
    assert c.normalize_symbol("BTCUSD") == "BTC-USD"
    assert c.normalize_symbol("ETHUSDT") == "ETH-USDT"


def test_coinbase_ws_url():
    c = CoinbaseExchange()
    assert c.ws_url(["BTC-USD"]) == "wss://ws-feed.exchange.coinbase.com"


def test_coinbase_subscribe_messages():
    c = CoinbaseExchange()
    msgs = c.subscribe_messages(["BTC-USD", "ETH-USD"])
    assert len(msgs) == 1
    assert msgs[0]["type"] == "subscribe"
    assert "BTC-USD" in msgs[0]["product_ids"]
    assert "level2_batch" in msgs[0]["channels"]
    assert "matches" in msgs[0]["channels"]


def test_coinbase_parse_trade():
    c = CoinbaseExchange()
    raw = {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": 555,
        "price": "65000.0",
        "size": "0.25",
        "side": "buy",
        "time": "2026-06-12T11:05:57.123456Z",
    }
    out = c.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "trade"
    assert out[0]["asset"] == "BTC-USD"
    assert out[0]["price"] == 65000.0
    assert out[0]["buyer_maker"] is True


def test_coinbase_parse_l2update():
    c = CoinbaseExchange()
    raw = {
        "type": "l2update",
        "product_id": "BTC-USD",
        "time": "2026-06-12T11:05:57.123456Z",
        "changes": [["buy", "65000.0", "0.5"], ["sell", "65001.0", "0.0"]],
    }
    out = c.parse_message(raw)
    assert len(out) == 2
    bid = [r for r in out if r["side"] == "bid"][0]
    ask = [r for r in out if r["side"] == "ask"][0]
    assert bid["price"] == 65000.0
    assert ask["quantity"] == 0.0
    assert bid["first_update_id"] == bid["last_update_id"]


def test_coinbase_snapshot_url_empty():
    # Coinbase delivers snapshot over socket
    assert CoinbaseExchange().snapshot_url("BTC-USD") == ""


def test_coinbase_parse_snapshot():
    c = CoinbaseExchange()
    raw = {
        "type": "snapshot",
        "product_id": "BTC-USD",
        "bids": [["65000.0", "0.5"]],
        "asks": [["65001.0", "0.3"]],
    }
    recs = c.parse_snapshot("BTC-USD", raw)
    assert len(recs) == 2
    assert recs[0]["asset"] == "BTC-USD"


def test_coinbase_ignores_control_messages():
    c = CoinbaseExchange()
    assert c.parse_message({"type": "subscriptions"}) == []
    assert c.parse_message({"type": "heartbeat"}) == []


# ── OKX ─────────────────────────────────────────────────────────────────────

from crypto_lob_stream.exchanges import OKXExchange


def test_okx_in_registry():
    assert "okx" in available_exchanges()
    assert get_exchange("okx").name == "okx"


def test_okx_normalize():
    o = OKXExchange()
    assert o.normalize_symbol("BTC-USDT") == "BTC-USDT"
    assert o.normalize_symbol("btc/usdt") == "BTC-USDT"
    assert o.normalize_symbol("BTCUSDT") == "BTC-USDT"


def test_okx_ws_url():
    assert OKXExchange().ws_url(["BTC-USDT"]).startswith("wss://ws.okx.com")


def test_okx_subscribe_messages():
    o = OKXExchange()
    msgs = o.subscribe_messages(["BTC-USDT"])
    assert len(msgs) == 1
    assert msgs[0]["op"] == "subscribe"
    channels = [a["channel"] for a in msgs[0]["args"]]
    assert "books" in channels
    assert "trades" in channels


def test_okx_parse_trade():
    o = OKXExchange()
    raw = {
        "arg": {"channel": "trades", "instId": "BTC-USDT"},
        "data": [{
            "instId": "BTC-USDT", "tradeId": "130639474",
            "px": "65000.0", "sz": "0.05", "side": "buy",
            "ts": "1700000000000",
        }],
    }
    out = o.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "trade"
    assert out[0]["asset"] == "BTC-USDT"
    assert out[0]["price"] == 65000.0
    assert out[0]["buyer_maker"] is False  # taker was buyer


def test_okx_parse_depth_update():
    o = OKXExchange()
    raw = {
        "arg": {"channel": "books", "instId": "BTC-USDT"},
        "action": "update",
        "data": [{
            "ts": "1700000000000", "seqId": 123, "prevSeqId": 122,
            "bids": [["65000.0", "0.5", "0", "1"]],
            "asks": [["65001.0", "0.0", "0", "0"]],
        }],
    }
    out = o.parse_message(raw)
    assert len(out) == 2
    bid = [r for r in out if r["side"] == "bid"][0]
    ask = [r for r in out if r["side"] == "ask"][0]
    assert bid["price"] == 65000.0
    assert bid["last_update_id"] == 123
    assert bid["first_update_id"] == 122
    assert ask["quantity"] == 0.0


def test_okx_snapshot_detection():
    o = OKXExchange()
    snap = {
        "arg": {"channel": "books", "instId": "BTC-USDT"},
        "action": "snapshot",
        "data": [{"ts": "1700000000000", "seqId": 100,
                  "bids": [["65000.0", "0.5", "0", "1"]],
                  "asks": [["65001.0", "0.3", "0", "1"]]}],
    }
    assert o.is_socket_snapshot(snap) is True
    assert o.socket_snapshot_asset(snap) == "BTC-USDT"
    # An update is not a snapshot
    upd = {"arg": {"channel": "books", "instId": "BTC-USDT"}, "action": "update", "data": []}
    assert o.is_socket_snapshot(upd) is False


def test_okx_parse_snapshot():
    o = OKXExchange()
    raw = {
        "arg": {"channel": "books", "instId": "BTC-USDT"},
        "action": "snapshot",
        "data": [{"ts": "1700000000000", "seqId": 100,
                  "bids": [["65000.0", "0.5", "0", "1"]],
                  "asks": [["65001.0", "0.3", "0", "1"]]}],
    }
    recs = o.parse_snapshot("BTC-USDT", raw)
    assert len(recs) == 2
    assert all(r["last_update_id"] == 100 for r in recs)


def test_okx_snapshot_returns_empty_in_parse_message():
    # Snapshot action should not produce depth records via parse_message
    o = OKXExchange()
    raw = {
        "arg": {"channel": "books", "instId": "BTC-USDT"},
        "action": "snapshot",
        "data": [{"ts": "1", "seqId": 1, "bids": [], "asks": []}],
    }
    assert o.parse_message(raw) == []


def test_binance_not_socket_snapshot():
    # Binance uses REST snapshots, so is_socket_snapshot is always False
    assert BinanceExchange().is_socket_snapshot({"anything": True}) is False


# ── Kraken ──────────────────────────────────────────────────────────────────

from crypto_lob_stream.exchanges import KrakenExchange


def test_kraken_in_registry():
    assert "kraken" in available_exchanges()
    assert get_exchange("kraken").name == "kraken"


def test_kraken_normalize():
    k = KrakenExchange()
    assert k.normalize_symbol("BTC/USD") == "BTC/USD"
    assert k.normalize_symbol("btc-usd") == "BTC/USD"
    assert k.normalize_symbol("BTCUSD") == "BTC/USD"


def test_kraken_ws_url():
    assert KrakenExchange().ws_url(["BTC/USD"]) == "wss://ws.kraken.com/v2"


def test_kraken_subscribe_messages():
    k = KrakenExchange()
    msgs = k.subscribe_messages(["BTC/USD"])
    assert len(msgs) == 2
    channels = [m["params"]["channel"] for m in msgs]
    assert "book" in channels
    assert "trade" in channels


def test_kraken_parse_trade():
    k = KrakenExchange()
    raw = {
        "channel": "trade",
        "type": "update",
        "data": [{
            "symbol": "BTC/USD", "side": "buy", "price": 65000.0,
            "qty": 0.1, "trade_id": 99, "timestamp": "2026-06-12T11:05:57.123456Z",
        }],
    }
    out = k.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "trade"
    assert out[0]["asset"] == "BTC/USD"
    assert out[0]["price"] == 65000.0
    assert out[0]["buyer_maker"] is False


def test_kraken_parse_book_update():
    k = KrakenExchange()
    raw = {
        "channel": "book",
        "type": "update",
        "data": [{
            "symbol": "BTC/USD",
            "timestamp": "2026-06-12T11:05:57.123456Z",
            "bids": [{"price": 65000.0, "qty": 0.5}],
            "asks": [{"price": 65001.0, "qty": 0.0}],
        }],
    }
    out = k.parse_message(raw)
    assert len(out) == 2
    bid = [r for r in out if r["side"] == "bid"][0]
    ask = [r for r in out if r["side"] == "ask"][0]
    assert bid["price"] == 65000.0
    assert ask["quantity"] == 0.0


def test_kraken_snapshot_detection():
    k = KrakenExchange()
    snap = {
        "channel": "book", "type": "snapshot",
        "data": [{"symbol": "BTC/USD", "timestamp": "2026-06-12T11:05:57.123456Z",
                  "bids": [{"price": 65000.0, "qty": 0.5}],
                  "asks": [{"price": 65001.0, "qty": 0.3}]}],
    }
    assert k.is_socket_snapshot(snap) is True
    assert k.socket_snapshot_asset(snap) == "BTC/USD"


def test_kraken_parse_snapshot():
    k = KrakenExchange()
    raw = {
        "channel": "book", "type": "snapshot",
        "data": [{"symbol": "BTC/USD", "timestamp": "2026-06-12T11:05:57.123456Z",
                  "bids": [{"price": 65000.0, "qty": 0.5}],
                  "asks": [{"price": 65001.0, "qty": 0.3}]}],
    }
    recs = k.parse_snapshot("BTC/USD", raw)
    assert len(recs) == 2
    assert recs[0]["asset"] == "BTC/USD"


# ── Bybit ───────────────────────────────────────────────────────────────────

from crypto_lob_stream.exchanges import BybitExchange


def test_bybit_in_registry():
    assert "bybit" in available_exchanges()
    assert get_exchange("bybit").name == "bybit"


def test_bybit_normalize():
    b = BybitExchange()
    assert b.normalize_symbol("BTCUSDT") == "BTCUSDT"
    assert b.normalize_symbol("btc-usdt") == "BTCUSDT"
    assert b.normalize_symbol("BTC/USDT") == "BTCUSDT"


def test_bybit_ws_url():
    assert BybitExchange().ws_url(["BTCUSDT"]).startswith("wss://stream.bybit.com")


def test_bybit_subscribe_messages():
    b = BybitExchange()
    msgs = b.subscribe_messages(["BTCUSDT"])
    assert len(msgs) == 1
    assert msgs[0]["op"] == "subscribe"
    assert "orderbook.50.BTCUSDT" in msgs[0]["args"]
    assert "publicTrade.BTCUSDT" in msgs[0]["args"]


def test_bybit_parse_trade():
    b = BybitExchange()
    raw = {
        "topic": "publicTrade.BTCUSDT",
        "type": "snapshot",
        "ts": 1672304486868,
        "data": [{
            "T": 1672304486865, "s": "BTCUSDT", "S": "Buy",
            "v": "0.001", "p": "16578.50", "i": "20f43950-d8dd-5b31",
        }],
    }
    out = b.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "trade"
    assert out[0]["asset"] == "BTCUSDT"
    assert out[0]["price"] == 16578.50
    assert out[0]["buyer_maker"] is False
    # UUID trade id hashed to int
    assert isinstance(out[0]["trade_id"], int)


def test_bybit_parse_depth_delta():
    b = BybitExchange()
    raw = {
        "topic": "orderbook.50.BTCUSDT",
        "type": "delta",
        "ts": 1672304486868,
        "data": {
            "s": "BTCUSDT", "u": 555, "seq": 554,
            "b": [["16578.0", "0.5"]],
            "a": [["16579.0", "0.0"]],
        },
    }
    out = b.parse_message(raw)
    assert len(out) == 2
    bid = [r for r in out if r["side"] == "bid"][0]
    ask = [r for r in out if r["side"] == "ask"][0]
    assert bid["price"] == 16578.0
    assert bid["last_update_id"] == 555
    # first_update_id == last_update_id == "u" (the real per-symbol
    # counter), NOT "seq" -- seq is a different, much larger-scale counter
    # ("cross sequence") that Bybit's docs describe as being for comparing
    # freshness across different data sources, not a chainable per-message
    # id. Using it here previously made every live message look like a
    # gap of billions of missed updates.
    assert bid["first_update_id"] == 555
    assert ask["last_update_id"] == 555
    assert ask["quantity"] == 0.0  # qty 0 represents a removed level


def test_bybit_depth_uses_u_not_seq_for_continuity():
    # Regression test for a real bug: seq and u are on wildly different
    # numeric scales (seq is roughly 1000x+ larger), so using seq for
    # first_update_id made the streamer's gap detector fire on every
    # single live message. first_update_id must equal last_update_id
    # (both "u") so consecutive messages chain correctly.
    b = BybitExchange()
    raw = {
        "topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": 1,
        "data": {"s": "BTCUSDT", "u": 18521289, "seq": 7961638999,
                  "b": [["1.0", "1.0"]], "a": []},
    }
    out = b.parse_message(raw)
    assert out[0]["first_update_id"] == out[0]["last_update_id"] == 18521289
    assert out[0]["first_update_id"] != 7961638999


def test_bybit_snapshot_detection():
    b = BybitExchange()
    snap = {
        "topic": "orderbook.50.BTCUSDT", "type": "snapshot",
        "ts": 1672304486868,
        "data": {"s": "BTCUSDT", "u": 100, "seq": 99,
                 "b": [["16578.0", "0.5"]], "a": [["16579.0", "0.3"]]},
    }
    assert b.is_socket_snapshot(snap) is True
    assert b.socket_snapshot_asset(snap) == "BTCUSDT"
    delta = {"topic": "orderbook.50.BTCUSDT", "type": "delta", "data": {}}
    assert b.is_socket_snapshot(delta) is False


def test_bybit_parse_snapshot():
    b = BybitExchange()
    raw = {
        "topic": "orderbook.50.BTCUSDT", "type": "snapshot",
        "ts": 1672304486868,
        "data": {"s": "BTCUSDT", "u": 100, "seq": 99,
                 "b": [["16578.0", "0.5"]], "a": [["16579.0", "0.3"]]},
    }
    recs = b.parse_snapshot("BTCUSDT", raw)
    assert len(recs) == 2
    assert all(r["last_update_id"] == 100 for r in recs)


def test_bybit_trade_id_int_passthrough():
    b = BybitExchange()
    assert b._trade_id("12345") == 12345
    assert isinstance(b._trade_id("uuid-string-here"), int)

# ── Binance Futures (perpetuals) ─────────────────────────────────────────────

from crypto_lob_stream.exchanges import BinanceFuturesExchange


def test_binance_futures_in_registry():
    assert "binance_futures" in available_exchanges()
    assert get_exchange("binance_futures").name == "binance_futures"


def test_binance_futures_ws_url_uses_futures_host():
    bf = BinanceFuturesExchange()
    url = bf.ws_url(["BTCUSDT"])
    assert url.startswith("wss://fstream.binance.com")
    assert "btcusdt@trade" in url
    assert "btcusdt@depth@100ms" in url
    # markPrice moved to the auxiliary /market-routed connection (see
    # test_binance_futures_aux_ws_url below) -- a connection without a
    # routed path only receives /public streams, and markPrice is /market.
    assert "markPrice" not in url


def test_binance_futures_aux_ws_url_routes_through_market_path():
    bf = BinanceFuturesExchange()
    aux_url = bf.aux_ws_url(["BTCUSDT"])
    assert aux_url is not None
    assert aux_url.startswith("wss://fstream.binance.com/market/")
    assert "btcusdt@markPrice@1s" in aux_url


def test_other_exchanges_have_no_aux_connection():
    for cls in (BinanceExchange, CoinbaseExchange, OKXExchange, KrakenExchange, BybitExchange):
        assert cls().aux_ws_url(["BTCUSDT"]) is None


def test_parse_aux_message_defaults_to_parse_message():
    bf = BinanceFuturesExchange()
    raw = {
        "stream": "btcusdt@markPrice@1s",
        "data": {"E": 1700000000000, "p": "65010.5", "r": "0.0001", "T": 1700001800000},
    }
    assert bf.parse_aux_message(raw) == bf.parse_message(raw)


def test_binance_futures_snapshot_url_uses_fapi_host():
    bf = BinanceFuturesExchange()
    assert bf.snapshot_url("BTCUSDT") == (
        "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
    )


def test_binance_futures_parse_trade_inherits_spot_logic():
    bf = BinanceFuturesExchange()
    raw = {
        "stream": "btcusdt@trade",
        "data": {"T": 1700000000000, "t": 1, "p": "65000.0", "q": "0.5", "m": False},
    }
    out = bf.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "trade"
    assert out[0]["price"] == 65000.0


def test_binance_futures_parse_depth_uses_pu_for_continuity():
    bf = BinanceFuturesExchange()
    raw = {
        "stream": "btcusdt@depth@100ms",
        "data": {
            "U": 1000, "u": 1010, "pu": 999,
            "b": [["65000.0", "0.5"]],
            "a": [["65001.0", "0.3"]],
        },
    }
    out = bf.parse_message(raw)
    assert len(out) == 2
    # first_update_id should come from "pu", not "U", per Binance futures docs
    assert all(r["first_update_id"] == 999 for r in out)
    assert all(r["last_update_id"] == 1010 for r in out)


def test_binance_futures_parse_depth_falls_back_to_U_without_pu():
    bf = BinanceFuturesExchange()
    raw = {
        "stream": "btcusdt@depth@100ms",
        "data": {"U": 1000, "u": 1010, "b": [["65000.0", "0.5"]], "a": []},
    }
    out = bf.parse_message(raw)
    assert out[0]["first_update_id"] == 1000


def test_binance_futures_parse_funding():
    bf = BinanceFuturesExchange()
    raw = {
        "stream": "btcusdt@markPrice@1s",
        "data": {"E": 1700000000000, "p": "65010.5", "r": "0.0001", "T": 1700001800000},
    }
    out = bf.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "funding"
    assert out[0]["mark_price"] == 65010.5
    assert out[0]["funding_rate"] == 0.0001
    assert out[0]["next_funding_ms"] == 1700001800000


def test_binance_futures_malformed_funding_ignored():
    bf = BinanceFuturesExchange()
    assert bf.parse_message({"stream": "btcusdt@markPrice@1s", "data": {"E": 1}}) == []


# ── Kraken checksum verification ─────────────────────────────────────────────

def test_kraken_has_sequence_ids_false():
    assert KrakenExchange().has_sequence_ids is False


def test_kraken_supports_checksum_true():
    assert KrakenExchange().supports_checksum is True


def test_kraken_fmt_num_strips_decimal_and_leading_zeros():
    k = KrakenExchange()
    assert k._fmt_num("23000.10000") == "2300010000"
    assert k._fmt_num("0.00500") == "500"
    assert k._fmt_num("5") == "5"
    assert k._fmt_num("0.0") == "0"
    assert k._fmt_num("100.0") == "1000"


def test_kraken_checksum_no_checksum_field_returns_none_match():
    k = KrakenExchange()
    book = {"symbol": "BTC/USD", "bids": [], "asks": []}
    asset, match, expected, received = k.update_book_and_checksum(book, is_snapshot=True)
    assert asset == "BTC/USD"
    assert match is None
    assert expected is None
    assert received is None


def test_kraken_checksum_mismatch_detected():
    k = KrakenExchange()
    book = {
        "symbol": "BTC/USD",
        "bids": [{"price": "65000.0", "qty": "0.5"}],
        "asks": [{"price": "65001.0", "qty": "0.3"}],
        "checksum": 1,  # deliberately wrong
    }
    asset, match, expected, received = k.update_book_and_checksum(book, is_snapshot=True)
    assert asset == "BTC/USD"
    assert match is False
    assert received == 1
    assert expected != 1


def test_kraken_checksum_is_deterministic_for_same_book():
    k1 = KrakenExchange()
    k2 = KrakenExchange()
    book = {
        "symbol": "BTC/USD",
        "bids": [{"price": "65000.0", "qty": "0.5"}, {"price": "64999.0", "qty": "1.0"}],
        "asks": [{"price": "65001.0", "qty": "0.3"}],
        "checksum": 0,
    }
    _, _, expected1, _ = k1.update_book_and_checksum(book, is_snapshot=True)
    _, _, expected2, _ = k2.update_book_and_checksum(book, is_snapshot=True)
    assert expected1 == expected2


def test_kraken_checksum_book_mirror_applies_removals():
    k = KrakenExchange()
    snapshot = {
        "symbol": "BTC/USD",
        "bids": [{"price": "65000.0", "qty": "0.5"}],
        "asks": [{"price": "65001.0", "qty": "0.3"}],
    }
    k.update_book_and_checksum(snapshot, is_snapshot=True)
    assert 65000.0 in k._books["BTC/USD"]["bid"]

    update = {
        "symbol": "BTC/USD",
        "bids": [{"price": "65000.0", "qty": "0.0"}],  # qty 0 removes the level
        "asks": [],
    }
    k.update_book_and_checksum(update, is_snapshot=False)
    assert 65000.0 not in k._books["BTC/USD"]["bid"]


def test_kraken_checksum_snapshot_resets_book():
    k = KrakenExchange()
    snapshot1 = {
        "symbol": "BTC/USD",
        "bids": [{"price": "65000.0", "qty": "0.5"}],
        "asks": [],
    }
    k.update_book_and_checksum(snapshot1, is_snapshot=True)
    assert 65000.0 in k._books["BTC/USD"]["bid"]

    # A fresh snapshot (e.g. after reconnect) should clear stale levels,
    # not merge with the old book.
    snapshot2 = {
        "symbol": "BTC/USD",
        "bids": [{"price": "70000.0", "qty": "1.0"}],
        "asks": [],
    }
    k.update_book_and_checksum(snapshot2, is_snapshot=True)
    assert 65000.0 not in k._books["BTC/USD"]["bid"]
    assert 70000.0 in k._books["BTC/USD"]["bid"]


def test_coinbase_has_sequence_ids_false():
    assert CoinbaseExchange().has_sequence_ids is False


def test_binance_has_sequence_ids_true():
    assert BinanceExchange().has_sequence_ids is True


def test_bybit_okx_have_sequence_ids_true():
    assert BybitExchange().has_sequence_ids is True
    assert OKXExchange().has_sequence_ids is True


# ── OKX swap (perpetual futures) ────────────────────────────────────────────

from crypto_lob_stream.exchanges import OKXSwapExchange, BybitLinearExchange


def test_okx_swap_in_registry():
    assert "okx_swap" in available_exchanges()
    assert get_exchange("okx_swap").name == "okx_swap"


def test_okx_swap_normalize_appends_swap_suffix():
    okx = OKXSwapExchange()
    assert okx.normalize_symbol("BTC-USDT") == "BTC-USDT-SWAP"
    assert okx.normalize_symbol("BTCUSDT") == "BTC-USDT-SWAP"
    # idempotent if already suffixed
    assert okx.normalize_symbol("BTC-USDT-SWAP") == "BTC-USDT-SWAP"


def test_okx_swap_subscribe_includes_funding_rate_channel():
    okx = OKXSwapExchange()
    msgs = okx.subscribe_messages(["BTC-USDT"])
    channels = {a["channel"] for a in msgs[0]["args"]}
    assert channels == {"books", "trades", "funding-rate", "open-interest", "liquidation-orders"}
    per_asset_args = [a for a in msgs[0]["args"] if a["channel"] != "liquidation-orders"]
    assert all(a["instId"] == "BTC-USDT-SWAP" for a in per_asset_args)


def test_okx_swap_parses_funding_rate_with_null_mark_price():
    okx = OKXSwapExchange()
    raw = {
        "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
        "data": [{"instId": "BTC-USDT-SWAP", "fundingRate": "0.0001",
                   "nextFundingTime": "1700028800000"}],
    }
    out = okx.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "funding"
    assert out[0]["funding_rate"] == 0.0001
    assert out[0]["next_funding_ms"] == 1700028800000
    assert out[0]["mark_price"] is None  # documented gap, see class docstring


def test_okx_swap_reuses_spot_books_and_trades_parsing():
    okx = OKXSwapExchange()
    raw = {
        "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
        "action": "update",
        "data": [{"bids": [["65000.0", "0.5", "0", "1"]], "asks": [],
                   "ts": "1700000000000", "seqId": 100, "prevSeqId": 99}],
    }
    out = okx.parse_message(raw)
    assert out[0]["type"] == "depth"
    assert out[0]["asset"] == "BTC-USDT-SWAP"


def test_okx_swap_malformed_funding_ignored():
    okx = OKXSwapExchange()
    raw = {"arg": {"channel": "funding-rate"}, "data": [{"instId": "X"}]}
    assert okx.parse_message(raw) == []


# ── Bybit linear (USDT perpetuals) ──────────────────────────────────────────

def test_bybit_linear_in_registry():
    assert "bybit_linear" in available_exchanges()
    assert get_exchange("bybit_linear").name == "bybit_linear"


def test_bybit_linear_ws_url_uses_linear_path():
    byb = BybitLinearExchange()
    assert byb.ws_url(["BTCUSDT"]) == "wss://stream.bybit.com/v5/public/linear"


def test_bybit_linear_subscribe_includes_tickers_topic():
    byb = BybitLinearExchange()
    msgs = byb.subscribe_messages(["BTCUSDT"])
    assert "tickers.BTCUSDT" in msgs[0]["args"]
    assert "orderbook.50.BTCUSDT" in msgs[0]["args"]
    assert "publicTrade.BTCUSDT" in msgs[0]["args"]


def test_bybit_linear_parses_funding_with_mark_price():
    byb = BybitLinearExchange()
    raw = {
        "topic": "tickers.BTCUSDT", "type": "snapshot", "ts": 1700000000000,
        "data": {"symbol": "BTCUSDT", "markPrice": "65010.5",
                  "fundingRate": "0.0001", "nextFundingTime": "1700028800000"},
    }
    out = byb.parse_message(raw)
    assert len(out) == 1
    assert out[0]["mark_price"] == 65010.5
    assert out[0]["funding_rate"] == 0.0001
    assert out[0]["next_funding_ms"] == 1700028800000


def test_bybit_linear_incomplete_delta_produces_no_funding_record():
    byb = BybitLinearExchange()
    raw = {
        "topic": "tickers.BTCUSDT", "type": "delta", "ts": 1700000001000,
        "data": {"symbol": "BTCUSDT", "lastPrice": "65011.0"},
    }
    assert byb.parse_message(raw) == []


def test_bybit_linear_reuses_spot_orderbook_and_trade_parsing():
    byb = BybitLinearExchange()
    raw = {"topic": "publicTrade.BTCUSDT",
           "data": [{"T": 1, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "1", "i": "1"}]}
    out = byb.parse_message(raw)
    assert out[0]["type"] == "trade"
    assert out[0]["asset"] == "BTCUSDT"


# ── Liquidations & open interest (stress-research coverage) ─────────────────

def test_binance_futures_aux_includes_forceorder():
    bf = BinanceFuturesExchange()
    aux_url = bf.aux_ws_url(["BTCUSDT"])
    assert "btcusdt@markPrice@1s" in aux_url
    assert "btcusdt@forceOrder" in aux_url
    # Confirmed via Binance's own routed-stream mapping table: forceOrder
    # is /market, same as markPrice -- both belong on this one connection.
    assert aux_url.startswith("wss://fstream.binance.com/market/")


def test_binance_futures_parses_forceorder_liquidation():
    bf = BinanceFuturesExchange()
    raw = {
        "stream": "btcusdt@forceOrder",
        "data": {
            "e": "forceOrder", "E": 1568014460893,
            "o": {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC",
                   "q": "0.014", "p": "9910", "ap": "9910", "X": "FILLED",
                   "l": "0.014", "z": "0.014", "T": 1568014460893},
        },
    }
    out = bf.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "liquidation"
    assert out[0]["asset"] == "BTCUSDT"
    assert out[0]["side"] == "sell"
    assert out[0]["price"] == 9910.0
    assert out[0]["quantity"] == 0.014


def test_binance_futures_malformed_forceorder_ignored():
    bf = BinanceFuturesExchange()
    raw = {"stream": "btcusdt@forceOrder", "data": {"o": {"s": "BTCUSDT"}}}
    assert bf.parse_message(raw) == []


def test_binance_futures_open_interest_url():
    bf = BinanceFuturesExchange()
    assert bf.open_interest_url("BTCUSDT") == (
        "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
    )


def test_binance_futures_parses_open_interest_rest_response():
    bf = BinanceFuturesExchange()
    raw = {"symbol": "BTCUSDT", "openInterest": "10659.509", "time": 1625184323456}
    out = bf.parse_open_interest("BTCUSDT", raw)
    assert len(out) == 1
    assert out[0]["type"] == "open_interest"
    assert out[0]["open_interest"] == 10659.509
    assert out[0]["open_interest_value"] is None  # Binance doesn't provide this
    assert out[0]["timestamp_ms"] == 1625184323456


def test_binance_futures_malformed_open_interest_ignored():
    bf = BinanceFuturesExchange()
    assert bf.parse_open_interest("BTCUSDT", {"symbol": "BTCUSDT"}) == []


def test_okx_swap_subscribes_liquidations_once_per_insttype_not_per_asset():
    okx = OKXSwapExchange()
    msgs = okx.subscribe_messages(["BTC-USDT", "ETH-USDT"])
    liq_args = [a for a in msgs[0]["args"] if a["channel"] == "liquidation-orders"]
    assert len(liq_args) == 1  # one subscription, not one per asset
    assert liq_args[0] == {"channel": "liquidation-orders", "instType": "SWAP"}


def test_okx_swap_parses_open_interest():
    okx = OKXSwapExchange()
    raw = {
        "arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
        "data": [{"instId": "BTC-USDT-SWAP", "instType": "SWAP",
                   "oi": "14546007", "oiCcy": "14546007000",
                   "oiUsd": "30921319.84032", "ts": "1728709646117"}],
    }
    out = okx.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "open_interest"
    assert out[0]["asset"] == "BTC-USDT-SWAP"
    assert out[0]["open_interest"] == 14546007.0
    assert out[0]["open_interest_value"] == 30921319.84032


def test_okx_swap_liquidation_filters_to_tracked_assets_only():
    okx = OKXSwapExchange()
    okx.subscribe_messages(["BTC-USDT"])  # populates _tracked_assets
    raw = {
        "arg": {"channel": "liquidation-orders", "instType": "SWAP"},
        "data": [
            {  # a different symbol we didn't subscribe to -- must be filtered out
                "instId": "DYDX-USDT-SWAP", "instFamily": "DYDX-USDT", "instType": "SWAP",
                "uly": "DYDX-USDT",
                "details": [{"bkLoss": "0", "bkPx": "1.057", "ccy": "",
                              "posSide": "long", "side": "sell", "sz": "768",
                              "ts": "1723892524781"}],
            },
            {  # the one we actually track
                "instId": "BTC-USDT-SWAP", "instFamily": "BTC-USDT", "instType": "SWAP",
                "uly": "BTC-USDT",
                "details": [{"bkLoss": "0", "bkPx": "65000.0", "ccy": "",
                              "posSide": "long", "side": "sell", "sz": "0.5",
                              "ts": "1723892524999"}],
            },
        ],
    }
    out = okx.parse_message(raw)
    assert len(out) == 1
    assert out[0]["asset"] == "BTC-USDT-SWAP"
    assert out[0]["price"] == 65000.0
    assert out[0]["quantity"] == 0.5
    assert out[0]["side"] == "sell"


def test_okx_swap_liquidation_without_subscribe_call_filters_everything():
    # If subscribe_messages was never called, _tracked_assets is empty --
    # every liquidation gets filtered out rather than crashing.
    okx = OKXSwapExchange()
    raw = {
        "arg": {"channel": "liquidation-orders", "instType": "SWAP"},
        "data": [{"instId": "BTC-USDT-SWAP",
                   "details": [{"bkPx": "1", "side": "sell", "sz": "1", "ts": "1"}]}],
    }
    assert okx.parse_message(raw) == []


def test_bybit_linear_subscribes_allliquidation_not_deprecated_topic():
    byb = BybitLinearExchange()
    msgs = byb.subscribe_messages(["BTCUSDT"])
    assert "allLiquidation.BTCUSDT" in msgs[0]["args"]
    assert "liquidation.BTCUSDT" not in msgs[0]["args"]  # the deprecated one


def test_bybit_linear_parses_allliquidation():
    byb = BybitLinearExchange()
    raw = {
        "topic": "allLiquidation.ROSEUSDT", "type": "snapshot", "ts": 1739502303204,
        "data": [{"T": 1739502302929, "s": "ROSEUSDT", "S": "Sell",
                   "v": "20000", "p": "0.04499"}],
    }
    out = byb.parse_message(raw)
    assert len(out) == 1
    assert out[0]["type"] == "liquidation"
    assert out[0]["asset"] == "ROSEUSDT"
    assert out[0]["side"] == "sell"
    assert out[0]["price"] == 0.04499
    assert out[0]["quantity"] == 20000.0


def test_bybit_linear_allliquidation_batches_multiple_events():
    byb = BybitLinearExchange()
    raw = {
        "topic": "allLiquidation.BTCUSDT", "type": "snapshot", "ts": 1,
        "data": [
            {"T": 1, "s": "BTCUSDT", "S": "Sell", "v": "1", "p": "65000"},
            {"T": 2, "s": "BTCUSDT", "S": "Buy", "v": "2", "p": "65001"},
        ],
    }
    out = byb.parse_message(raw)
    assert len(out) == 2


def test_bybit_linear_tickers_emits_funding_and_open_interest_together():
    byb = BybitLinearExchange()
    raw = {
        "topic": "tickers.BTCUSDT", "type": "snapshot", "ts": 1760325052630,
        "data": {
            "symbol": "BTCUSDT", "markPrice": "66666.60",
            "openInterest": "492373.72", "openInterestValue": "32824881841.75",
            "fundingRate": "-0.005", "nextFundingTime": "1760342400000",
        },
    }
    out = byb.parse_message(raw)
    types = {r["type"] for r in out}
    assert types == {"funding", "open_interest"}
    funding = next(r for r in out if r["type"] == "funding")
    oi = next(r for r in out if r["type"] == "open_interest")
    assert funding["mark_price"] == 66666.60
    assert oi["open_interest"] == 492373.72
    assert oi["open_interest_value"] == 32824881841.75


def test_bybit_linear_tickers_emits_only_funding_when_oi_fields_absent():
    byb = BybitLinearExchange()
    raw = {
        "topic": "tickers.BTCUSDT", "type": "delta", "ts": 1,
        "data": {"symbol": "BTCUSDT", "markPrice": "66666.60",
                  "fundingRate": "-0.005", "nextFundingTime": "1760342400000"},
    }
    out = byb.parse_message(raw)
    assert [r["type"] for r in out] == ["funding"]


def test_bybit_linear_tickers_emits_only_open_interest_when_funding_fields_absent():
    byb = BybitLinearExchange()
    raw = {
        "topic": "tickers.BTCUSDT", "type": "delta", "ts": 1,
        "data": {"symbol": "BTCUSDT", "openInterest": "1.0", "openInterestValue": "2.0"},
    }
    out = byb.parse_message(raw)
    assert [r["type"] for r in out] == ["open_interest"]