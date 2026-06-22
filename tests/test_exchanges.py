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
    assert bid["first_update_id"] == 554
    assert ask["quantity"] == 0.0


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