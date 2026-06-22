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