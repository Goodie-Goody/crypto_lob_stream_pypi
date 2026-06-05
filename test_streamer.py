import pytest
from crypto_lob_stream import LOBStreamer


def test_instantiation_local():
    s = LOBStreamer(assets=["BTCUSDT"], output="local")
    assert s.assets == ["btcusdt"]
    assert s.output == "local"


def test_instantiation_gcs():
    s = LOBStreamer(assets=["BTCUSDT"], output="gcs", bucket="test-bucket")
    assert s.bucket == "test-bucket"


def test_no_assets_raises():
    with pytest.raises(ValueError, match="At least one asset"):
        LOBStreamer(assets=[])


def test_gcs_without_bucket_raises():
    with pytest.raises(ValueError, match="bucket is required"):
        LOBStreamer(assets=["BTCUSDT"], output="gcs")


def test_invalid_output_raises():
    with pytest.raises(ValueError, match="output must be"):
        LOBStreamer(assets=["BTCUSDT"], output="s3")


def test_assets_lowercased():
    s = LOBStreamer(assets=["BTCUSDT", "EthUsdt"])
    assert s.assets == ["btcusdt", "ethusdt"]


def test_handle_trade_valid():
    s = LOBStreamer(assets=["BTCUSDT"], output="local")
    s._handle_trade("btcusdt", {
        "T": 1700000000000,
        "t": 123456,
        "p": "65000.00",
        "q": "0.01",
        "m": False,
    })
    assert len(s._trade_buffer["btcusdt"]) == 1
    record = s._trade_buffer["btcusdt"][0]
    assert record["price"] == 65000.0
    assert record["asset"] == "BTCUSDT"


def test_handle_trade_malformed():
    s = LOBStreamer(assets=["BTCUSDT"], output="local")
    s._handle_trade("btcusdt", {"T": 1700000000000})  # missing fields
    assert len(s._trade_buffer["btcusdt"]) == 0


def test_handle_depth_valid():
    s = LOBStreamer(assets=["BTCUSDT"], output="local")
    s._handle_depth("btcusdt", {
        "U": 1000,
        "u": 1005,
        "b": [["65000.00", "0.5"], ["64999.00", "1.0"]],
        "a": [["65001.00", "0.3"]],
    })
    assert len(s._depth_buffer["btcusdt"]) == 3
    bids = [r for r in s._depth_buffer["btcusdt"] if r["side"] == "bid"]
    asks = [r for r in s._depth_buffer["btcusdt"] if r["side"] == "ask"]
    assert len(bids) == 2
    assert len(asks) == 1
    assert bids[0]["first_update_id"] == 1000
    assert bids[0]["last_update_id"] == 1005


def test_handle_depth_malformed():
    s = LOBStreamer(assets=["BTCUSDT"], output="local")
    s._handle_depth("btcusdt", {"b": [], "a": []})  # missing U and u
    assert len(s._depth_buffer["btcusdt"]) == 0


def test_on_trade_callback():
    received = []
    s = LOBStreamer(assets=["BTCUSDT"], output="local", on_trade=received.append)
    s._handle_trade("btcusdt", {
        "T": 1700000000000, "t": 1, "p": "100.0", "q": "1.0", "m": True
    })
    assert len(received) == 1
    assert received[0]["price"] == 100.0


def test_on_depth_callback():
    received = []
    s = LOBStreamer(assets=["BTCUSDT"], output="local", on_depth=received.append)
    s._handle_depth("btcusdt", {
        "U": 1, "u": 2,
        "b": [["100.0", "1.0"]],
        "a": [],
    })
    assert len(received) == 1
    assert received[0]["side"] == "bid"


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