"""
Local config management for crypto-lob-stream.

Config is stored at ~/.crypto_lob_stream/config.json so users
only need to run `crypto-lob-stream setup` once.
"""

import json
import os
from pathlib import Path

CONFIG_DIR  = Path.home() / ".crypto_lob_stream"
CONFIG_FILE = CONFIG_DIR / "config.json"


def get_config_dir() -> Path:
    CONFIG_DIR.mkdir(exist_ok=True)
    return CONFIG_DIR


def load_config() -> dict:
    """Load saved config. Returns empty dict if no config exists yet."""
    if not CONFIG_FILE.exists():
        return {}
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_config(data: dict):
    """Merge data into existing config and save."""
    existing = load_config()
    existing.update(data)
    get_config_dir()
    with open(CONFIG_FILE, "w") as f:
        json.dump(existing, f, indent=2)


def get_credentials_path() -> str | None:
    """Return saved GCS credentials path, or None if not configured."""
    return load_config().get("gcs_credentials_path")


def get_saved_bucket() -> str | None:
    """Return saved GCS bucket name, or None if not configured."""
    return load_config().get("gcs_bucket")


def apply_credentials(credentials_path: str | None = None):
    """
    Set GOOGLE_APPLICATION_CREDENTIALS from:
      1. Explicit credentials_path argument
      2. Saved config file
      3. Already set environment variable (no-op)

    Raises FileNotFoundError if a path is found but the file doesn't exist.
    """
    # Already set in environment -- nothing to do
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        return

    path = credentials_path or get_credentials_path()
    if not path:
        return

    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(
            f"GCS credentials file not found: {resolved}\n"
            f"Re-run `crypto-lob-stream setup` to update the path."
        )

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(resolved)


def check_gcs_connection(bucket_name: str) -> tuple[bool, str]:
    """
    Attempt a lightweight GCS connection test.
    Returns (success: bool, message: str).
    """
    try:
        from google.cloud import storage as gcs
    except ImportError:
        return False, (
            "google-cloud-storage is not installed.\n"
            "Run: pip install crypto-lob-stream[gcs]"
        )
    try:
        client = gcs.Client()
        bucket = client.bucket(bucket_name)
        # list_blobs with max_results=1 is a minimal permissions check
        next(iter(client.list_blobs(bucket, max_results=1)), None)
        return True, f"Connected to gs://{bucket_name}"
    except Exception as e:
        return False, str(e)