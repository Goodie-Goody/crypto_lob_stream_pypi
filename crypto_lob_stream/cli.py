import argparse
import os
import sys
from pathlib import Path

from .config import (
    apply_credentials,
    check_gcs_connection,
    get_saved_bucket,
    load_config,
    save_config,
)
from .streamer import LOBStreamer


def _check_path_warning():
    """Warn Windows users if the Scripts directory is not on PATH."""
    if sys.platform != "win32":
        return
    scripts_dir = Path(sys.executable).parent / "Scripts"
    path_dirs = [Path(p) for p in os.environ.get("PATH", "").split(os.pathsep)]
    if scripts_dir not in path_dirs:
        print(
            f"\nNote: {scripts_dir} is not on your PATH.\n"
            f"To use 'crypto-lob-stream' directly in future, add it:\n\n"
            f"  [System Settings > Environment Variables > Path > New]\n"
            f"  Add: {scripts_dir}\n\n"
            f"For now, run with: python -m crypto_lob_stream.cli\n",
            file=sys.stderr,
        )


# ── Setup wizard ──────────────────────────────────────────────────────────────

def run_setup():
    """Interactive GCS setup wizard. Saves config to ~/.crypto_lob_stream/config.json."""
    print("\ncrypto-lob-stream -- GCS Setup\n" + "-" * 34)

    # Bucket name
    existing_bucket = get_saved_bucket()
    prompt = f"GCS bucket name [{existing_bucket}]: " if existing_bucket else "GCS bucket name: "
    bucket = input(prompt).strip() or existing_bucket
    if not bucket:
        print("Error: bucket name is required.", file=sys.stderr)
        sys.exit(1)

    # Credentials path
    existing_creds = load_config().get("gcs_credentials_path", "")
    prompt = (
        f"Path to service account JSON key [{existing_creds}]: "
        if existing_creds
        else "Path to service account JSON key: "
    )
    raw_path = input(prompt).strip() or existing_creds
    if not raw_path:
        print("Error: credentials path is required.", file=sys.stderr)
        sys.exit(1)

    resolved = Path(raw_path).expanduser().resolve()
    if not resolved.exists():
        print(f"Error: file not found: {resolved}", file=sys.stderr)
        sys.exit(1)

    # Save before testing so apply_credentials can find it
    save_config({
        "gcs_bucket":             bucket,
        "gcs_credentials_path":   str(resolved),
    })
    print("\nSaved config to ~/.crypto_lob_stream/config.json")

    # Test the connection
    print(f"Testing connection to gs://{bucket} ...", end=" ", flush=True)
    apply_credentials()
    ok, msg = check_gcs_connection(bucket)
    if ok:
        print(f"OK\n\n{msg}")
        print(
            f"\nSetup complete. You can now run:\n\n"
            f"  crypto-lob-stream --assets BTCUSDT,ETHUSDT --output gcs\n"
        )
    else:
        print(f"FAILED\n\nConnection error: {msg}")
        print(
            "\nCredentials saved but connection test failed. "
            "Check that the service account has Storage Object Admin on the bucket.",
            file=sys.stderr,
        )
        sys.exit(1)


# ── Show current config ───────────────────────────────────────────────────────

def run_config():
    """Print current saved configuration."""
    cfg = load_config()
    if not cfg:
        print("No configuration saved yet. Run: crypto-lob-stream setup")
        return
    print("\nSaved configuration (~/.crypto_lob_stream/config.json):")
    for k, v in cfg.items():
        print(f"  {k}: {v}")
    print()


# ── Main entry point ──────────────────────────────────────────────────────────

def main():
    _check_path_warning()
    parser = argparse.ArgumentParser(
        prog="crypto-lob-stream",
        description="Stream Binance Level 2 order book and trade data to local disk or GCS.",
    )

    subparsers = parser.add_subparsers(dest="command")

    # -- setup subcommand
    subparsers.add_parser(
        "setup",
        help="Interactive GCS setup wizard -- run this once before streaming to GCS.",
    )

    # -- config subcommand
    subparsers.add_parser(
        "config",
        help="Show current saved configuration.",
    )

    # -- stream subcommand (default behaviour)
    stream_parser = subparsers.add_parser(
        "stream",
        help="Start streaming (default command).",
    )
    _add_stream_args(stream_parser)

    # Also attach stream args directly to the root parser so users can run
    # `crypto-lob-stream --assets BTCUSDT` without typing `stream` explicitly.
    _add_stream_args(parser)

    args = parser.parse_args()

    if args.command == "setup":
        run_setup()
        return

    if args.command == "config":
        run_config()
        return

    # Default: stream
    _run_stream(args)


def _add_stream_args(p: argparse.ArgumentParser):
    p.add_argument(
        "--assets",
        default=None,
        help="Comma-separated symbols, e.g. BTCUSDT,ETHUSDT (Binance) or BTC-USD (Coinbase)",
    )
    p.add_argument(
        "--exchange",
        default="binance",
        help="Exchange to stream from: binance (default) or coinbase",
    )
    p.add_argument(
        "--output",
        choices=["local", "gcs"],
        default="local",
        help="Output target (default: local)",
    )
    p.add_argument(
        "--output-dir",
        default="./lob_data",
        help="Base directory for local output (default: ./lob_data)",
    )
    p.add_argument(
        "--bucket",
        default=None,
        help=(
            "GCS bucket name. If omitted, uses bucket saved by `setup`. "
            "Required when --output=gcs and no saved config exists."
        ),
    )
    p.add_argument(
        "--credentials",
        default=None,
        metavar="PATH",
        help=(
            "Path to GCS service account JSON key. "
            "If omitted, uses path saved by `setup` or GOOGLE_APPLICATION_CREDENTIALS."
        ),
    )
    p.add_argument(
        "--fallback-dir",
        default="./lob_fallback",
        help="Local fallback directory for failed GCS uploads (default: ./lob_fallback)",
    )
    p.add_argument(
        "--flush-interval",
        type=int,
        default=300,
        help="Seconds between buffer flushes (default: 300)",
    )
    p.add_argument(
        "--log-dir",
        default="./logs",
        help="Directory for rotating log files (default: ./logs)",
    )


def _run_stream(args):
    # Validate assets
    if not getattr(args, "assets", None):
        print(
            "Error: --assets is required.\n"
            "Example: crypto-lob-stream --assets BTCUSDT,ETHUSDT",
            file=sys.stderr,
        )
        sys.exit(1)

    assets = [a.strip() for a in args.assets.split(",") if a.strip()]
    if not assets:
        print("Error: --assets must contain at least one symbol.", file=sys.stderr)
        sys.exit(1)

    # Resolve GCS settings
    bucket = getattr(args, "bucket", None) or get_saved_bucket()
    credentials = getattr(args, "credentials", None)

    if args.output == "gcs":
        if not bucket:
            print(
                "Error: no GCS bucket specified.\n"
                "Either run `crypto-lob-stream setup` first, "
                "or pass --bucket YOUR_BUCKET_NAME.",
                file=sys.stderr,
            )
            sys.exit(1)
        # Apply credentials (explicit flag > saved config > env var)
        try:
            apply_credentials(credentials)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        streamer = LOBStreamer(
            assets=assets,
            exchange=getattr(args, "exchange", "binance"),
            output=args.output,
            output_dir=args.output_dir,
            bucket=bucket,
            fallback_dir=args.fallback_dir,
            flush_interval=args.flush_interval,
            log_dir=args.log_dir,
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    streamer.run()


if __name__ == "__main__":
    main()