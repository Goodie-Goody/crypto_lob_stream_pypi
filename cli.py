import argparse
import sys

from .streamer import LOBStreamer


def main():
    parser = argparse.ArgumentParser(
        prog="crypto-lob-stream",
        description="Stream Binance order book and trade data to local disk or GCS.",
    )
    parser.add_argument(
        "--assets",
        required=True,
        help="Comma-separated list of Binance symbols, e.g. BTCUSDT,ETHUSDT,SOLUSDT",
    )
    parser.add_argument(
        "--output",
        choices=["local", "gcs"],
        default="local",
        help="Output target (default: local)",
    )
    parser.add_argument(
        "--output-dir",
        default="./lob_data",
        help="Base directory for local output (default: ./lob_data)",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="GCS bucket name (required when --output=gcs)",
    )
    parser.add_argument(
        "--fallback-dir",
        default="./lob_fallback",
        help="Local fallback directory for failed GCS uploads (default: ./lob_fallback)",
    )
    parser.add_argument(
        "--flush-interval",
        type=int,
        default=300,
        help="Seconds between buffer flushes (default: 300)",
    )
    parser.add_argument(
        "--log-dir",
        default="./logs",
        help="Directory for rotating log files (default: ./logs)",
    )

    args = parser.parse_args()

    assets = [a.strip() for a in args.assets.split(",") if a.strip()]
    if not assets:
        print("Error: --assets must contain at least one symbol.", file=sys.stderr)
        sys.exit(1)

    if args.output == "gcs" and not args.bucket:
        print("Error: --bucket is required when --output=gcs.", file=sys.stderr)
        sys.exit(1)

    streamer = LOBStreamer(
        assets=assets,
        output=args.output,
        output_dir=args.output_dir,
        bucket=args.bucket,
        fallback_dir=args.fallback_dir,
        flush_interval=args.flush_interval,
        log_dir=args.log_dir,
    )
    streamer.run()


if __name__ == "__main__":
    main()
