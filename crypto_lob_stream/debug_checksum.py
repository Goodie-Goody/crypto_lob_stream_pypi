"""
Standalone Kraken v2 checksum diagnostic. Doesn't touch crypto-lob-stream's
install -- just needs `pip install websockets` in whatever env you run it.

Subscribes at depth=10 specifically (not 25) so there are no off-screen
levels to truncate -- isolates whether the mismatch is a depth-truncation
issue or a pure string-formatting issue. Prints the exact top-10 levels,
the exact concatenated checksum input string, and computed-vs-received,
for the first 6 book messages.

Run: python3 kraken_checksum_debug.py
"""

import asyncio
import json
import zlib

import websockets

SYMBOL = "BTC/USD"
MAX_MESSAGES = 6


def fmt_num(raw_value) -> str:
    s = str(raw_value)
    if "." in s:
        whole, frac = s.split(".", 1)
    else:
        whole, frac = s, ""
    digits = (whole + frac).lstrip("0")
    return digits or "0"


async def main():
    book = {"bid": {}, "ask": {}}
    seen = 0

    async with websockets.connect("wss://ws.kraken.com/v2") as ws:
        await ws.send(json.dumps({
            "method": "subscribe",
            "params": {"channel": "book", "symbol": [SYMBOL], "depth": 10},
        }))

        async for raw_message in ws:
            raw = json.loads(raw_message, parse_float=str)
            if not isinstance(raw, dict) or raw.get("channel") != "book":
                continue
            msg_type = raw.get("type")
            if msg_type not in ("snapshot", "update"):
                continue

            for entry in raw.get("data", []):
                if msg_type == "snapshot":
                    book["bid"] = {}
                    book["ask"] = {}

                for side_key, wire_key in (("bid", "bids"), ("ask", "asks")):
                    for lvl in entry.get(wire_key, []):
                        price_str = str(lvl["price"])
                        qty_str = str(lvl["qty"])
                        price = float(price_str)
                        if float(qty_str) == 0:
                            book[side_key].pop(price, None)
                        else:
                            book[side_key][price] = (price_str, qty_str)

                received = entry.get("checksum")
                if received is None:
                    continue

                asks_top = sorted(book["ask"].items())[:10]
                bids_top = sorted(book["bid"].items(), reverse=True)[:10]

                parts = []
                for _, (p, q) in asks_top:
                    parts.append(fmt_num(p))
                    parts.append(fmt_num(q))
                for _, (p, q) in bids_top:
                    parts.append(fmt_num(p))
                    parts.append(fmt_num(q))

                payload = "".join(parts)
                computed = zlib.crc32(payload.encode("ascii"))
                received_int = int(received)

                seen += 1
                print(f"\n=== message {seen} ({msg_type}) ===")
                print("asks (top10, ascending):", asks_top)
                print("bids (top10, descending):", bids_top)
                print("checksum input string  :", payload)
                print(f"computed={computed}  received={received_int}  MATCH={computed == received_int}")

                if seen >= MAX_MESSAGES:
                    return

asyncio.run(main())