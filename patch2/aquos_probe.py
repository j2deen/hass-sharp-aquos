#!/usr/bin/env python3
"""Raw capture probe for Sharp AQUOS IP control (TCP, default port 10002).

Read-only by default: it only sends status queries (POWR?, VOLM?, ...).
It records exactly what the TV sends back, chunk by chunk with timings, so
we can see how replies are framed and what the TV answers in each state
(on / wallpaper / standby).

Usage:
    python3 aquos_probe.py 172.16.20.85 --label on
    python3 aquos_probe.py 172.16.20.85 --label wallpaper
    python3 aquos_probe.py 172.16.20.85 --label standby

    # Same exchange the integration performs (login sent in one write):
    python3 aquos_probe.py 172.16.20.85 --label on --burst

    # Explicit set command (NOT read-only) - e.g. "off" while in wallpaper:
    python3 aquos_probe.py 172.16.20.85 --label wallpaper --send POWR 0

Results are printed and appended to aquos_probe.jsonl next to this script.
Standard library only; needs Python 3.8+.
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

DEFAULT_QUERIES = ["POWR", "VOLM", "MUTE", "IAVD", "AVMD", "WIDE"]


def recv_until_quiet(sock: socket.socket, first_wait: float, quiet: float) -> list[dict]:
    """Collect chunks: wait up to first_wait for the first byte, then keep
    reading until `quiet` seconds pass with nothing new."""
    chunks: list[dict] = []
    start = time.monotonic()
    sock.settimeout(first_wait)
    while True:
        try:
            data = sock.recv(1024)
        except socket.timeout:
            break
        except OSError as err:
            chunks.append({"t": round(time.monotonic() - start, 3), "error": repr(err)})
            break
        elapsed = round(time.monotonic() - start, 3)
        if not data:
            chunks.append({"t": elapsed, "closed": True})
            break
        chunks.append({"t": elapsed, "data": data.decode("latin-1")})
        sock.settimeout(quiet)
    return chunks


def exchange(host: str, port: int, user: str, password: str, code: str, value: str,
             burst: bool, connect_timeout: float) -> dict:
    """One full connection: login, one command, capture everything."""
    result: dict = {"command": f"{code}{value}", "burst": burst, "phases": {}}
    command = f"{code}{str(value).ljust(4)}\r".encode()
    t0 = time.monotonic()
    try:
        sock = socket.create_connection((host, port), timeout=connect_timeout)
    except OSError as err:
        result["connect_error"] = repr(err)
        result["connect_s"] = round(time.monotonic() - t0, 3)
        return result
    result["connect_s"] = round(time.monotonic() - t0, 3)

    try:
        phases = result["phases"]
        # Anything the TV says before we send a byte (login prompt, if any).
        phases["banner"] = recv_until_quiet(sock, first_wait=1.5, quiet=0.5)
        if burst:
            # Exactly what the integration does: user and password in one write.
            sock.sendall(f"{user}\r{password}\r".encode())
            phases["after_login"] = recv_until_quiet(sock, first_wait=3.0, quiet=0.7)
        else:
            sock.sendall(f"{user}\r".encode())
            phases["after_user"] = recv_until_quiet(sock, first_wait=3.0, quiet=0.7)
            sock.sendall(f"{password}\r".encode())
            phases["after_password"] = recv_until_quiet(sock, first_wait=3.0, quiet=0.7)
        sock.sendall(command)
        phases["reply"] = recv_until_quiet(sock, first_wait=6.0, quiet=1.0)
    except OSError as err:
        result["io_error"] = repr(err)
    finally:
        try:
            sock.close()
        except OSError:
            pass
    result["total_s"] = round(time.monotonic() - t0, 3)
    return result


def reply_text(result: dict) -> str:
    if "connect_error" in result:
        return "<connect failed>"
    chunks = result["phases"].get("reply", [])
    text = "".join(c.get("data", "") for c in chunks)
    if not chunks:
        return "<no reply>"
    return text


def show(result: dict) -> None:
    print(f"\n== {result['command']!r}  connect={result.get('connect_s')}s "
          f"total={result.get('total_s', '-')}s")
    if "connect_error" in result:
        print(f"   connect error: {result['connect_error']}")
        return
    for phase, chunks in result["phases"].items():
        if not chunks:
            print(f"   {phase:<15} (nothing)")
        for c in chunks:
            if "data" in c:
                print(f"   {phase:<15} +{c['t']:>6.3f}s  {c['data']!r}")
            elif c.get("closed"):
                print(f"   {phase:<15} +{c['t']:>6.3f}s  <TV closed the connection>")
            else:
                print(f"   {phase:<15} +{c['t']:>6.3f}s  error {c['error']}")
    if "io_error" in result:
        print(f"   io error: {result['io_error']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("host")
    ap.add_argument("--port", type=int, default=10002)
    ap.add_argument("--user", default="")
    ap.add_argument("--password", default="")
    ap.add_argument("--label", required=True,
                    help="what state the TV is in right now, e.g. on / wallpaper / standby")
    ap.add_argument("--queries", nargs="+", default=DEFAULT_QUERIES,
                    help="four-letter status codes to query (default: %(default)s)")
    ap.add_argument("--repeat", type=int, default=5, help="rounds of all queries (default 5)")
    ap.add_argument("--gap", type=float, default=0.5, help="pause between connections, seconds")
    ap.add_argument("--burst", action="store_true",
                    help="send user+password in one write, as the integration does")
    ap.add_argument("--send", nargs=2, metavar=("CODE", "VALUE"),
                    help="send ONE set command instead of the queries (not read-only), e.g. --send POWR 0")
    ap.add_argument("--connect-timeout", type=float, default=3.0)
    ap.add_argument("--out", default=str(Path(__file__).with_name("aquos_probe.jsonl")))
    args = ap.parse_args()

    if args.send:
        plan = [(args.send[0].upper(), args.send[1])]
        rounds = 1
        print(f"Sending SET command {plan[0][0]}{plan[0][1]} to {args.host}:{args.port} "
              f"(label={args.label})")
    else:
        plan = [(q.upper(), "?") for q in args.queries]
        rounds = args.repeat
        print(f"Read-only probe of {args.host}:{args.port} (label={args.label}, "
              f"{rounds} rounds, {'burst' if args.burst else 'stepwise'} login)")

    summary: dict[str, Counter] = defaultdict(Counter)
    timings: dict[str, list[float]] = defaultdict(list)
    with open(args.out, "a", encoding="utf-8") as fh:
        for rnd in range(1, rounds + 1):
            for code, value in plan:
                res = exchange(args.host, args.port, args.user, args.password, code, value,
                               args.burst, args.connect_timeout)
                res.update({"label": args.label, "round": rnd,
                            "when": datetime.now().astimezone().isoformat(timespec="seconds")})
                fh.write(json.dumps(res) + "\n")
                show(res)
                summary[res["command"]][reply_text(res)] += 1
                if "total_s" in res:
                    timings[res["command"]].append(res["total_s"])
                time.sleep(args.gap)

    print("\n" + "=" * 60)
    print(f"SUMMARY  label={args.label}")
    for cmd, counts in summary.items():
        t = timings.get(cmd)
        avg = f"  avg {sum(t) / len(t):.2f}s per exchange" if t else ""
        print(f"  {cmd}{avg}")
        for text, n in counts.most_common():
            print(f"      {n:>3} x  {text!r}")
    print(f"\nFull capture appended to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
