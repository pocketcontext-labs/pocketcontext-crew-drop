#!/usr/bin/env python3
"""Measure whether an idle CrewDrop guest writes to its database.

Starts the packaged guest on loopback with a temporary volume and file
replica, waits for replication, confirms a sync, then sends no requests for
the idle window. It reports changes to the live database and WAL and new
Litestream LTX files. Exit status 1 means the idle window caused writes: a
changed database or WAL, or a new level-0 file (an upload of new WAL data).
Higher-level files only compact data written before the window; they are
listed but do not fail the run.

The settle period must exceed Litestream's one-minute checkpoint interval:
Litestream checkpoints the startup WAL once (a single LTX file recording its
_litestream_seq bookkeeping) and does not repeat that without new writes.
"""

import argparse
from pathlib import Path
import tempfile
import time

from check import APP, Guest, require


def snapshot(volume, replica):
    live = volume / "crew/private/live"
    files = {p.name: (p.stat().st_size, p.stat().st_mtime_ns) for p in live.iterdir()
             if p.name.startswith("data.db")}
    ltx = sorted(str(p.relative_to(replica)) for p in replica.rglob("*.ltx"))
    return files, ltx


def measure(binary, scratch, idle, interval, settle):
    replica, volume = scratch / "replica", scratch / "volume"
    with Guest(binary, scratch, volume, replica, CREW_DROP_BOOTSTRAP="1",
               DROP_REPLICA_SYNC_INTERVAL=interval) as guest:
        guest.public_ok()
        guest.until(lambda: guest.status()["replication_state"] == "ready", "filesystem replica")
        require(guest.status()["replica_sync_interval"] == interval, "guest ignored the sync interval")
        # Let startup and Litestream's one-time checkpoint settle, then confirm
        # the replicated cut the window starts from.
        time.sleep(settle)
        guest.control("/control/sync")
        before = snapshot(volume, replica / APP)
        print(f"idle for {idle}s with sync interval {interval}", flush=True)
        time.sleep(idle)
        after = snapshot(volume, replica / APP)
    new_ltx = sorted(set(after[1]) - set(before[1]))
    changed = {name: (before[0].get(name), value) for name, value in after[0].items()
               if before[0].get(name) != value}
    for name, (old, new) in sorted(changed.items()):
        print(f"changed {name}: size/mtime {old} -> {new}")
    uploads = [name for name in new_ltx if "/ltx/0/" in f"/{name}"]
    for name in new_ltx:
        print(f"new replica file {name}" + ("" if name in uploads else " (compaction of earlier data)"))
    if changed or uploads:
        print("FAIL idle guest wrote to its database", flush=True)
        return 1
    print("PASS idle guest made no database writes and uploaded no new WAL data", flush=True)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parent / "build/pocketcontext-crew-drop-linux-amd64")
    parser.add_argument("--idle", type=int, default=360, help="idle window in seconds")
    parser.add_argument("--interval", default="1s", help="DROP_REPLICA_SYNC_INTERVAL for the run")
    parser.add_argument("--settle", type=int, default=90, help="seconds after replication is ready before the window")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="cd-idle-", dir="/tmp") as root:
        raise SystemExit(measure(binary, Path(root), args.idle, args.interval, args.settle))
