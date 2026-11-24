#!/usr/bin/env python3
"""Run the packaged CrewDrop guest on loopback with temporary, synthetic data."""

import argparse
import concurrent.futures
import contextlib
import http.client
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time


DOMAIN = "crew.example.test"
OWNER = "owner@crew.example.test"
APP = "d-1234567890abcde"
PUBLIC_ROUTE = "/api/bundle/auth-mode"


def require(ok, message):
    if not ok:
        raise AssertionError(message)


def loopback_ports():
    # Hold all three until chosen so the OS cannot return a duplicate.
    with contextlib.ExitStack() as ports:
        return [reserve_port(ports) for _ in range(3)]


def reserve_port(stack):
    listener = stack.enter_context(socket.socket())
    listener.bind(("127.0.0.1", 0))
    return listener.getsockname()[1]


class Guest:
    def __init__(self, binary, scratch, volume, replica, **settings):
        self.binary = binary
        self.volume = volume
        self.remote = replica
        self.public, self.hooks, backend = loopback_ports()
        self.token = secrets.token_urlsafe(24)
        # Keep the caller's home, credentials, and services out of this run.
        self.env = {
            "PATH": os.defpath, "HOME": str(scratch), "TMPDIR": str(scratch),
            "NIBRUN_DATA_DIR": str(volume), "CREW_DROP_APP_ID": APP,
            "AWS_REGION": "eu-north-1", "AWS_WORKLOAD_IDENTITY": "async",
            "AWS_EC2_METADATA_DISABLED": "true",
            "CREW_DROP_PUBLIC_ADDR": f"127.0.0.1:{self.public}",
            "CREW_DROP_HOOK_ADDR": f"127.0.0.1:{self.hooks}",
            "CREW_DROP_BACKEND_ADDR": f"127.0.0.1:{backend}",
            "CREW_DROP_HOOK_TOKEN": self.token, "CREW_DROP_WAIT": "25s",
            "CREW_DROP_HOOK_TIMEOUT": "25s", "POCKETCONTEXT_DOMAIN": DOMAIN,
            "POCKETCONTEXT_OWNER_EMAIL": OWNER, "POCKETCONTEXT_SMTP_FROM": "signin@" + DOMAIN,
            "POCKETCONTEXT_SMTP_HOST": "127.0.0.1", "POCKETCONTEXT_SMTP_PORT": "1",
            "POCKETCONTEXT_SMTP_TLS": "tls", "POCKETCONTEXT_SMTP_USERNAME": "fixture",
            "POCKETCONTEXT_SMTP_PASSWORD": secrets.token_urlsafe(16),
            **settings,
        }
        if replica:
            self.env["CREW_DROP_FILE_REPLICA"] = str(replica)

    def __enter__(self):
        self.log = tempfile.TemporaryFile()
        self.proc = subprocess.Popen(
            [self.binary, "drop-serve"], env=self.env, stdin=subprocess.DEVNULL,
            stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            self.until(self.tcp_listening, "public TCP listener")
        except BaseException:
            self.close(True)
            raise
        return self

    def tcp_listening(self):
        with socket.create_connection(("127.0.0.1", self.public), timeout=1):
            return True

    def __exit__(self, kind, exc, tb):
        self.close(kind is not None)

    def close(self, failed):
        try:
            self.proc.terminate()
            self.proc.wait(timeout=12)
        except subprocess.TimeoutExpired:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait()
            if not failed:
                raise RuntimeError("supervisor did not exit on SIGTERM")
        finally:
            if failed:
                self.log.seek(0)
                print(self.log.read().decode(errors="replace")[-1800:], flush=True)
            self.log.close()

    def request(self, path, *, private=False, method="GET", auth=False, body=None):
        headers = {"Host": DOMAIN}
        if auth:
            headers["Authorization"] = "Bearer " + self.token
        if body is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(body)
        port = self.hooks if private else self.public
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        try:
            conn.request(method, path, body=body, headers=headers)
            response = conn.getresponse()
            return response.status, response.read()
        finally:
            conn.close()

    def status(self):
        status, raw = self.request("/status", private=True, auth=True)
        require(status == 200, f"private status returned {status}")
        return json.loads(raw)

    def until(self, condition, label, limit=70):
        deadline = time.monotonic() + limit
        while time.monotonic() < deadline:
            require(self.proc.poll() is None, f"supervisor exited while waiting for {label}")
            try:
                if value := condition():
                    return value
            except ConnectionError:
                pass
            time.sleep(0.025)
        raise TimeoutError(f"timed out waiting for {label}")

    def control(self, path):
        code, _ = self.request(path, private=True, auth=True, method="POST")
        require(code == 200, f"{path} returned {code}")

    def crew_route(self, name, payload):
        code, _ = self.request("/api/crew/" + name, method="POST", body=payload)
        require(code == 401, f"anonymous POST /api/crew/{name} returned {code}, expected 401 from Crew's registered route")

    def crew_routes(self):
        self.crew_route("read", {"collection": "notes"})
        self.crew_route("write", {})

    def public_ok(self):
        code, raw = self.request(PUBLIC_ROUTE)
        require(code == 200 and json.loads(raw).get("mode") == "email", f"hosted auth mode returned {code}: {raw[:120]!r}")


def durable_rows(volume):
    path = volume / "crew/private/live/data.db"
    with contextlib.closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        integrity = db.execute("PRAGMA integrity_check").fetchall()
        require(integrity == [("ok",)], f"SQLite integrity check returned {integrity[:1]}")
        owners = db.execute("SELECT id, email FROM users ORDER BY id").fetchall()
        require(len(owners) == 1 and owners[0][1] == OWNER, "expected exactly the synthetic owner")
        app_ids = db.execute("SELECT app_id FROM _crew_drop_identity").fetchall()
        require(app_ids == [(APP,)], "database belongs to another application")
        return owners[0], app_ids[0]


def check(binary, scratch):
    replica, source = scratch / "replica", scratch / "source"
    with Guest(binary, scratch, source, replica, CREW_DROP_BOOTSTRAP="1") as guest:
        guest.public_ok()
        require(guest.status()["boot_mode"] == "new", "expected explicit initial enrollment")
        code, _ = guest.request("/status", private=True)
        require(code == 401, "private status accepted an anonymous caller")
        code, _ = guest.request("/status", auth=True)
        require(code == 404, "control status is reachable on the public listener")
        code, _ = guest.request("/ready")
        require(code == 404, "public listener still implements a synthetic readiness route")
        for method, path in (("GET", "/health"), ("POST", "/hooks/start")):
            code, _ = guest.request(path, private=True, auth=True, method=method)
            require(code == 404, f"private listener unexpectedly served {method} {path}")
        guest.crew_routes()
        baseline = durable_rows(source)
        # Record the real content digest while the process still owns its
        # expanded assets, whether they live on the volume or in TMPDIR.
        cache_ids = {path.parents[1].name for path in scratch.rglob("pb_hooks/crew.pb.js")
                     if (path.parents[1] / ".complete").is_file()}
        require(len(cache_ids) == 1, "expected one active content-addressed hook cache")
        guest.until(lambda: guest.status()["replication_state"] == "ready", "filesystem replica")
        guest.control("/control/sync")
    print("PASS first boot, public/private routing, Crew authorization and durable sync", flush=True)

    # A lost VM left a complete marker on its durable volume even though
    # some cached JavaScript files had lost their bytes. Recreate that exact
    # condition in this temporary volume, preserving its database and marker.
    persistent_cache = source / "crew/app"
    if persistent_cache.is_symlink():
        # Replace only the disposable test's generated runtime pointer. A real
        # upgrade may instead find the older physical cache directory here.
        persistent_cache.unlink()
    for cache_id in cache_ids:
        hook = persistent_cache / cache_id / "pb_hooks/crew.pb.js"
        hook.parent.mkdir(parents=True, exist_ok=True)
        (hook.parents[1] / ".complete").touch()
        hook.write_bytes(b"")
    missing_token = scratch / "no-such-token"
    with Guest(binary, scratch, source, None, CREW_DROP_BUCKET="synthetic-unused",
               AWS_ROLE_ARN="arn:aws:iam::123456789012:role/unused",
               AWS_WEB_IDENTITY_TOKEN_FILE=str(missing_token), CREW_DROP_HOLD_HOOKS="1") as guest:
        guest.until(guest.status, "protected hook listener")
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            blocked = pool.submit(guest.control, "/hooks/resume")
            guest.until(lambda: guest.status()["hook_waiters"] == 1, "held lifecycle request")
            guest.public_ok()
            guest.crew_routes()
            state = guest.status()
            require(state["boot_mode"] == "local" and state["data_state"] == "ready", "expected a live local database")
            require(not blocked.done() and state["replication_state"] != "ready" and not missing_token.exists(),
                    "first public request did not precede lifecycle and AWS readiness")
            require(durable_rows(source) == baseline, "local restart changed owner or identity")
            guest.control("/control/release?gate=hooks")
            blocked.result(timeout=5)
    print("PASS local Crew routes recovered from an incomplete cache with a blocked hook and absent AWS token", flush=True)

    target = scratch / "target"
    with Guest(binary, scratch, target, replica, CREW_DROP_HOLD_RECOVERY="1") as guest:
        with concurrent.futures.ThreadPoolExecutor(3) as pool:
            requests = [
                pool.submit(guest.public_ok),
                pool.submit(guest.crew_route, "read", {"collection": "notes"}),
                pool.submit(guest.crew_route, "write", {}),
            ]

            def pending_recovery():
                state = guest.status()
                if state["data_waiters"] == 3 and state["recovery_attempts"] > 0:
                    return state

            state = guest.until(pending_recovery, "three pending public requests and a recovery worker")
            require(state["recovery_attempts"] == 1 and state["restore_attempts"] == 0,
                    "cold worker started multiple attempts or bypassed the recovery gate")
            require(not (target / "crew/private/live/data.db").exists(), "cold start opened a live database early")
            guest.control("/control/release?gate=recovery")
            for result in requests:
                result.result(timeout=30)
        state = guest.status()
        require(state["boot_mode"] == "recovered" and state["restore_attempts"] == state["recovery_attempts"] == 1,
                "queued requests did not share exactly one restore")
        guest.crew_routes()
        require(durable_rows(target) == baseline, "restored owner or identity differs from source")
    print("PASS three first requests shared one restore; SQLite integrity, owner ID and app binding match", flush=True)

    # Keep the first enrollment record and omit its LTX data. Even if an operator
    # leaves bootstrap enabled on the next start, an established app must not reset.
    broken = scratch / "broken-replica" / APP / "crew-drop"
    broken.mkdir(parents=True)
    (broken / "enrollment").write_bytes((replica / APP / "crew-drop/enrollment").read_bytes())
    lost = scratch / "lost"
    with Guest(binary, scratch, lost, scratch / "broken-replica", CREW_DROP_BOOTSTRAP="1") as guest:
        guest.until(lambda: guest.status()["data_state"] == "failed", "missing replica rejection")
        code, _ = guest.request(PUBLIC_ROUTE)
        require(code == 503, f"missing replica served HTTP {code}")
        require(not any((lost / relative).exists() for relative in ("crew/private/live/data.db", "crew/project/crew.db")),
                "a failed restore published an empty database")
    print("PASS missing replica refused to seed or serve an established app", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parent / "build/pocketcontext-crew-drop-linux-amd64")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    with tempfile.TemporaryDirectory(prefix="cd-", dir="/tmp") as root:
        check(binary, Path(root))
