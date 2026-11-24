#!/usr/bin/python3
"""Run source, warm restart, abrupt same-volume restart, and hard-fenced fresh-worker acceptance."""

import argparse
import concurrent.futures
import json
import os
import select
import shlex
import shutil
import sqlite3
import subprocess
import threading
import time
from datetime import datetime

from aws_checks import check_file, remote_note, trust, write_gate
from bootstrap import APP, APP_ID, RUN_CONFIG, dev
from client import (
    PRIVATE,
    RESULTS,
    check_attachment,
    expect,
    guest_ip,
    hook,
    issue_owner,
    journal,
    new_note,
    owner,
    populate,
    public_until,
    read_note,
    request,
    safe_status,
    save,
    status,
    utc,
)
from experiment import report, signer
from provision import cloud
from worker import driver, ssh


def wait_status(role, condition, event, limit=115):
    until = time.monotonic() + limit
    last = {}
    while time.monotonic() < until:
        last = status(role)
        if condition(last):
            journal(event, role=role, **safe_status(last))
            return last
        time.sleep(0.2)
    raise RuntimeError(f"{event}: condition timed out; last={safe_status(last)}")


def wait_state(role, target):
    until = time.monotonic() + 120
    while time.monotonic() < until:
        row = next((i for i in report(role)["instances"] if i["appId"] == APP_ID), {})
        if row.get("state") == target:
            journal("worker-" + target, role=role, **row)
            return
        time.sleep(0.3)
    raise RuntimeError(role + " did not report " + target)


def ctl(role, path):
    ip = guest_ip(role)
    if not ip:
        raise RuntimeError("missing guest address")
    began = utc()
    code, data = hook(role, ip, "POST", path)
    journal("private-hook", role=role, path=path, http=code, at_start=began)
    if code != 200:
        raise ValueError(path + " returned " + str(code))
    return data


def source():
    for role in ("source", "dest"):
        cloud.verify_role(role, RUN_CONFIG)
        row = report(role)
        if row["volumes"] or row["instances"]:
            raise ValueError("initial placement requires two empty runners")
    signer("source")
    journal("source-place-request")
    dev("place", APP, RUN_CONFIG["workers"]["source"]["name"])
    populate()
    wait_status(
        "source",
        lambda s: s.get("boot_mode") == "new" and s.get("replication_state") == "ready",
        "initial-replicator-ready",
    )
    ip = guest_ip("source")
    code, _ = hook("source", ip, "GET", "/status", authenticated=False)
    if code != 401:
        raise ValueError("private hook accepted an anonymous caller")
    journal("private-hook-anonymous", role="source", http=code)
    for route in ("/status", "/hooks/resume"):
        method = "GET" if route == "/status" else "POST"
        expect("source", method, route, event="public-hook-hidden", code=404)
    check_file()
    ctl("source", "/control/sync")


def upgrade():
    """Prove a corrected release can reuse previously acknowledged data on the retained disk."""
    release = json.loads((cloud.EVIDENCE / "release.json").read_bytes())
    deployment = dev("app", "show", APP)["app"]["deployment_id"]
    end = time.monotonic() + 120
    while time.monotonic() < end:
        row = next(
            (a for a in report("source")["instances"] if a["appId"] == APP_ID), {}
        )
        if row.get("deploymentId") == deployment and row.get("state") == "running":
            break
        time.sleep(0.3)
    else:
        raise RuntimeError("corrected release did not activate")
    wait_status(
        "source",
        lambda s: (
            s.get("boot_mode") == "local" and s.get("replication_state") == "ready"
        ),
        "corrected-old-volume-ready",
    )
    ctl("source", "/control/release?gate=hooks")
    rec = json.loads((PRIVATE / "records.json").read_bytes())
    read_note("source", rec["notes"][0], owner("source"), "corrected-old-volume-read")
    check_attachment(
        "source", rec["attachment"], owner("source"), "corrected-old-volume"
    )
    check_file()
    new_note("source", "corrected-old-volume-write")
    ctl("source", "/control/sync")
    journal(
        "corrupt-code-cache-repaired",
        deployment_id=deployment,
        source_sha256=release["sha256"],
    )


def warm():
    rec = json.loads((PRIVATE / "records.json").read_bytes())
    ctl("source", "/hooks/suspend")
    dev("app", "stop", APP)
    wait_state("source", "stopped")
    dev("app", "env", APP, "CREW_DROP_BOOTSTRAP=0", "CREW_DROP_HOLD_HOOKS=1")
    trust(False)
    journal("local-restart-request")
    dev("app", "start", APP)
    try:
        # Observe only the private listener while waiting for an actual in-flight hook.
        wait_status(
            "source",
            lambda s: bool(s.get("held", {}).get("hooks")),
            "local-private-listening",
        )
        with concurrent.futures.ThreadPoolExecutor(3) as p:
            held = p.submit(ctl, "source", "/hooks/resume")
            try:
                wait_status(
                    "source",
                    lambda s: s.get("hook_waiters", 0) >= 1,
                    "local-hook-pending",
                )
                read = p.submit(
                    read_note,
                    "source",
                    rec["notes"][0],
                    owner("source"),
                    "local-first-read",
                )
                write = p.submit(new_note, "source", "local-first-write")
                read.result()
                note = write.result()
                read_note("source", note, owner("source"), "local-denied-read-back")
                while_pending = wait_status(
                    "source",
                    lambda s: s.get("hook_waiters", 0) >= 1,
                    "local-io-while-hook-pending",
                )
                if (
                    while_pending.get("boot_mode") != "local"
                    or while_pending["restore_attempts"] != 0
                    or held.done()
                    or while_pending.get("replication_state") == "ready"
                ):
                    raise ValueError(
                        "warm test did not actually overlap the injected faults"
                    )
            finally:
                ctl("source", "/control/release?gate=hooks")
            held.result()
        check_attachment("source", rec["attachment"], owner("source"), "local-cached")
        # Independently confirm IAM remained denied after the write acknowledgements.
        trust(False)
    finally:
        trust(True)
    wait_status(
        "source",
        lambda s: s.get("replication_state") == "ready",
        "local-replication-caught-up",
        160,
    )
    ctl("source", "/control/sync")


def measure_external_activation():
    """Start an independent public reader at the same time as the dev Drop start request."""
    original = json.loads((PRIVATE / "records.json").read_bytes())["notes"][0]
    dev("app", "stop", APP)
    wait_state("source", "stopped")
    start = time.monotonic()
    journal("external-activation-start", role="source")
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        launch = pool.submit(dev, "app", "start", APP)
        deadline = start + 120
        attempts = 0
        while time.monotonic() < deadline:
            attempts += 1
            metrics, _, raw = request(
                "source",
                "POST",
                "/api/crew/read",
                token=owner("source"),
                body={"collection": "notes", "id": original["id"]},
                timeout=10,
            )
            journal("external-activation-read", attempt=attempts, **metrics)
            if metrics["http"] == 200:
                rows = json.loads(raw)["items"]
                if len(rows) != 1 or rows[0]["body"] != original["body"]:
                    raise ValueError(
                        "first external activation read did not return the acknowledged marker"
                    )
                journal(
                    "external-activation-usable",
                    elapsed_ms=round(1000 * (time.monotonic() - start), 3),
                    attempts=attempts,
                )
                break
            time.sleep(0.10)
        else:
            raise RuntimeError(
                "external activation reader never received the authenticated note"
            )
        launch.result()
    ctl("source", "/control/release?gate=hooks")
    wait_status(
        "source",
        lambda s: s.get("replication_state") == "ready",
        "external-activation-replicator",
    )
    ctl("source", "/control/sync")


def crash():
    measure_external_activation()
    cloud.verify_role("source", RUN_CONFIG)
    rec = json.loads((PRIVATE / "records.json").read_bytes())
    read_note("source", rec["notes"][0], owner("source"), "before-abrupt-read")
    write_gate(True)
    try:
        note = crash_while_replica_denied()
        if remote_note("after-crash-replica-still-denied", note):
            raise ValueError(
                "immediate-crash marker reached S3 during the write denial"
            )
    finally:
        write_gate(False)
    wait_status(
        "source",
        lambda s: (
            s.get("boot_mode") == "local" and s.get("replication_state") == "ready"
        ),
        "same-volume-replicator-caught-up",
        155,
    )
    ctl("source", "/control/sync")
    if not remote_note("after-crash-replica-resumed", note):
        raise ValueError("replica never caught up with the recovered local marker")


def verify_local_db_and_wal(note):
    """Read only the disposable application's ext4 backing file into a private, independent SQLite copy."""
    disk = "/var/lib/nibrunner/volumes/" + APP_ID + "-vol"
    base = PRIVATE / "forensics" / "same-volume-recovered"
    base.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name in ("data.db", "data.db-wal"):
        read = (
            "sudo debugfs -R "
            + shlex.quote("cat /upper/app/data/crew/private/live/" + name)
            + " "
            + disk
        )
        with (base / name).open("wb") as out:
            result = subprocess.run(
                [*driver("source"), read],
                stdout=out,
                stderr=subprocess.PIPE,
                check=True,
            )
        if b"File not found" in result.stderr and name == "data.db":
            raise ValueError("recovered physical working DB missing")
    shutil.copyfile(base / "data.db", base / "base-only.db")

    def lookup(path):
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("physical working DB integrity check failed")
            found = db.execute(
                "SELECT title, body FROM crew_notes WHERE id=?", (note["id"],)
            ).fetchone()
            return found == (note["title"], note["body"])

    in_full = lookup(base / "data.db")
    try:
        base_only = lookup(base / "base-only.db")
    except sqlite3.DatabaseError:
        base_only = None
    if not in_full:
        raise ValueError(
            "the independent physical SQLite/WAL copy lacks the acknowledged marker"
        )
    journal(
        "independent-physical-sqlite",
        note_present=in_full,
        base_only_contains_marker=base_only,
        sqlite_bytes=(base / "data.db").stat().st_size,
        wal_bytes=(base / "data.db-wal").stat().st_size,
        integrity="ok",
    )


def crash_while_replica_denied():
    rec = json.loads((PRIVATE / "records.json").read_bytes())
    # The fresh disposable source has one VM, and only that recorded process may receive SIGKILL.
    code = """
import json, os, pathlib, re, signal, sys
a=sys.argv[1]
base=pathlib.Path('/run/nibrunner')
files=list(base.glob('vm-*.json'))
if len(files)!=1 or files[0].name!='vm-'+a+'.json':
    raise SystemExit('unexpected VM inventory')
r=json.loads(files[0].read_bytes())
proc=pathlib.Path('/proc')/str(r['pid'])
if r['appId']!=a or r.get('exitCode') is not None or r.get('signal') or r.get('stopRequested'):
    raise SystemExit('unexpected VM receipt')
jail=pathlib.Path(r['jailRoot'])
if ((proc/'comm').read_text().strip()!='firecracker' or not re.fullmatch('[a-f0-9]{32}',jail.parent.name)
        or r['hostBootId']!=pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        or jail.parent.name not in (proc/'cgroup').read_text()):
    raise SystemExit('VM process identity mismatch')
fd=os.pidfd_open(r['pid'])
try:
    if not os.path.samefile(proc/'root',jail):
        raise SystemExit('VM jail root mismatch')
    print('armed', flush=True)
    if sys.stdin.readline().strip() != a:
        raise SystemExit('fault was never triggered')
    signal.pidfd_send_signal(fd, signal.SIGKILL)
finally:
    os.close(fd)
print(json.dumps({'app_id':a,'pid':r['pid'],'started_at_ms':r['startedAtMs'],'signal':'SIGKILL'}))
"""
    with subprocess.Popen(
        [*driver("source"), "sudo python3 -c " + shlex.quote(code) + " " + APP_ID],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as proc:
        if (
            not select.select([proc.stdout], [], [], 8)[0]
            or proc.stdout.readline().strip() != b"armed"
        ):
            proc.terminate()
            raise RuntimeError(
                "could not arm the verified disposable Firecracker process"
            )
        try:
            note = new_note("source", "before-process-loss")
            send_at = utc()
            out, err = proc.communicate((APP_ID + "\n").encode(), timeout=8)
        except BaseException:
            proc.stdin.close()
            raise
        if proc.returncode:
            raise RuntimeError("same-volume SIGKILL did not complete")
    journal("same-volume-process-killed", triggered_at=send_at, **json.loads(out))
    # This first post-fault measurement includes retries until an authenticated business response.
    start = time.monotonic()
    until = start + 140
    attempts = 0
    while time.monotonic() < until:
        attempts += 1
        metrics, _, raw = request(
            "source",
            "POST",
            "/api/crew/read",
            token=owner("source"),
            body={"collection": "notes", "id": note["id"]},
            timeout=10,
        )
        journal("same-volume-restart-attempt", attempt=attempts, **metrics)
        if metrics["http"] == 200:
            rows = json.loads(raw)["items"]
            if not rows or rows[0]["body"] != note["body"]:
                raise ValueError(
                    "the first authenticated read after SIGKILL lost the acknowledged marker"
                )
            break
        time.sleep(0.15)
    else:
        raise RuntimeError("same-volume restart never served the authenticated read")
    journal(
        "same-volume-first-read",
        total_ms=round((time.monotonic() - start) * 1000, 3),
        attempts=attempts,
    )
    verify_local_db_and_wal(note)
    current = json.loads((PRIVATE / "records.json").read_bytes())
    read_note(
        "source", current["notes"][-1], owner("source"), "same-volume-acknowledged-note"
    )
    next_note = new_note("source", "after-process-loss")
    read_note("source", next_note, owner("source"), "same-volume-new-write-read-back")
    check_attachment("source", rec["attachment"], owner("source"), "same-volume")
    # The hook hold is an environment input and returns after process loss.
    ctl("source", "/control/release?gate=hooks")
    row = wait_status(
        "source",
        lambda s: s.get("boot_mode") == "local" and s.get("data_state") == "ready",
        "same-volume-recovered",
    )
    if row["restore_attempts"] != 0:
        raise ValueError("same-volume restart unexpectedly restored from S3")
    return note


def tail_traffic(stop, ready, failed):
    records = json.loads((PRIVATE / "records.json").read_bytes())
    token = owner("source")
    index = 0
    ack = 0
    clock = time.monotonic()
    try:
        while not stop.is_set():
            index += 1
            title = f"fence-tail-{RUN_CONFIG['run']}-{index:04d}"
            data = {
                "title": title,
                "bucket": records["bucket_id"],
                "body": "Synthetic acknowledged-tail record " + title,
            }
            envelope = {
                "action": "note.create",
                "reported_agent": "Acceptance",
                "reason": "Synthetic fault boundary",
                "data": data,
                "idempotency_key": title,
            }
            journal("tail-attempt", index=index, title=title)
            metrics, _, raw = request(
                "source",
                "POST",
                "/api/crew/write",
                body=envelope,
                token=token,
                timeout=2,
            )
            observed = {"index": index, "title": title, **metrics}
            if metrics["http"] == 200:
                row = json.loads(raw)
                if row["title"] != title or row["body"] != data["body"]:
                    raise ValueError("incorrect success acknowledgement")
                observed["id"] = row["id"]
                ack += 1
            journal("tail-response", **observed)
            if ack >= 8:
                ready.set()
            elif not metrics["http"] == 200:
                raise ValueError("tail write failed before the fault was issued")
            if ack >= 8 and metrics["http"] == 0:
                break
            clock += 0.18
            stop.wait(max(0, clock - time.monotonic()))
    except Exception as error:
        failed.append(str(error))
        ready.set()


def verify_tail(rec, token):
    observations = [
        json.loads(line)
        for line in (RESULTS / "observations.jsonl").read_text().splitlines()
    ]
    attempts = {o["index"]: o for o in observations if o["event"] == "tail-attempt"}
    responses = {o["index"]: o for o in observations if o["event"] == "tail-response"}
    if not attempts:
        raise RuntimeError("no externally journaled in-flight writes")
    _, _, raw = expect(
        "dest",
        "POST",
        "/api/crew/read",
        event="restore-tail-inventory",
        token=token,
        body={
            "collection": "notes",
            "query": "fence-tail-" + RUN_CONFIG["run"],
            "limit": 100,
        },
    )
    rows = json.loads(raw)["items"]
    by_title = {row["title"]: row for row in rows}
    ack = [i for i in sorted(attempts) if responses.get(i, {}).get("http") == 200]
    unknown = [i for i in sorted(attempts) if i not in ack]
    recovered = []
    for i in sorted(attempts):
        title = attempts[i]["title"]
        row = by_title.get(title)
        if row:
            if (
                row["body"] != "Synthetic acknowledged-tail record " + title
                or row["bucket"] != rec["bucket_id"]
            ):
                raise ValueError("restored tail record failed integrity check")
            if i in ack and responses[i]["id"] != row["id"]:
                raise ValueError("id differs from source ACK")
            recovered.append(i)
    kept = [i for i in ack if i in recovered]
    if kept != ack[: len(kept)]:
        raise ValueError("restored acknowledged writes have a hole")
    rejected = [i for i in unknown if i not in recovered]
    ms = None
    if kept:
        end = lambda i: datetime.fromisoformat(responses[i]["at_end"])
        ms = round(1000 * (end(ack[-1]) - end(kept[-1])).total_seconds(), 3)
    out = {
        "attempted": len(attempts),
        "acknowledged": len(ack),
        "recovered_acknowledged": len(kept),
        "lost_acknowledged": len(ack) - len(kept),
        "unacknowledged": len(unknown),
        "unacknowledged_present": len(unknown) - len(rejected),
        "unacknowledged_absent": len(rejected),
        "last_retained_to_last_ack_ms": ms,
        "contiguous_acknowledged_prefix": True,
        "first_lost_ack_index": ack[len(kept)] if len(kept) < len(ack) else None,
        "last_retained_ack_index": kept[-1] if kept else None,
        "last_ack_index": ack[-1] if ack else None,
    }
    save(RESULTS / "tail-recovery.json", out)
    journal("acknowledged-tail-result", **out)
    return out


def fence():
    current = cloud.verify_role("source", RUN_CONFIG)
    destination = cloud.verify_role("dest", RUN_CONFIG)
    if (
        destination["State"]["Name"] != "running"
        or current["State"]["Name"] != "running"
    ):
        raise ValueError("both disposable workers must be running")
    empty = report("dest")
    files = ssh(
        "dest",
        "sudo sh -c 'ls -A /var/lib/nibrunner/volumes 2>/dev/null || true'",
        capture_output=True,
    ).stdout
    if empty["instances"] or empty["volumes"] or files.strip():
        raise ValueError("replacement storage is not empty")
    save(
        RESULTS / "empty-destination.json",
        {
            "host": empty["hostId"],
            "empty_volumes": True,
            "empty_instances": True,
            "empty_volume_directory": True,
            "at": utc(),
        },
    )
    note = new_note("source", "last-before-physical-fence")
    read_note("source", note, owner("source"), "last-acknowledged-note-before-fence")
    ctl("source", "/control/sync")
    app = APP_ID
    instance = current["InstanceId"]
    stop = threading.Event()
    ready = threading.Event()
    failed = []
    writer = threading.Thread(target=tail_traffic, args=(stop, ready, failed))
    writer.start()
    try:
        if not ready.wait(20) or failed:
            raise ValueError("externally acknowledged tail failed to warm")
        journal(
            "ec2-source-stop-request",
            app_id=app,
            instance=instance,
            skip_os_shutdown=True,
        )
        cloud.aws(
            "ec2", "stop-instances", "--instance-ids", instance, "--skip-os-shutdown"
        )
        journal("ec2-source-stop-api-accepted", instance=instance)
        writer.join(6)
    finally:
        stop.set()
        writer.join(4)
    if failed or writer.is_alive():
        raise RuntimeError("tail writer failed: " + repr(failed))
    until = time.monotonic() + 220
    while time.monotonic() < until:
        state = cloud.verify_role("source", RUN_CONFIG)["State"]["Name"]
        journal("source-ec2-fence", instance=instance, state=state)
        if state == "stopped":
            break
        time.sleep(2)
    else:
        raise RuntimeError("source EC2 fence never confirmed")
    save(
        RESULTS / "fenced.json",
        {"at": utc(), "app_id": app, "instance": instance, "state": "stopped"},
    )
    subprocess.run(
        [
            "sudo",
            "systemctl",
            "stop",
            "drop-m1-loss-token-source",
            "drop-m1-loss-link-source",
        ],
        check=True,
    )


def destination():
    if (
        not (RESULTS / "fenced.json").exists()
        or cloud.verify_role("source", RUN_CONFIG)["State"]["Name"] != "stopped"
    ):
        raise ValueError("recovery requires a positively fenced source")
    before = report("dest")
    if before["instances"] or before["volumes"]:
        raise ValueError("replacement already owns local app data")
    rec = json.loads((PRIVATE / "records.json").read_bytes())
    dev(
        "app",
        "env",
        APP,
        "CREW_DROP_BOOTSTRAP=0",
        "CREW_DROP_HOLD_HOOKS=0",
        "CREW_DROP_HOLD_RECOVERY=1",
    )
    signer("dest")
    journal("dest-placement-request")
    dev("place", APP, RUN_CONFIG["workers"]["dest"]["name"])
    public_until("dest", "/ready")
    pending = wait_status(
        "dest",
        lambda s: (
            s.get("data_state") == "recovering" and s.get("held", {}).get("recovery")
        ),
        "fresh-listener-waiting-for-recovery",
    )
    if pending["restore_attempts"] != 0:
        raise ValueError("restore started before release of test hold")
    with concurrent.futures.ThreadPoolExecutor(2) as p:
        # The old session rejection and normal /up stay queued until the replica is restored.
        old = p.submit(
            expect,
            "dest",
            "POST",
            "/api/crew/read",
            event="fresh-revoked-owner-request",
            code=(401, 403),
            token=owner("source"),
            body={"collection": "notes", "id": rec["notes"][0]["id"]},
        )
        up = p.submit(expect, "dest", "GET", "/up", event="fresh-first-up")
        wait_status(
            "dest", lambda s: s.get("data_waiters", 0) >= 2, "fresh-requests-queued"
        )
        if old.done() or up.done():
            raise ValueError("a business route answered before restoring")
        ctl("dest", "/control/release?gate=recovery")
        up.result()
        old.result()
    s = wait_status(
        "dest",
        lambda s: s.get("boot_mode") == "recovered" and s.get("data_state") == "ready",
        "fresh-restore-completed",
    )
    if s["restore_attempts"] != 1:
        raise ValueError("cold recovery was not single-flight")
    token = issue_owner("dest")
    for i, note in enumerate(rec["notes"]):
        read_note("dest", note, token, "fresh-verified-note-" + str(i + 1))
    tail = verify_tail(rec, token)
    check_attachment("dest", rec["attachment"], token, "fresh")
    note = new_note("dest", "fresh-new-write")
    read_note("dest", note, token, "fresh-new-write-read-back")
    wait_status(
        "dest",
        lambda s: s.get("replication_state") == "ready",
        "fresh-replication-ready",
    )
    ctl("dest", "/control/sync")
    save(
        RESULTS / "verified.json",
        {
            "at": utc(),
            "app_id": APP_ID,
            "pre_fence_acknowledged_notes": len(rec["notes"]),
            "restored_notes": len(rec["notes"]),
            "post_recovery_writes": 1,
            "acknowledged_tail": tail,
            "attachment_sha256": rec["attachment"]["sha256"],
            "attachment_bytes": rec["attachment"]["size"],
            "former_session_rejected": True,
            "fresh_owner_otp_succeeded": True,
            "anonymous_attachment_http": 401,
            "source_confirmed_stopped": True,
            "db_restore_attempts": s["restore_attempts"],
        },
    )


if __name__ == "__main__":
    os.umask(0o077)
    opts = argparse.ArgumentParser(description=__doc__)
    opts.add_argument(
        "scenario",
        choices=("source", "upgrade", "warm", "crash", "fence", "destination"),
    )
    action = opts.parse_args().scenario
    globals()[action]()
