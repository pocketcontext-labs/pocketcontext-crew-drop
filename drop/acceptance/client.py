#!/usr/bin/python3
"""External test client. Secrets go only to the corresponding private endpoint and the local vault."""

import base64
import hashlib
import http.client
import ipaddress
import json
import os
import re
import shlex
import subprocess
import threading
import time
from datetime import UTC, datetime

from provision import cloud
from bootstrap import APP_ID, RUN_CONFIG
from worker import PORT, driver, ssh

PRIVATE = cloud.PRIVATE
RESULTS = cloud.EVIDENCE / "experiment"
HOST = RUN_CONFIG["app"]["hostname"]
LOCK = threading.Lock()


def utc():
    return datetime.now(UTC).isoformat(timespec="microseconds")


def journal(event, **fields):
    RESULTS.mkdir(parents=True, exist_ok=True)
    with LOCK, (RESULTS / "observations.jsonl").open("a") as out:
        print(
            json.dumps({"at": utc(), "event": event, **fields}, separators=(",", ":")),
            file=out,
        )
        out.flush()
        os.fsync(out.fileno())


def save(path, obj):
    temp = path.with_suffix(".new")
    temp.write_text(json.dumps(obj, indent=2) + "\n")
    with temp.open("rb") as f:
        os.fsync(f.fileno())
    temp.replace(path)


def request(role, method, path, *, body=None, token=None, timeout=35):
    headers = {"Host": HOST, "Cache-Control": "no-cache"}
    if token:
        headers["Authorization"] = token
    if body is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(body).encode()
    start = time.monotonic_ns()
    began = utc()
    conn = http.client.HTTPConnection("127.0.0.1", PORT[role], timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers)
        response = conn.getresponse()
        content = response.read(4 * 1024 * 1024)
        status = response.status
        hdr = {k.lower(): v for k, v in response.getheaders()}
    except (OSError, http.client.HTTPException) as exc:
        status, content, hdr = 0, b"", {"error": type(exc).__name__}
    finally:
        conn.close()
    metrics = {
        "at_start": began,
        "at_end": utc(),
        "elapsed_ms": round((time.monotonic_ns() - start) / 1e6, 3),
        "http": status,
    }
    return metrics, hdr, content


def expect(role, method, path, *, event, code=200, token=None, body=None, timeout=35):
    metrics, hdr, content = request(
        role, method, path, body=body, token=token, timeout=timeout
    )
    allowed = [code] if isinstance(code, int) else list(code)
    journal(event, role=role, **metrics)
    if metrics["http"] not in allowed:
        raise RuntimeError(
            f"{role} {event}: {method} {path.split('?')[0]} HTTP {metrics['http']}, expected {allowed}"
        )
    return metrics, hdr, content


def public_until(role, path="/up", limit=145):
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        metrics, _, _ = request(role, "GET", path, timeout=8)
        if metrics["http"] == 200:
            journal("public-ready", role=role, path=path, **metrics)
            return
        time.sleep(0.24)
    raise RuntimeError(f"{role} {path} never became available")


def issue_owner(role):
    ssh(
        "dest",
        "sudo find /run/crew-drop-mail -maxdepth 1 -type f -name 'code-*' -delete",
    )
    _, _, raw = expect(
        role,
        "POST",
        "/api/collections/users/request-otp",
        event="owner-request-otp",
        body={"email": "owner@crew-drop.invalid"},
    )
    otp_id = json.loads(raw)["otpId"]
    deadline = time.monotonic() + 18
    code = None
    while time.monotonic() < deadline:
        result = ssh(
            "dest",
            'sudo sh -c \'for f in /run/crew-drop-mail/code-*; do test ! -f "$f" || '
            '{ cat "$f"; rm -f -- "$f"; break; }; done\'',
            capture_output=True,
        )
        code = result.stdout.decode().strip()
        if re.fullmatch(r"[0-9]{6}", code):
            break
        time.sleep(0.3)
    else:
        raise RuntimeError(
            "the local-only TLS mailbox did not receive a synthetic owner code"
        )
    _, _, raw = expect(
        role,
        "POST",
        "/api/collections/users/auth-with-otp-code",
        event="owner-redeem",
        body={"otpId": otp_id, "code": code},
    )
    out = json.loads(raw)
    if out["record"]["email"] != "owner@crew-drop.invalid":
        raise ValueError("unexpected owner")
    (PRIVATE / (role + "-owner-token")).write_text(out["token"])
    return out["token"]


def owner(role):
    return (PRIVATE / (role + "-owner-token")).read_text().strip()


def write(role, key, action, data, token):
    envelope = {
        "action": action,
        "reported_agent": "Acceptance",
        "reason": "Synthetic recovery test",
        "idempotency_key": "crew-" + RUN_CONFIG["run"] + "-" + key,
        "data": data,
    }
    _, _, raw = expect(
        role, "POST", "/api/crew/write", event=key, body=envelope, token=token
    )
    return json.loads(raw)


def read_note(role, note, token, event):
    _, _, raw = expect(
        role,
        "POST",
        "/api/crew/read",
        event=event,
        body={"collection": "notes", "id": note["id"]},
        token=token,
    )
    items = json.loads(raw)["items"]
    if (
        len(items) != 1
        or items[0]["title"] != note["title"]
        or items[0]["body"] != note["body"]
    ):
        raise ValueError("acknowledged note differs on " + role)


def attach_data():
    return (
        b"Synthetic Crew Drop attachment.\n"
        + hashlib.sha256(RUN_CONFIG["run"].encode()).digest() * 8192
    )


def check_attachment(role, item, token, step):
    path = "/api/crew/attachment?id=" + item["id"]
    _, hdr, raw = expect(role, "GET", path, event=step + "-owner-file", token=token)
    if (
        hashlib.sha256(raw).hexdigest() != item["sha256"]
        or raw != attach_data()
        or not hdr.get("content-disposition", "").startswith("attachment")
        or hdr.get("x-content-type-options") != "nosniff"
    ):
        raise ValueError("attachment bytes or security headers differ")
    expect(role, "GET", path, event=step + "-anonymous-file", code=401)
    expect(
        role,
        "POST",
        "/api/crew/read",
        event=step + "-anonymous-note",
        code=(401, 403),
        body={"collection": "notes"},
    )
    journal(
        step + "-file-verified",
        role=role,
        sha256=item["sha256"],
        bytes=len(raw),
        disposition="attachment",
        nosniff=True,
    )


def populate():
    public_until("source")
    token = issue_owner("source")
    bucket = write(
        "source",
        "create-bucket",
        "bucket.create",
        {"name": "Synthetic acceptance"},
        token,
    )
    note = write(
        "source",
        "seed-note",
        "note.create",
        {
            "bucket": bucket["id"],
            "title": "Source pre-restart",
            "body": "Accepted synthetic source marker " + RUN_CONFIG["run"],
        },
        token,
    )
    content = attach_data()
    out = write(
        "source",
        "add-protected-file",
        "attachment.add",
        {
            "note": note["id"],
            "files": [
                {
                    "name": "synthetic.bin",
                    "content_base64": base64.b64encode(content).decode(),
                }
            ],
        },
        token,
    )
    file = out["attachments"][0]
    if file["sha256"] != hashlib.sha256(content).hexdigest() or file["size"] != len(
        content
    ):
        raise ValueError("upload acknowledgement differs")
    records = {
        "bucket_id": bucket["id"],
        "notes": [{k: note[k] for k in ("id", "title", "body")}],
        "attachment": {k: file[k] for k in ("id", "sha256", "size")},
    }
    save(PRIVATE / "records.json", records)
    read_note("source", note, token, "initial-note-read")
    check_attachment("source", file, token, "initial")


def new_note(role, marker):
    prior = json.loads((PRIVATE / "records.json").read_bytes())
    note = write(
        role,
        marker,
        "note.create",
        {
            "bucket": prior["bucket_id"],
            "title": marker,
            "body": "Accepted synthetic note in "
            + marker
            + " for "
            + RUN_CONFIG["run"],
        },
        owner(role),
    )
    if not any(n["id"] == note["id"] for n in prior["notes"]):
        prior["notes"].append({k: note[k] for k in ("id", "title", "body")})
        save(PRIVATE / "records.json", prior)
    return note


def guest_ip(role):
    report = json.loads(
        ssh(
            role, "sudo cat /var/lib/nibrunner/reported.json", capture_output=True
        ).stdout
    )
    guest = next((x for x in report["instances"] if x["appId"] == APP_ID), {})
    return (
        str(ipaddress.IPv4Address(guest["guestIpv4"]))
        if guest.get("guestIpv4")
        else None
    )


def hook(role, ip, method, path, *, authenticated=True, timeout=35, required=True):
    script = """import json, sys, urllib.error, urllib.request
method, url, bearer, timeout = json.load(sys.stdin)
headers = {'Authorization':'Bearer ' + bearer} if bearer else {}
req = urllib.request.Request(url, headers=headers, method=method)
try:
    response = urllib.request.urlopen(req,timeout=timeout)
except urllib.error.HTTPError as error:
    response = error
try:
    result = json.load(response)
except ValueError:
    result = {}
print(json.dumps([response.status,result]))
"""
    bearer = (PRIVATE / "hook-bearer").read_text() if authenticated else ""
    proc = subprocess.run(
        [*driver(role), "python3 -c " + shlex.quote(script)],
        input=json.dumps(
            [
                method,
                "http://" + str(ipaddress.IPv4Address(ip)) + ":8081" + path,
                bearer,
                timeout,
            ]
        ).encode(),
        capture_output=True,
    )
    if proc.returncode:
        if required:
            raise RuntimeError("private hook " + path + " unreachable on " + role)
        return 0, {}
    return json.loads(proc.stdout)


def status(role):
    ip = guest_ip(role)
    if not ip:
        return {}
    code, result = hook(role, ip, "GET", "/status", required=False)
    return result if code == 200 else {}


def safe_status(data):
    keys = (
        "boot_mode",
        "data_state",
        "replication_state",
        "hook_waiters",
        "data_waiters",
        "restore_attempts",
        "recovery_attempts",
        "phases",
    )
    return {k: data[k] for k in keys if k in data}
