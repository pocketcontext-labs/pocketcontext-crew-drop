#!/usr/bin/env python3
"""Exercise two real Crew guests on an isolated Linux/KVM development runner."""

import argparse
import contextlib
import http.client
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile

from managed_failure import check_lost_data_reply

HERE = Path(__file__).resolve().parent
DROP = HERE.parents[2] / "pocketcontext-drop"
sys.path.insert(0, str(DROP / "tests"))
from drop_e2e import (
    APP_DOMAIN,
    HOSTED_DOMAIN,
    HOST_ID,
    NODE,
    Node,
    Server,
    isolated_env,
    listening,
    read_json,
    wait_for,
)

OWNERS = ("owner@crew-drop.invalid", "second@crew-drop.invalid")
FROM = "login@crew-drop.invalid"
CONTROL_FROM = "drop@crew-drop.invalid"
PROXY = 443


def request(port, host, method, path, data=None, token=None):
    headers = {"Host": host}
    if token:
        headers["Authorization"] = token
    if data is not None and not isinstance(data, bytes):
        data = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    if port == PROXY:
        conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=60)
    else:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
    try:
        conn.request(method, path, data, headers)
        response = conn.getresponse()
        raw = response.read()
        return response.status, json.loads(raw) if response.getheader(
            "Content-Type", ""
        ).startswith("application/json") else {}
    finally:
        conn.close()


def api(port, host, method, path, data=None, token=None):
    status, body = request(port, host, method, path, data, token)
    if status // 100 != 2:
        raise RuntimeError(
            f"{method} request on {host} returned {status}: {body.get('message', '')}"
        )
    return body


def code_for(mail, owner):
    path = wait_for(
        "mail to the bound recipient",
        lambda: next(mail.glob("code-" + owner + "-*"), None),
        timeout=25,
    )
    code = path.read_text().strip()
    path.unlink()
    return code


def login(port, host, owner, mail):
    otp = api(
        port, host, "POST", "/api/collections/users/request-otp", {"email": owner}
    )
    form = {"otpId": otp["otpId"], "code": code_for(mail, owner)}
    path = "/api/collections/users/auth-with-otp-code"
    auth = api(port, host, "POST", path, form)
    assert auth["record"]["email"] == owner and auth["token"]
    assert request(port, host, "POST", path, form)[0] == 400
    return auth["token"]


def start(proc_args, log, env):
    with log.open("ab") as stream:
        return subprocess.Popen(
            proc_args, env=env, stdout=stream, stderr=subprocess.STDOUT
        )


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def make_mailbox(root, stack, env):
    mail = root / "mail"
    mail.mkdir()
    password = secrets.token_urlsafe(32)
    (mail / "password").write_text(password)
    (mail / "drop-password").write_text(secrets.token_urlsafe(32))
    cert = mail / "mail.crt"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            f"subjectAltName=IP:127.0.0.1,DNS:*.{APP_DOMAIN}",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "extendedKeyUsage=serverAuth",
            "-keyout",
            mail / "mail.key",
            "-out",
            cert,
        ],
        check=True,
        capture_output=True,
    )
    proc = start(
        [
            sys.executable,
            HERE / "smtp_sink.py",
            "--listen",
            "127.0.0.1",
            "--base",
            mail,
            "--owner",
            OWNERS[0],
            "--owner",
            OWNERS[1],
            "--drop-sender",
            CONTROL_FROM,
        ],
        root / "mail.log",
        env,
    )
    stack.callback(stop, proc)
    return mail, password


def configure_runner(root, args, node, mail, password):
    creds = root / "credentials"
    creds.mkdir()
    (creds / "sign-in-email.toml").write_text(
        f'host = "127.0.0.1"\nport = 2525\nusername = "fixture"\npassword = "{password}"\n'
        f'sender = "{FROM}"\nca_file = "{mail}/mail.crt"\n'
    )
    versions = node.nib / "versions.json"
    shutil.copyfile(args.versions, versions)
    config = root / "config.toml"
    config.write_text(f"""
max_apps = 4
max_concurrent_vm_starts = 2
[cron]
max_jobs_per_app = 10
time_zone = "UTC"
[network]
denied_egress_addresses_v4 = []
denied_egress_addresses_v6 = []
[logs]
keep_mib_per_app = 8
[metrics]
listen_address = "127.0.0.1"
port = 9100
[paths]
state_dir = "{node.nib}"
runtime_dir = "{root}/run"
snapshot_dir = "{root}/snap"
guest_image_dir = "{args.guest}"
desired_state_file = "{node.desired}"
versions_file = "{versions}"
[artifacts]
store_url = "{node.artifacts}"
[volumes]
backend = "local-file"
storage_prefix = "volumes"
[exports]
store_url = "{root}/export-store"
staging_dir = "{root}/exports"
[proxy.http]
listen_address = "127.0.0.1"
port = {PROXY}
[proxy.http.tls]
certificate = "{mail}/mail.crt"
key = "{mail}/mail.key"
""")
    return {
        "PATH": os.environ["PATH"],
        "HOME": str(root / "home"),
        "NIBRUNNER_CONFIG": str(config),
        "NIBRUNNER_LOG": "info,nibrunnerd::adapters::vm::sign_in_email=debug",
        "CREDENTIALS_DIRECTORY": str(creds),
    }


def run(root, stack, args):
    home = root / "home"
    home.mkdir()
    env = isolated_env(home)
    host_env = env.copy()
    mail, password = make_mailbox(root, stack, env)
    os.environ["SSL_CERT_FILE"] = str(mail / "mail.crt")
    env.update(
        DROP_OWNER_EMAIL=OWNERS[0],
        DROP_SMTP_FROM=CONTROL_FROM,
        DROP_APP_DOMAIN=APP_DOMAIN,
        DROP_SMTP_HOST="127.0.0.1",
        DROP_SMTP_PORT="2525",
        DROP_SMTP_USERNAME="fixture-drop",
        DROP_SMTP_PASSWORD=(mail / "drop-password").read_text(),
        SSL_CERT_FILE=str(mail / "mail.crt"),
    )
    directory = root / "server"
    directory.mkdir()
    server = Server(args.drop, directory, env)
    server.start(init=True)
    stack.callback(server.stop)
    admin = login(server.port, HOSTED_DOMAIN, OWNERS[0], mail)

    def op(token, operation, **fields):
        return api(
            server.port,
            HOSTED_DOMAIN,
            "POST",
            "/api/drop/op",
            {"op": operation, "input": fields},
            token,
        )["result"]

    op(admin, "invite.create", email=OWNERS[1])
    member = login(server.port, HOSTED_DOMAIN, OWNERS[1], mail)
    apps = []
    for index, token in enumerate((admin, member)):
        slug = f"crew-{index}"
        app = op(
            token,
            "app.create",
            name=slug,
            slug=slug,
            kind="crew",
            desired_state="on-request" if index == 1 else "running",
            idle_timeout_ms=60000,
            executable_name="crew",
            args=["drop-serve"],
            memory_mib=512,
        )["app"]
        assert app["crew_owner_email"] == OWNERS[index]
        apps.append(app)
        op(
            token,
            "app.set",
            app=slug,
            env={
                "set": {
                    "CREW_DROP_APP_ID": app["nib_app_id"],
                    "POCKETCONTEXT_DOMAIN": app["hostname"],
                    "CREW_DROP_FILE_REPLICA": "/app/data/fixture-replica",
                    "CREW_DROP_BOOTSTRAP": "1",
                }
            },
        )
        op(
            token,
            "secret.set",
            app=slug,
            name="CREW_DROP_HOOK_TOKEN",
            value=secrets.token_urlsafe(32),
        )
        upload = op(token, "release.upload_link", app=slug, activate=True)
        api(server.port, HOSTED_DOMAIN, "PUT", upload["path"], args.crew.read_bytes())
    node = Node(
        args.drop,
        root / "node",
        server,
        op(admin, "node.create", name=NODE, host_id=HOST_ID)["password"],
        host_env,
    )
    node.write_empty()
    runner_env = configure_runner(root, args, node, mail, password)

    def launch():
        proc = start([args.runner], root / "runner.log", runner_env)
        stack.callback(stop, proc)
        return proc

    runner = launch()
    wait_for("runner registration", lambda: read_json(node.reported))
    node.start()
    stack.callback(node.stop)
    for app in apps:
        op(admin, "placement.set", app=app["slug"], node=NODE)
    wait_for(
        "two rendered guests",
        lambda: len((read_json(node.desired) or {}).get("instances", [])) == 2,
    )
    for instance in read_json(node.desired)["instances"]:
        guest_env = instance["config"]["command"]["environment"]
        assert instance["signInEmail"]["recipient"] == guest_env["DROP_OWNER_EMAIL"]
        assert not any("SMTP" in key for key in guest_env)

    def ready(app):
        try:
            return (
                request(PROXY, app["hostname"], "GET", "/api/bundle/auth-mode")[1].get(
                    "codeOnly"
                )
                is True
            )
        except (OSError, ValueError):
            return False

    wait_for(
        "two Crew code-entry screens", lambda: all(ready(a) for a in apps), timeout=180
    )
    # The node only has its last applied state from here until reconnection.
    server.stop()
    stop(runner)
    runner = launch()
    # Its surviving HTTP worker can serve Crew while the new daemon is still
    # verifying the guest image. Metrics opens after reattaching guest channels.
    wait_for("new daemon to finish readoption", lambda: listening(9100), timeout=120)
    wait_for(
        "both guests after readoption", lambda: all(ready(a) for a in apps), timeout=90
    )
    submissions = mail / "submissions"
    before = submissions.stat().st_size
    for index, app in enumerate(apps):
        api(
            PROXY,
            app["hostname"],
            "POST",
            "/api/collections/users/request-otp",
            {"email": OWNERS[1 - index]},
        )
    assert not list(mail.glob("code-*"))
    subprocess.run(
        [
            args.node,
            HERE / "managed_browser.mjs",
            str(mail),
            *(a["hostname"] for a in apps),
        ],
        check=True,
    )
    assert not list(mail.glob("code-*"))
    assert submissions.stat().st_size == before + 4  # two SMTP transactions
    print(
        "PASS: two verified owners, two VMs, code-only browser sign-in during Drop outage after runner restart",
        flush=True,
    )
    server.start(init=False)
    check_lost_data_reply(
        mail,
        direct=(server.port, HOSTED_DOMAIN, OWNERS[1]),
        managed=(PROXY, apps[0]["hostname"], OWNERS[0]),
        request=request,
        code_for=code_for,
    )
    op(admin, "app.set", app=apps[0]["slug"], sign_in_email_enabled=False)
    wait_for(
        "applied revocation",
        lambda: (
            any(
                i["appId"] == apps[0]["nib_app_id"] and "signInEmail" not in i
                for i in read_json(node.desired)["instances"]
            )
            and (read_json(node.reported) or {}).get("acceptedRevision")
            == node.revision()
        ),
        timeout=90,
    )
    app_id = apps[0]["nib_app_id"]

    def replacement_running():
        desired = next(
            i for i in read_json(node.desired)["instances"] if i["appId"] == app_id
        )
        return any(
            i["appId"] == app_id
            and i["deploymentId"] == desired["deploymentId"]
            and i["state"] == "running"
            for i in read_json(node.reported)["instances"]
        ) and ready(apps[0])

    wait_for("replaced Crew backend", replacement_running, timeout=120)
    before = submissions.stat().st_size
    api(
        PROXY,
        apps[0]["hostname"],
        "POST",
        "/api/collections/users/request-otp",
        {"email": OWNERS[0]},
    )

    def disabled_request():
        with (root / "runner.log").open() as log:
            for line in log:
                try:
                    fields = json.loads(line).get("fields", {})
                except ValueError:
                    continue
                if (
                    fields.get("app_id") == app_id
                    and fields.get("outcome") == "disabled"
                ):
                    return True
        return False

    wait_for("runner to refuse the revoked guest", disabled_request, timeout=25)
    assert not list(mail.glob("code-*"))
    assert submissions.stat().st_size == before
    print(
        "PASS: restored control plane, applied disable, zero additional messages",
        flush=True,
    )
    sleepy = apps[1]

    def is_sleeping():
        return any(
            i["appId"] == sleepy["nib_app_id"] and i["state"] == "idle"
            for i in read_json(node.reported)["instances"]
        )

    wait_for("the on-request guest to sleep", is_sleeping, timeout=200)
    # A fresh request wakes the snapshot. The same owner can redeem the new
    # code exactly once with Drop offline, including after the guest clock resumes.
    server.stop()
    wait_for("wake Crew through its public route", lambda: ready(sleepy), timeout=150)
    login(PROXY, sleepy["hostname"], OWNERS[1], mail)
    server.start(init=False)
    print(
        "PASS: request-triggered wake, bound email delivery and one-time redemption",
        flush=True,
    )
    for app in apps:
        op(admin, "app.stop", app=app["slug"])
    wait_for(
        "both VMs stopped",
        lambda: all(
            i["state"] == "stopped" for i in read_json(node.reported)["instances"]
        ),
        timeout=90,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("drop", "crew", "runner", "node", "guest", "versions"):
        parser.add_argument(
            "--" + name, type=lambda v: Path(v).resolve(), required=True
        )
    args = parser.parse_args()
    # The runner owns firewall state inside its network namespace. Private PID/mount
    # namespaces contain its workers and its Firecracker jails on both success and failure.
    if os.geteuid() or os.getpid() != 1:
        raise SystemExit(
            "launch with sudo unshare --net --mount --pid --fork --mount-proc --propagation private"
        )
    # Firecracker outlives a restarted daemon. PID 1 must reap its former
    # children when they later exit, just as the host's init does.
    child = os.fork()
    if child:
        while True:
            pid, status = os.wait()
            if pid == child:
                raise SystemExit(os.waitstatus_to_exitcode(status))

    interfaces = json.loads(subprocess.check_output(["ip", "-j", "link"]))
    if [i["ifname"] for i in interfaces] != ["lo"]:
        raise SystemExit("expected a fresh network namespace with only loopback")
    subprocess.run(["ip", "link", "set", "lo", "up"], check=True)
    os.umask(0o077)
    with tempfile.TemporaryDirectory(prefix="cs-", dir="/tmp") as directory:
        root = Path(directory)
        try:
            with contextlib.ExitStack() as stack:
                run(root, stack, args)
        except Exception:
            # Print only runner infrastructure diagnostics. Guest and SMTP logs stay private.
            log = root / "runner.log"
            if log.exists():
                for line in log.read_text().splitlines():
                    try:
                        fields = json.loads(line).get("fields", {})
                    except ValueError:
                        continue
                    message = fields.get("message", "")
                    if "sign-in email" in message or "guest channels" in message:
                        print(message, fields.get("outcome", ""), flush=True)
            counter = root / "mail" / "submissions"
            if counter.exists():
                print(
                    "SMTP transactions including Drop verification:",
                    len(counter.read_bytes().splitlines()),
                    flush=True,
                )
            report = next(root.rglob("reported.json"), None)
            if report and (state := read_json(report)):
                print(
                    "Guest states:",
                    [
                        (i["appId"], i["state"], i.get("message"))
                        for i in state["instances"]
                    ],
                    flush=True,
                )
            raise


if __name__ == "__main__":
    main()
