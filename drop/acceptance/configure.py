#!/usr/bin/python3
"""Prepare the synthetic hosted app and stage a specific tested executable on both dev agents."""

import argparse
import hashlib
import json
import os
import secrets
import subprocess
from pathlib import Path

from provision import PROOF, cloud
from bootstrap import APP, APP_ID, RUN_CONFIG, dev, upload
from identity import ARN
from worker import ssh

PRIVATE = cloud.PRIVATE
HOOK_TOKEN = PRIVATE / "hook-bearer"


def set_secret(name, path):
    parameter = "/drop/apps/" + APP_ID.removeprefix("d-") + "/" + name
    digest = hashlib.sha256(parameter.encode()).hexdigest()
    folder = "/var/lib/drop/dev-store/secrets/" + digest
    records = dev("secret", "list", APP)["secrets"]
    entry = next((item for item in records if item["name"] == name), None)
    if not entry:
        with path.open("rb") as value:
            subprocess.run(
                [PROOF / "host/drop-dev", "secret", "set", APP, name],
                stdin=value,
                stdout=subprocess.DEVNULL,
                check=True,
            )
        entry = next(
            item
            for item in dev("secret", "list", APP)["secrets"]
            if item["name"] == name
        )
    version = str(int(entry["version"]))
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    stored = subprocess.check_output(
        ["sudo", "sha256sum", folder + "/" + version], text=True
    ).split()[0]
    if stored != expected:
        raise ValueError("existing dev secret differs for " + name)
    for role in ("source", "dest"):
        ssh(role, "sudo install -d -m 0700 " + folder)
        ssh(
            role,
            "sudo install -m 0600 /dev/stdin " + folder + "/name",
            data=(parameter + "\n").encode(),
        )
        upload(role, path, folder + "/" + version)


def configure():
    mail = json.loads((cloud.EVIDENCE / "mail-fixture.json").read_bytes())
    if not HOOK_TOKEN.exists():
        HOOK_TOKEN.write_text(secrets.token_urlsafe(36))
    set_secret("CREW_DROP_HOOK_TOKEN", HOOK_TOKEN)
    set_secret("POCKETCONTEXT_SMTP_PASSWORD", PRIVATE / "mail-password")
    env = {
        "AWS_WORKLOAD_IDENTITY": "async",
        "AWS_REGION": cloud.REGION,
        "AWS_EC2_METADATA_DISABLED": "true",
        "CREW_DROP_APP_ID": APP_ID,
        "CREW_DROP_BUCKET": RUN_CONFIG["replica_bucket"],
        "CREW_DROP_BOOTSTRAP": "1",
        "CREW_DROP_WAIT": "30s",
        "CREW_DROP_HOOK_TIMEOUT": "30s",
        "POCKETCONTEXT_DOMAIN": RUN_CONFIG["app"]["hostname"],
        "POCKETCONTEXT_OWNER_EMAIL": "owner@crew-drop.invalid",
        "POCKETCONTEXT_SMTP_HOST": mail["public_ipv4"],
        "POCKETCONTEXT_SMTP_PORT": "2525",
        "POCKETCONTEXT_SMTP_TLS": "tls",
        "POCKETCONTEXT_SMTP_FROM": "login@crew-drop.invalid",
        "POCKETCONTEXT_SMTP_USERNAME": "fixture",
    }
    dev("app", "env", APP, *(k + "=" + v for k, v in env.items()))
    for role in ("source", "dest"):
        allowlist = "/etc/nibrunner/workload-identity/" + APP_ID
        ssh(role, "sudo install -d -m 0700 " + allowlist)
        ssh(
            role,
            "sudo install -m 0600 /dev/stdin " + allowlist + "/role-arn",
            data=ARN.encode(),
        )
    return env


def release(binary):
    data = binary.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if data[:4] != b"\x7fELF" or len(data) > 90 * 1024 * 1024:
        raise ValueError(
            "expected linux/amd64 executable within the Drop release limit"
        )
    for role in ("source", "dest"):
        upload(role, binary, "/var/lib/drop/dev-store/blobs/releases/" + digest)
    staged = Path("/var/tmp/drop-m1-release") / ("crew-" + APP_ID + "-" + digest)
    subprocess.run(["sudo", "install", "-m", "0644", binary, staged], check=True)
    try:
        # An existing dev placement may still be configured for the retired HTTP probe.
        if dev("app", "show", APP)["app"]["health_kind"] != "tcp":
            dev("app", "set", APP, "--health", "tcp")
        dev(
            "release",
            "upload",
            APP,
            staged,
            "--activate",
            "--note",
            "Synthetic Crew Drop acceptance",
        )
    finally:
        subprocess.run(["sudo", "rm", "--", staged], check=True)
    (cloud.EVIDENCE / "release.json").write_text(
        json.dumps(
            {
                "app_id": APP_ID,
                "sha256": digest,
                "bytes": len(data),
                "executable": "crew",
                "argv": ["drop-serve"],
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    os.umask(0o077)
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument("--release", type=Path)
    args = args.parse_args()
    if args.release:
        release(args.release)
    else:
        configure()
