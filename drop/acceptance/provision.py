#!/usr/bin/python3
"""Create or resume an isolated pair of dev runners with a distinct Crew app and AWS principal."""

import ipaddress
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROOF = Path(os.environ.get("DROP_M1_PROOF", ROOT.parent / "drop-m1-proof"))
private = Path(os.environ["DROP_M1_LOSS_STATE_DIR"]).expanduser().resolve()
evidence = Path(os.environ["DROP_M1_LOSS_EVIDENCE_DIR"]).expanduser().resolve()
if private == (Path.home() / ".local/state/drop-m1-abrupt").resolve():
    raise ValueError("Crew acceptance needs a separate run directory")
sys.path.insert(0, str(PROOF / "abrupt"))
import cloud


def main():
    os.umask(0o077)
    private.mkdir(parents=True, exist_ok=True)
    evidence.mkdir(parents=True, exist_ok=True)
    if cloud.aws("sts", "get-caller-identity")["Account"] != cloud.ACCOUNT:
        raise ValueError("wrong development AWS account")
    if not cloud.STATE.exists():
        controller = cloud.aws(
            "ec2",
            "describe-instances",
            "--filters",
            "Name=private-ip-address,Values=172.31.23.28",
        )["Reservations"][0]["Instances"][0]
        image = cloud.aws(
            "ec2", "describe-images", "--image-ids", controller["ImageId"]
        )["Images"][0]
        if not image["Public"] or image["OwnerId"] != "099720109477":
            raise ValueError("expected a public Canonical image")
        run = uuid.uuid4().hex[:12]
        cli = PROOF / "host/drop-dev"
        app = json.loads(
            subprocess.check_output(
                [
                    cli,
                    "app",
                    "create",
                    "Crew Drop acceptance",
                    "--slug",
                    "crew-drop-" + run,
                    "--mode",
                    "running",
                    "--port",
                    "8080",
                    "--memory",
                    "1024",
                    "--disk",
                    "2048",
                    "--executable",
                    "crew",
                    "--args-json",
                    '["drop-serve"]',
                    "--health",
                    "http",
                    "--health-path",
                    "/ready",
                ]
            )
        )["app"]
        cloud.save(
            {
                "run": run,
                "region": cloud.REGION,
                "controller": controller["InstanceId"],
                "controller_ip": str(
                    ipaddress.ip_address(controller["PrivateIpAddress"])
                ),
                "ami": controller["ImageId"],
                "subnet": controller["SubnetId"],
                "vpc": controller["VpcId"],
                "workers": {},
                "app": {k: app[k] for k in ("slug", "hostname", "nib_app_id")},
                "issuer_bucket": f"crew-drop-issuer-{cloud.ACCOUNT}-{run}",
                "replica_bucket": f"crew-drop-data-{cloud.ACCOUNT}-{run}",
            }
        )
    state = json.loads(cloud.STATE.read_bytes())
    if state["app"]["slug"] != "crew-drop-" + state["run"]:
        raise ValueError("unexpected app in operator state")
    cloud.launch()
    state = json.loads(cloud.STATE.read_bytes())
    for role, worker in state["workers"].items():
        worker["name"] = f"crew-{role}-{state['run']}"
    cloud.save(state)


if __name__ == "__main__":
    main()
