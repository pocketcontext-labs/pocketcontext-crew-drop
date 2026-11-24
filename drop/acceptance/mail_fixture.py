#!/usr/bin/python3
"""Install the isolated OTP sink and its public test CA on the two empty acceptance runners."""

import concurrent.futures
import hashlib
import ipaddress
import json
import os
import secrets
import shlex
import subprocess
from pathlib import Path

import boto3
from provision import cloud
from bootstrap import upload
from worker import ssh

BASE = cloud.PRIVATE
CERT = BASE / "mail.crt"
KEY = BASE / "mail.key"
PASSWORD = BASE / "mail-password"
MAIL = "/run/crew-drop-mail"


def fixture_group(ec2, state, ips, dest):
    groups = ec2.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [state["vpc"]]},
            {"Name": "group-name", "Values": ["crew-drop-mail-" + state["run"]]},
        ]
    )["SecurityGroups"]
    if not groups:
        group = ec2.create_security_group(
            VpcId=state["vpc"],
            GroupName="crew-drop-mail-" + state["run"],
            Description="Disposable synthetic SMTP sink, TLS only",
            TagSpecifications=[
                {
                    "ResourceType": "security-group",
                    "Tags": cloud.taglist(state["run"], "mail"),
                }
            ],
        )["GroupId"]
        ec2.authorize_security_group_ingress(
            GroupId=group,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 2525,
                    "ToPort": 2525,
                    "IpRanges": [{"CidrIp": ip + "/32"} for ip in sorted(ips)],
                }
            ],
        )
    else:
        group = groups[0]["GroupId"]
        if {t["Key"]: t["Value"] for t in groups[0]["Tags"]}.get("Run") != state["run"]:
            raise ValueError("unrelated SMTP security group")
    eni = dest["NetworkInterfaces"][0]
    current = {g["GroupId"] for g in eni["Groups"]}
    if current != {state["sg"], group}:
        if current != {state["sg"]}:
            raise ValueError("unexpected destination management groups")
        ec2.modify_network_interface_attribute(
            NetworkInterfaceId=eni["NetworkInterfaceId"], Groups=[state["sg"], group]
        )
    return group


def trust(role):
    report = json.loads(
        ssh(
            role, "sudo cat /var/lib/nibrunner/reported.json", capture_output=True
        ).stdout
    )
    if report["instances"] or report["volumes"]:
        raise ValueError("test CA installation requires an empty runner")
    upload(role, CERT, "/tmp/crew-drop-test-ca.crt")
    script = """set -eu
cd /var/lib/nibrunner/guest
newcert=$(sha256sum /tmp/crew-drop-test-ca.crt | cut -d' ' -f1)
if test "$(cat crew-mail-fixture.sha256 2>/dev/null || true)" != "$newcert"; then
    pristine=/tmp/drop-loss-install
    original=$(jq -r '.artifacts[] | select(.name=="rootfs.ext4") | .sha256' "$pristine/manifest.json")
    printf '%s  %s/rootfs.ext4\n' "$original" "$pristine" | sha256sum --status -c -
    printf '%s\n' "$original" > rootfs.production-sha256
    cp --sparse=always "$pristine/rootfs.ext4" rootfs.ext4.test
    chmod 0644 /tmp/crew-drop-test-ca.crt
    debugfs -w -R 'write /tmp/crew-drop-test-ca.crt /etc/ssl/certs/crew-drop-test-ca.pem' rootfs.ext4.test >/dev/null 2>&1
    debugfs -R 'cat /etc/ssl/certs/crew-drop-test-ca.pem' rootfs.ext4.test 2>/dev/null | cmp - /tmp/crew-drop-test-ca.crt
    sum=$(sha256sum rootfs.ext4.test | cut -d' ' -f1)
    jq --arg sum "$sum" '(.artifacts[] | select(.name=="rootfs.ext4") | .sha256)=$sum | .version += "-crew-mail-test"' "$pristine/manifest.json" >manifest.test
    systemctl stop nibrunnerd
    mv rootfs.ext4.test rootfs.ext4
    mv manifest.test manifest.json
    jq --arg ver "$(jq -r .version manifest.json)" '.guestImage=$ver' ../versions.json > ../versions.test
    mv ../versions.test ../versions.json
    systemctl start nibrunnerd
    printf '%s\n' "$newcert" > crew-mail-fixture.sha256
fi
debugfs -R 'cat /etc/ssl/certs/crew-drop-test-ca.pem' rootfs.ext4 2>/dev/null | cmp - /tmp/crew-drop-test-ca.crt
cat rootfs.production-sha256
sha256sum rootfs.ext4 | cut -d' ' -f1
"""
    report = (
        ssh(role, "sudo sh -s", data=script.encode(), capture_output=True)
        .stdout.decode()
        .splitlines()
    )
    return {
        "role": role,
        "base_rootfs_sha256": report[-2],
        "test_rootfs_sha256": report[-1],
    }


def main():
    os.umask(0o077)
    state = json.loads(cloud.STATE.read_bytes())
    if state["app"]["slug"] != "crew-drop-" + state["run"]:
        raise ValueError("unexpected acceptance app")
    workers = {r: cloud.verify_role(r, state) for r in ("source", "dest")}
    ips = {
        r: str(ipaddress.IPv4Address(w["PublicIpAddress"])) for r, w in workers.items()
    }
    if not CERT.exists():
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:3072",
                "-nodes",
                "-days",
                "2",
                "-subj",
                "/CN=Crew Drop OTP fixture",
                "-addext",
                "subjectAltName=IP:" + ips["dest"],
                "-addext",
                "extendedKeyUsage=serverAuth",
                "-keyout",
                KEY,
                "-out",
                CERT,
            ],
            check=True,
            capture_output=True,
        )
    if not PASSWORD.exists():
        PASSWORD.write_text(secrets.token_urlsafe(32))
    ec2 = boto3.Session(profile_name="drop-m1", region_name=cloud.REGION).client("ec2")
    group = fixture_group(ec2, state, set(ips.values()), workers["dest"])
    ssh("dest", "sudo install -d -m 0700 " + MAIL)
    for path, name in [
        (CERT, "mail.crt"),
        (KEY, "mail.key"),
        (PASSWORD, "password"),
        (Path(__file__).with_name("smtp_sink.py"), "server.py"),
    ]:
        upload("dest", path, MAIL + "/" + name)
    ssh(
        "dest",
        "systemctl is-active --quiet crew-drop-test-mail || sudo systemd-run --quiet --collect"
        " --unit=crew-drop-test-mail --property=Restart=on-failure /usr/bin/python3 "
        + MAIL
        + "/server.py",
    )
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        images = list(pool.map(trust, ("source", "dest")))
    for role in ("source", "dest"):
        # This proves both VPC paths, including destination-to-its-own-public-IPv4.
        cmd = (
            "echo QUIT | timeout 7 openssl s_client -quiet -ignore_unexpected_eof -verify_return_error -verify_ip "
            + ips["dest"]
            + " -CAfile /tmp/crew-drop-test-ca.crt -connect "
            + ips["dest"]
            + ":2525 2>/dev/null"
        )
        if (
            "220 fixture"
            not in ssh(
                role, "sudo sh -c " + shlex.quote(cmd), capture_output=True
            ).stdout.decode()
        ):
            raise RuntimeError("mail fixture TLS handshake did not complete")
    (cloud.EVIDENCE / "mail-fixture.json").write_text(
        json.dumps(
            {
                "sink_worker": "dest",
                "recipient": "owner@crew-drop.invalid",
                "mail_security_group": group,
                "public_ipv4": ips["dest"],
                "port": 2525,
                "images": images,
                "ca_certificate_sha256": hashlib.sha256(CERT.read_bytes()).hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
