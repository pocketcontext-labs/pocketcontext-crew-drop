#!/usr/bin/python3
"""Install the isolated OTP sink and host relay credentials on the two empty acceptance runners."""

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


def configure_relay(role, host):
    report = json.loads(ssh(role, "sudo cat /var/lib/nibrunner/reported.json", capture_output=True).stdout)
    if report["instances"] or report["volumes"]:
        raise ValueError("the SMTP fixture requires an empty acceptance runner")
    folder = "/etc/nibrunner/crew-mail-test"
    ssh(role, "sudo install -d -m 0700 " + folder)
    upload(role, CERT, folder + "/ca.pem")
    credential = "\n".join((
        "host = " + json.dumps(host),
        "port = 2525",
        'username = "fixture"',
        "password = " + json.dumps(PASSWORD.read_text().strip()),
        'sender = "login@crew-drop.invalid"',
        "ca_file = " + json.dumps(folder + "/ca.pem"),
    ))
    ssh(role, "sudo install -m 0600 /dev/stdin " + folder + "/smtp.toml", data=credential.encode())
    dropin = "/etc/systemd/system/nibrunnerd.service.d"
    ssh(role, "sudo install -d " + dropin)
    ssh(role, "sudo install -m 0644 /dev/stdin " + dropin + "/crew-mail-test.conf",
        data=("[Service]\nLoadCredential=sign-in-email.toml:" + folder + "/smtp.toml\n").encode())
    ssh(role, "sudo systemctl daemon-reload && sudo systemctl restart nibrunnerd")


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
                "-addext",
                "basicConstraints=critical,CA:FALSE",
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
        list(pool.map(lambda role: configure_relay(role, ips["dest"]), ("source", "dest")))
    for role in ("source", "dest"):
        # This proves both VPC paths, including destination-to-its-own-public-IPv4.
        cmd = (
            "echo QUIT | timeout 7 openssl s_client -quiet -ignore_unexpected_eof -verify_return_error -verify_ip "
            + ips["dest"]
            + " -CAfile /etc/nibrunner/crew-mail-test/ca.pem -connect "
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
                "ca_certificate_sha256": hashlib.sha256(CERT.read_bytes()).hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
