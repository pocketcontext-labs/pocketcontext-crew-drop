#!/usr/bin/python3
"""Archive the synthetic app, remove only this run's AWS resources, and verify their absence."""

import argparse
import json
import os
import subprocess

from botocore.exceptions import ClientError

from aws_checks import OPERATOR, ROLE
from bootstrap import APP, APP_ID, RUN_CONFIG, dev
from client import PRIVATE, RESULTS, save, utc
from identity import DOMAIN
from provision import cloud


def absent(action, codes):
    try:
        action()
    except ClientError as err:
        if err.response["Error"]["Code"] in codes:
            return True
        raise
    return False


def remove_bucket(s3, name, allowed):
    if absent(
        lambda: s3.head_bucket(Bucket=name, ExpectedBucketOwner=cloud.ACCOUNT),
        {"404", "NoSuchBucket"},
    ):
        return {"objects": 0, "bytes": 0}
    tags = {a["Key"]: a["Value"] for a in s3.get_bucket_tagging(Bucket=name)["TagSet"]}
    if tags.get("Purpose") != "DisposableProof":
        raise ValueError("unexpected bucket owner tags")
    if s3.get_bucket_versioning(Bucket=name).get("Status"):
        raise ValueError("this fixture expects an unversioned bucket")
    keys = []
    bytes_deleted = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=name, ExpectedBucketOwner=cloud.ACCOUNT
    ):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
        bytes_deleted += sum(obj["Size"] for obj in page.get("Contents", []))
    if not all(allowed(key) for key in keys):
        raise ValueError("unexpected keys in test bucket")
    for start in range(0, len(keys), 1000):
        deleted = s3.delete_objects(
            Bucket=name,
            ExpectedBucketOwner=cloud.ACCOUNT,
            Delete={"Objects": [{"Key": key} for key in keys[start : start + 1000]]},
        )
        if deleted.get("Errors"):
            raise ValueError("one or more fixture objects were not deleted")
    s3.delete_bucket(Bucket=name, ExpectedBucketOwner=cloud.ACCOUNT)
    if not absent(
        lambda: s3.head_bucket(Bucket=name, ExpectedBucketOwner=cloud.ACCOUNT),
        {"404", "NoSuchBucket"},
    ):
        raise ValueError("temporary bucket still exists")
    return {"objects": len(keys), "bytes": bytes_deleted}


def main():
    os.umask(0o077)
    if (
        APP != "crew-drop-" + RUN_CONFIG["run"]
        or OPERATOR.client("sts").get_caller_identity()["Account"] != cloud.ACCOUNT
    ):
        raise ValueError("unexpected dev run/account")
    opts = argparse.ArgumentParser(description=__doc__)
    opts.add_argument(
        "--archived-failed-pilot",
        action="store_true",
        help="remove a separately archived forensic pilot after evidence export",
    )
    flagged = opts.parse_args().archived_failed_pilot
    if flagged:
        original = json.loads((RESULTS / "archived-failed-pilot.json").read_bytes())
        forensic = json.loads(
            (RESULTS / "acknowledged-data-forensics.json").read_bytes()
        )
        if (
            original["app_id"] != APP_ID
            or not original["app_archived"]
            or not original["source_disk_sha256"]
            or not forensic["s3_txid_2_verified_original_note_and_file_metadata"]
            or not (RESULTS / "encrypted-forensic-archive.json").exists()
        ):
            raise ValueError(
                "the failed pilot must have a complete archived receipt and verified encrypted backup"
            )
    else:
        verified = json.loads((RESULTS / "verified.json").read_bytes())
        if (
            verified["app_id"] != APP_ID
            or not verified["source_confirmed_stopped"]
            or not verified["fresh_owner_otp_succeeded"]
        ):
            raise ValueError(
                "fenced authenticated recovery must be recorded before fixture cleanup"
            )
    path = RESULTS / "cleanup.json"
    audit = json.loads(path.read_bytes()) if path.exists() else {}
    ec2, iam, s3 = [OPERATOR.client(s) for s in ("ec2", "iam", "s3")]
    if "workers" not in audit:
        workers = {r: cloud.verify_role(r, RUN_CONFIG) for r in ("source", "dest")}
        if workers["source"]["State"]["Name"] != "stopped":
            raise ValueError("source has lost its EC2 fence")
        audit["workers"] = {
            r: {
                "id": w["InstanceId"],
                "instance_type": w["InstanceType"],
                "volumes": [v["Ebs"]["VolumeId"] for v in w["BlockDeviceMappings"]],
                "enis": [e["NetworkInterfaceId"] for e in w["NetworkInterfaces"]],
            }
            for r, w in workers.items()
        }
        for w in audit["workers"].values():
            w["block_storage"] = [
                {
                    "id": v["VolumeId"],
                    "gib": v["Size"],
                    "type": v["VolumeType"],
                    "encrypted": v["Encrypted"],
                }
                for v in ec2.describe_volumes(VolumeIds=w["volumes"])["Volumes"]
            ]
        audit["region"] = cloud.REGION
        save(path, audit)
    if flagged:
        audit["app_archived"] = True
    if not audit.get("app_archived"):
        for secret in dev("secret", "list", APP)["secrets"]:
            if secret["name"] not in (
                "CREW_DROP_HOOK_TOKEN",
                "POCKETCONTEXT_SMTP_PASSWORD",
            ):
                raise ValueError("unexpected secret on the synthetic app")
            dev("secret", "delete", APP, secret["name"])
        dev("app", "archive", APP)
        audit["app_archived"] = True
        for role in ("source", "dest"):
            dev("node", "set-state", RUN_CONFIG["workers"][role]["name"], "disabled")
            subprocess.run(
                [
                    "sudo",
                    "systemctl",
                    "stop",
                    "drop-m1-loss-token-" + role,
                    "drop-m1-loss-link-" + role,
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        save(path, audit)
    ids = [v["id"] for v in audit["workers"].values()]
    if not audit.get("instances_terminated"):
        ec2.terminate_instances(InstanceIds=ids)
        ec2.get_waiter("instance_terminated").wait(
            InstanceIds=ids, WaiterConfig={"Delay": 3, "MaxAttempts": 100}
        )
        if any(
            cloud.verify_role(r, RUN_CONFIG)["State"]["Name"] != "terminated"
            for r in ("source", "dest")
        ):
            raise ValueError("one or both workers escaped termination")
        audit["instances_terminated"] = True
        save(path, audit)
    for w in audit["workers"].values():
        for vid in w["volumes"]:
            check = lambda: ec2.describe_volumes(VolumeIds=[vid])
            if not absent(check, {"InvalidVolume.NotFound"}):
                v = check()["Volumes"][0]
                if (
                    v["State"] != "available"
                    or v["Attachments"]
                    or {x["Key"]: x["Value"] for x in v["Tags"]}.get("Run")
                    != RUN_CONFIG["run"]
                ):
                    raise ValueError("unexpected worker disk")
                ec2.delete_volume(VolumeId=vid)
                ec2.get_waiter("volume_deleted").wait(
                    VolumeIds=[vid], WaiterConfig={"Delay": 3, "MaxAttempts": 80}
                )
            if not absent(check, {"InvalidVolume.NotFound"}):
                raise ValueError("temporary worker disk remains")
        if not all(
            absent(
                lambda n=n: ec2.describe_network_interfaces(NetworkInterfaceIds=[n]),
                {"InvalidNetworkInterfaceID.NotFound"},
            )
            for n in w["enis"]
        ):
            raise ValueError("temporary ENI still exists")
        w["disks_removed"] = w["enis_removed"] = True
    groups = [
        RUN_CONFIG["sg"],
        json.loads((cloud.EVIDENCE / "mail-fixture.json").read_bytes())[
            "mail_security_group"
        ],
    ]
    for g in groups:
        lookup = lambda: ec2.describe_security_groups(GroupIds=[g])
        if not absent(lookup, {"InvalidGroup.NotFound"}):
            tags = lookup()["SecurityGroups"][0]["Tags"]
            if {x["Key"]: x["Value"] for x in tags}.get("Run") != RUN_CONFIG["run"]:
                raise ValueError("incorrect security group")
            ec2.delete_security_group(GroupId=g)
        if not absent(lookup, {"InvalidGroup.NotFound"}):
            raise ValueError("temporary security group remains")
    audit["security_groups_removed"] = groups
    if not absent(lambda: iam.get_role(RoleName=ROLE), {"NoSuchEntity"}):
        tags = iam.get_role(RoleName=ROLE)["Role"]["Tags"]
        if {t["Key"]: t["Value"] for t in tags}.get("AppId") != APP_ID:
            raise ValueError("wrong role for cleanup")
        if not absent(
            lambda: iam.get_role_policy(RoleName=ROLE, PolicyName="ReplicaPrefix"),
            {"NoSuchEntity"},
        ):
            iam.delete_role_policy(RoleName=ROLE, PolicyName="ReplicaPrefix")
        iam.delete_role(RoleName=ROLE)
    provider = f"arn:aws:iam::{cloud.ACCOUNT}:oidc-provider/{DOMAIN}"
    if not absent(
        lambda: iam.get_open_id_connect_provider(OpenIDConnectProviderArn=provider),
        {"NoSuchEntity"},
    ):
        iam.delete_open_id_connect_provider(OpenIDConnectProviderArn=provider)
    audit["role_removed"] = absent(
        lambda: iam.get_role(RoleName=ROLE), {"NoSuchEntity"}
    )
    audit["oidc_removed"] = absent(
        lambda: iam.get_open_id_connect_provider(OpenIDConnectProviderArn=provider),
        {"NoSuchEntity"},
    )
    if not audit["role_removed"] or not audit["oidc_removed"]:
        raise ValueError("temporary AWS identity remains")
    for kind, allowed in [
        ("issuer", lambda k: k in (".well-known/openid-configuration", "keys.json")),
        ("replica", lambda k: k.startswith(APP_ID + "/")),
    ]:
        name = RUN_CONFIG[kind + "_bucket"]
        n = remove_bucket(s3, name, allowed)
        audit[kind + "_bucket_removed"] = True
        audit.setdefault(kind + "_objects_deleted", n["objects"])
        audit.setdefault(kind + "_bytes_deleted", n["bytes"])
        save(path, audit)
    private_files = [
        "worker-key",
        "worker-key.pub",
        "loss-signer.pem",
        "source-node-secret",
        "dest-node-secret",
        "mail.key",
        "mail-password",
        "hook-bearer",
        "source-owner-token",
        "dest-owner-token",
        "cloud-init",
    ]
    for f in private_files:
        (PRIVATE / f).unlink(missing_ok=True)
    audit["ephemeral_credentials_removed"] = all(
        not (PRIVATE / f).exists() for f in private_files
    )
    snapshots = ec2.describe_snapshots(
        OwnerIds=["self"], Filters=[{"Name": "tag:Run", "Values": [RUN_CONFIG["run"]]}]
    )["Snapshots"]
    if snapshots:
        raise ValueError("unexpected EBS snapshots for this run")
    audit["retained_aws_snapshots"] = 0
    audit["retained_running_instances"] = 0
    audit["retained_aws_disks"] = 0
    audit["retained_aws_buckets"] = 0
    audit["verified_at"] = utc()
    save(path, audit)


if __name__ == "__main__":
    main()
