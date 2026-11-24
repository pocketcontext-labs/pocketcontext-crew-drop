#!/usr/bin/python3
"""Exercise actual AWS role denial, app-prefix isolation, and byte durability with the proof principal."""

import argparse
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import time

import boto3
from botocore.exceptions import ClientError

from provision import PROOF, cloud
from bootstrap import APP_ID, RUN_CONFIG
from identity import ARN, CFG, DOMAIN, broker, key
from client import PRIVATE, attach_data, journal, save

REGION = cloud.REGION
BUCKET = RUN_CONFIG["replica_bucket"]
ROLE = "drop-m1-" + APP_ID
OPERATOR = boto3.Session(profile_name="drop-m1", region_name=REGION)


def sts_session():
    k = key()
    assertion = broker.token(
        k,
        broker.public_jwk(k)["kid"],
        "https://" + DOMAIN,
        CFG["audience"],
        APP_ID,
        int(time.time()),
        120,
    )
    res = OPERATOR.client("sts").assume_role_with_web_identity(
        RoleArn=ARN,
        RoleSessionName="crew-drop-acceptance",
        WebIdentityToken=assertion.decode(),
        DurationSeconds=900,
    )
    cred = res["Credentials"]
    session = boto3.Session(
        aws_access_key_id=cred["AccessKeyId"],
        aws_secret_access_key=cred["SecretAccessKey"],
        aws_session_token=cred["SessionToken"],
        region_name=REGION,
    )
    return session


def trust(enabled):
    if OPERATOR.client("sts").get_caller_identity()["Account"] != cloud.ACCOUNT:
        raise ValueError("unexpected AWS account")
    iam = OPERATOR.client("iam")
    role = iam.get_role(RoleName=ROLE)["Role"]
    if (
        role["Arn"] != ARN
        or {t["Key"]: t["Value"] for t in role["Tags"]}.get("AppId") != APP_ID
    ):
        raise ValueError("unexpected AWS role")
    path = PRIVATE / "original-role-trust.json"
    if not path.exists():
        doc = role["AssumeRolePolicyDocument"]
        grant = doc["Statement"]
        if (
            len(grant) != 1
            or grant[0]["Effect"] != "Allow"
            or grant[0]["Condition"]["StringEquals"]
            != {
                DOMAIN + ":aud": CFG["audience"],
                DOMAIN + ":sub": APP_ID,
            }
        ):
            raise ValueError("unexpected role trust conditions")
        save(path, doc)
    document = json.loads(path.read_bytes())
    if not enabled:
        document["Statement"][0]["Effect"] = "Deny"
    iam.update_assume_role_policy(RoleName=ROLE, PolicyDocument=json.dumps(document))
    deadline = time.monotonic() + 65
    while time.monotonic() < deadline:
        try:
            session = sts_session()
        except ClientError as exc:
            error = exc.response["Error"]["Code"]
            if error != "AccessDenied":
                raise RuntimeError("unexpected STS response " + error) from None
            if not enabled:
                journal(
                    "real-sts-denied",
                    app_id=APP_ID,
                    aws_http=exc.response["ResponseMetadata"]["HTTPStatusCode"],
                    request_id=exc.response["ResponseMetadata"]["RequestId"],
                    aws_code=error,
                )
                return
        else:
            if enabled:
                ident = session.client("sts").get_caller_identity()
                journal(
                    "real-sts-allowed",
                    app_id=APP_ID,
                    assumed_role=ident["Arn"].split("/")[-2],
                    aws_http=ident["ResponseMetadata"]["HTTPStatusCode"],
                )
                return
        time.sleep(1.1)
    raise RuntimeError("changed role trust did not propagate")


def write_gate(deny):
    """Change the policy evaluated for existing role sessions as well as new sessions."""
    iam = OPERATOR.client("iam")
    role = iam.get_role(RoleName=ROLE)["Role"]
    if (
        role["Arn"] != ARN
        or {t["Key"]: t["Value"] for t in role["Tags"]}.get("AppId") != APP_ID
    ):
        raise ValueError("unexpected role in S3 write gate")
    policy = "CrewAcceptancePauseWrites"
    probe = APP_ID + "/proof/write-gate"
    app_s3 = sts_session().client("s3")
    admin = OPERATOR.client("s3")
    if deny:
        iam.put_role_policy(
            RoleName=ROLE,
            PolicyName=policy,
            PolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Action": ["s3:PutObject", "s3:DeleteObject"],
                            "Resource": "arn:aws:s3:::" + BUCKET + "/" + APP_ID + "/*",
                        }
                    ],
                }
            ),
        )
    else:
        iam.delete_role_policy(RoleName=ROLE, PolicyName=policy)
    deadline = time.monotonic() + 60
    try:
        while time.monotonic() < deadline:
            try:
                app_s3.put_object(
                    Bucket=BUCKET, Key=probe, Body=b"synthetic visibility probe"
                )
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "AccessDenied":
                    raise
                if deny:
                    journal("app-role-own-s3-put-denied", aws_http=403)
                    return
            else:
                if not deny:
                    journal("app-role-own-s3-put-resumed", aws_http=200)
                    return
            time.sleep(0.45)
        raise RuntimeError("S3 write policy did not propagate")
    finally:
        admin.delete_object(Bucket=BUCKET, Key=probe)


def remote_note(marker, note):
    """Restore the latest independent S3 state without writing guest data."""
    base = PRIVATE / "forensics"
    base.mkdir(exist_ok=True, mode=0o700)
    litestream = base / "litestream"
    if not litestream.exists():
        with (
            gzip.open(PROOF / "smoke/assets/litestream.gz", "rb") as src,
            litestream.open("wb") as dst,
        ):
            shutil.copyfileobj(src, dst)
        litestream.chmod(0o700)
    with litestream.open("rb") as f:
        if (
            hashlib.file_digest(f, "sha256").hexdigest()
            != "f2d452c1527be81f06d5f135655b81b0e8d9a338cb4c2825cf2243134bc2dbd6"
        ):
            raise ValueError("incorrect proof Litestream distribution")
    output = base / (marker + ".db")
    subprocess.run(
        [
            litestream,
            "restore",
            "-force",
            "-integrity-check",
            "full",
            "-o",
            output,
            "s3://" + BUCKET + "/" + APP_ID + "/crew-drop/sqlite",
        ],
        env={**os.environ, "AWS_REGION": REGION, "AWS_PROFILE": "drop-m1"},
        check=True,
        capture_output=True,
    )
    with sqlite3.connect(output.as_uri() + "?mode=ro", uri=True) as conn:
        identity = conn.execute("SELECT app_id FROM _crew_drop_identity").fetchone()
        if identity != (APP_ID,):
            raise ValueError("incorrect S3 database identity")
        row = conn.execute(
            "SELECT title,body FROM crew_notes WHERE id=?", (note["id"],)
        ).fetchone()
    if row is not None and row != (note["title"], note["body"]):
        raise ValueError("remote marker content differs")
    journal(
        "independent-s3-restore",
        marker=marker,
        note_present=row is not None,
        integrity="ok",
    )
    return row is not None


def check_file():
    records = json.loads((PRIVATE / "records.json").read_bytes())
    digest = records["attachment"]["sha256"]
    own = APP_ID + "/files/" + digest
    s3 = sts_session().client("s3")
    head = s3.head_object(Bucket=BUCKET, Key=own)
    obj = s3.get_object(Bucket=BUCKET, Key=own)
    data = obj["Body"].read()
    if (
        hashlib.sha256(data).hexdigest() != digest
        or data != attach_data()
        or head["ContentLength"] != len(data)
    ):
        raise ValueError("S3 bytes differ from acknowledged upload")
    journal(
        "app-role-s3-own-attachment",
        object_key=own,
        bytes=len(data),
        sha256=digest,
        aws_http=200,
    )
    foreign = (
        "d-"
        + hashlib.sha256(("synthetic-other-" + RUN_CONFIG["run"]).encode()).hexdigest()[
            :15
        ]
    )
    if foreign == APP_ID:
        raise ValueError("foreign test prefix matches app")
    admin = OPERATOR.client("s3")
    canary = foreign + "/files/" + digest
    new_key = foreign + "/write-probe"
    admin.put_object(Bucket=BUCKET, Key=canary, Body=b"separate synthetic app prefix")
    try:
        checks = [
            ("GetObject", lambda: s3.get_object(Bucket=BUCKET, Key=canary)),
            (
                "PutObject",
                lambda: s3.put_object(
                    Bucket=BUCKET, Key=new_key, Body=b"test rejection"
                ),
            ),
            (
                "ListBucket",
                lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix=foreign + "/"),
            ),
        ]
        for op, fn in checks:
            try:
                fn()
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "AccessDenied":
                    raise RuntimeError(
                        "unexpected cross-prefix AWS error during " + op
                    ) from None
                journal(
                    "app-role-s3-foreign-prefix",
                    action=op,
                    prefix=foreign,
                    aws_http=exc.response["ResponseMetadata"]["HTTPStatusCode"],
                    aws_code="AccessDenied",
                )
            else:
                raise ValueError("AWS allowed foreign prefix access: " + op)
    finally:
        admin.delete_objects(
            Bucket=BUCKET, Delete={"Objects": [{"Key": canary}, {"Key": new_key}]}
        )


if __name__ == "__main__":
    os.umask(0o077)
    opts = argparse.ArgumentParser(description=__doc__)
    opts.add_argument("action", choices=("allow", "deny", "file"))
    action = opts.parse_args().action
    check_file() if action == "file" else trust(action == "allow")
