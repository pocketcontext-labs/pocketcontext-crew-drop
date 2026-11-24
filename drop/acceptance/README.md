# Crew Drop development acceptance

The [24 November 2026 acceptance receipts](evidence/2026-11-24/README.md) record the completed pilot and the earlier failed pilot.

This harness drives the packaged Linux/amd64 executable through the Drop agent, NibRunner HTTP proxy, guest public API and private hook port. The controller records status and timings outside both worker VMs. Source and destination get separate encrypted local disks. Both have the same workload role for this one synthetic application. The destination receives its short-lived workload assertion only after EC2 confirms the source has stopped.

Run on the M1 development controller with the matching private `drop-m1-proof` and `pocketcontext-drop` checkouts next to this repository. Shared helpers install the released runner, create the dev Drop nodes, open node-private tunnels and provision a distinct signing key, OIDC provider, IAM role and S3 buckets. The controller's `drop-m1` AWS profile needs the same development permissions as the M1 abrupt recovery proof. Choose new private state and evidence directories. Port 18601 and 18602 and the `drop-m1-loss-{link,token}-{source,dest}` controller services are reserved for one pair of workers at a time.

```sh
umask 077
export DROP_M1_LOSS_STATE_DIR="$(mktemp -d "$HOME/.local/state/crew-drop-acceptance.XXXXXX")"
export DROP_M1_LOSS_EVIDENCE_DIR="$DROP_M1_LOSS_STATE_DIR/evidence"
export DROP_M1_PROOF="$(realpath ../drop-m1-proof)"
mkdir -m 0700 "$DROP_M1_LOSS_EVIDENCE_DIR"
python3 drop/acceptance/provision.py
bash "$DROP_M1_PROOF/abrupt/bundle.sh"
python3 "$DROP_M1_PROOF/abrupt/identity.py" setup
```

Wait for both new instances to accept SSH. For each role, run the following. The bootstrap can run on the two workers concurrently.

```sh
role=source # then repeat for dest
python3 "$DROP_M1_PROOF/abrupt/worker.py" "$role" 'sudo cloud-init status --wait && sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nftables xfsprogs e2fsprogs util-linux mount nbd-client fuse3 kmod curl ca-certificates jq'
python3 "$DROP_M1_PROOF/abrupt/bootstrap.py" "$role"
```

The fixture adds a temporary self-signed public CA to each disposable guest image for private test mail. It records the base and test rootfs digests. Its TLS-only SMTP server accepts one synthetic sender and recipient, requires authentication and has no forwarding operation. The extra security group on the destination accepts TCP 2525 from the public IPv4 addresses of the two workers only. Codes, tokens, SSH and signing keys stay in the private run directory or the receiving worker’s root-only tmpfs.

```sh
python3 drop/acceptance/mail_fixture.py
python3 drop/acceptance/configure.py
python3 drop/acceptance/configure.py --release drop/build/pocketcontext-crew-drop-linux-amd64
python3 drop/acceptance/scenarios.py source
python3 drop/acceptance/scenarios.py warm
python3 drop/acceptance/scenarios.py crash
python3 drop/acceptance/scenarios.py fence
python3 drop/acceptance/scenarios.py destination
python3 drop/acceptance/cleanup.py
```

- **Source** enrolls an empty volume, signs in through Crew's real email-code flow, acknowledges and reads a note, uploads a protected file, and checks both authenticated and anonymous access. The actual app role reads the file by digest. Foreign-prefix GetObject, PutObject and ListBucket must return AccessDenied.
- **Warm** stops the app on its original volume and denies the role's actual AssumeRoleWithWebIdentity policy. After a signed STS request returns AccessDenied the app restarts, an authenticated private resume hook remains in flight, and the public client reads an existing note and writes a new one. The run then releases the private hook, restores trust, and requires a successful Litestream sync. A separate activation reader starts concurrently with Drop and retries through temporary routing responses.
- **Crash** sends SIGKILL to the single verified Firecracker process for this app on the source. The app must reopen its own local database, read an acknowledged note and commit another without restoring. The guest kernel, writer and replicator all lose process state together. An S3 role policy denies writes throughout the crash even for existing sessions. The first successful read must return the note acknowledged immediately before the signal. A read-only physical SQLite and WAL copy includes it, and independent S3 restores check that it appears only after uploads resume.
- **Fence** verifies the destination has no instance, volume or volume file. Source synchronizes a baseline, then accepts a stream of unique synthetic notes without another explicit sync. The external journal records each attempt and acknowledgement as EC2 receives a skip-OS-shutdown stop request. Placement waits for the actual stopped state.
- **Destination** initially holds recovery. The public listener responds and normal data requests remain queued. After releasing the gate, one Litestream restore must serve the recovered Crew state. The old owner session must be rejected. A new owner email-code session checks the baseline notes, contiguous acknowledged tail, protected bytes, access controls and a fresh write. Unknown responses are compared to the recovered data separately.
- **Cleanup** archives the synthetic app, disables its two development nodes, terminates those instances and removes their disks, interfaces, security groups, per-run S3 data and issuer, workload role and temporary credentials. It requires prior verified recovery. The dev Drop registry retains the archived app and disabled node records.

For timing analysis use the JSONL in the private evidence directory. Drop checks TCP on the public application port. Time the first authenticated note read or write through that port. The private status timestamps show when the process bound its listeners and opened the database. Read and write request duration differs from process-start-to-first-ack. Source and replacement use authenticated SMTP, actual STS and S3, direct dev node HTTP forwarding, and the dev Drop artifact/secret store.
