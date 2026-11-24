# Crew on Drop — development acceptance, 24 November 2026

The two-port test runs the actual Crew v0.45.4 application. Its acceptance condition is an authenticated note read or committed mutation through the node’s public HTTP port with a known payload. The separate hook port reports diagnostic timestamps and controls test holds. All records, attachments and mailboxes in these runs were synthetic.

## Architecture and measurement

Drop stages one static Linux/amd64 executable into a NibRunner microVM. The executable binds public port 8080 and authenticated hook port 8081, starts hosted Crew on loopback port 18080 and runs Litestream 0.5.18 as a separate child. The measured release answered an HTTP listener probe during recovery. The [current guest contract](README.md#build-and-stage) uses a TCP check on the same port. The timing and durability figures below belong to the measured release. Before forwarding a business request, the supervisor checks that Crew's protected JavaScript API has registered. The backend keeps the real SQLite working copy and WAL on the retained guest volume. Each process extracts its reader, migrations and hooks from the staged executable into private temporary storage.

When local data exists, the guest starts the owner’s application before token delivery, STS exchange or Litestream catch-up. On an empty established volume, up to 64 waiting requests share a single restore worker. It restores into a temporary SQLite file, performs a full integrity check, verifies the application ID and atomically publishes the project snapshot. Hosted restore revokes copied sessions and outstanding OTPs. The owner signs in again through the usual mail flow. Attachment originals have immutable SHA-256 object keys within the app prefix, and Crew applies the original access checks before materializing bytes.

The external client used each real NibRunner HTTP origin over an SSH tunnel with the configured Host header. The timings include that path. The prior public Cloudflare HTTPS and native snapshot wake experiments used a different path and remain documented in the [smoke-workload report](https://github.com/pocketcontext-labs/drop-m1-proof/blob/main/REVISED-M1.md). Single-run observations describe the particular disks, CPUs and workloads that produced them.

## What the first pilot caught

The first app successfully signed in and acknowledged a bucket, a note and a 262,176-byte protected attachment. A subsequent local boot returned 200 on `/up` and 404 on both real Crew read and write routes. The retained bundle contained a completion marker alongside 14 empty JavaScript files. PocketBase had added a 54-byte type-reference preamble to two empty route files. The fixed guest extracts a fresh code tree for each backend and verifies the protected route. Its first attempted repair exposed Core's requirement for a real private socket directory. The working implementation keeps SQLite, keys and the socket on their original physical paths and redirects only the generated code directory.

After restoring the routes, all three business collections were empty even though the owner's token still worked. Independent database restores identified the exact cut: S3 transaction 2 contained the original user, bucket, note and attachment metadata. Transactions 3 through 6, the retained live database including its WAL, and the project snapshot all contained the original user and zero business rows. The attachment original in S3 still matched its recorded digest. The test recovered and checked the original note and file metadata from transaction 2 before archiving that pilot. Resuming replication from the rolled-back local state had advanced the remote history with empty business collections.

The pinned Core database driver uses WAL with synchronous=NORMAL. The development runner's local-file stop path terminates the VMM without a guest shutdown or guest filesystem flush. The corrected write path adds a guest-local durability barrier before returning success on mutations. The startup or completion of an AWS credential exchange remains independent of a note write. A local flush cannot preserve a write when the entire source volume is lost before Litestream replicates it.

## Corrected guest and observed latencies

The final release was **83,078,960 bytes (79.23 MiB)**, leaving 10.77 MiB under Drop's 90 MiB limit. The SHA-256 was `c8ce601c75e051ae04886f2ffb2a7f43e296761a7e56415cc94150f628fe30a2`, built on clean runtime commit [57c4280](https://github.com/pocketcontext-labs/pocketcontext-crew-drop/commit/57c4280eff6e5bd663dce53501d71986883810a4). Two separate EC2 c8i.large hosts in eu-north-1 supplied source and destination for the final pilot. The Drop guest was configured with 1,024 MiB memory and a 2,048 MiB disk. Each pilot had its own workload identity and S3 namespace.

| Scenario | Measurement | Observed outcome |
| --- | --- | --- |
| First enrollment | Process entry → listener / data-ready / replica-ready | 1.250 ms / 1,176.343 ms / 2,214.481 ms |
| First authenticated operations | Create bucket / note / upload attachment | 63.892 ms / 16.647 ms / 186.011 ms. Authenticated note read and file download took 3.756 ms and 5.923 ms. |
| Existing database, STS denied and resume hook held | Process entry → listener / backend start / Crew data-ready | 1.034 ms / 2.909 ms / 297.162 ms. Zero restore attempts. |
| First concurrent requests in that fault window | Authenticated note read / new note write | Both HTTP 200, in 39.239 ms / 41.641 ms. The external client issued them after observing the private hook pending. A readback took 3.820 ms and the cached attachment download took 11.405 ms. |
| Separate clean start, client already attempting reads | Drop start → first authenticated read | 1,227.370 ms across two attempts. The first returned 503 in 1.270 ms; the successful request took 1,120.849 ms. Guest entry → Crew data-ready was 262.739 ms. |
| Retained-volume Firecracker SIGKILL with S3 writes denied | Last note commit / from client ACK to kill trigger / reactivation to authenticated read | 26.436 ms / 8.666 ms / 3,779.823 ms, including retries. Boot mode was local with zero S3 restores. |

The hard-kill experiment used a separate role policy that also denied object writes under already-issued credentials. After restart the authenticated client found its acknowledged note, committed a new note and read the original attachment. An independent SQLite check on a physical copy (528,384-byte DB, 45,352-byte WAL) verified integrity and the marker. A separately restored S3 copy lacked the marker while the deny was active and recovered it after replication resumed and an explicit sync completed. This distinguishes local acknowledgment durability from asynchronous remote recovery.

The file test uploaded a synthetic 262,176-byte attachment and verified every downloaded byte and the SHA-256. Anonymous requests received HTTP 401. Using temporary credentials for the same app role, a verifier could fetch the expected object and received HTTP 403 for a foreign-prefix list, read and write. The private hook endpoint required a bearer (401 without one); the public listener returned 404 for hook/control paths tested.

## Loss of the source host

The controller fsynced an external ledger of attempted and acknowledged writes. It verified the destination had no application, volume or files in the runner’s volume directory; forced the EC2 source to stop with OS shutdown skipped; and required AWS to report it stopped before assigning the destination. No source or destination volume was shared. The per-app S3 namespace and the verified data supplied the recovery input.

| Observation | Value |
| --- | ---: |
| EC2 stop request → confirmed stopped | 6.015 s |
| Protected notes explicitly synchronized before fault | 5 acknowledged, 5 recovered |
| Additional tail write attempts | 21 |
| Tail acknowledgments / recovered / lost | 20 / 14 / 6 |
| Last retained acknowledgment → last acknowledged write | 1.086007 s |
| Requests without an acknowledgment | 1; absent on destination |
| Concurrent requests observed queued behind one restore | 2 |
| Litestream S3 restore plus full SQLite integrity and app ID check | 812.744 ms |
| Fresh guest entry → public listener / Crew data-ready / replica-ready | 1.263 ms / 3.777 s / 5.222 s |
| Placement request → first successful `/up` / first authenticated note | 6.440 s / 7.678 s |
| Placement request → authorized attachment bytes | 7.897 s |
| First fresh-host file request / new write / its readback | 127.209 ms / 10.795 ms / 8.005 ms |

The test deliberately held the cold worker to observe both waiting requests. The old session received 401 after restoration; the private TLS mailbox delivered a fresh owner OTP. The authenticated client recovered the exact five checkpointed notes, all 14 retained tail notes in order, and the attachment with SHA-256 `6ad772b42c3fe36f9f639431a4d34e74237ea561762c398ae57b92f34d265e02`. It then wrote a new note, read it back and synchronized again. The 1.086-second lost interval is one observed asynchronous replication outcome.

## Validation and final resource state

The packaged check passed all four local scenarios, including the original stale-code failure. Go’s race detector passed the supervisor suite, including concurrent acknowledgments and injected disk-flush failure with a retryable response. The exact live binary passed eleven executable and package checks covering no-argument startup, the named CLI entry point, the real Crew browser, both end-to-end suites, historical upgrades, invitations, installer safeguards, native backup/recovery and storage tests. The unchanged reader passed all 944 tests after correcting a time-dependent test fixture.

Both synthetic apps were archived. Across the two pilots, cleanup verified termination of four c8i.large workers, removal of four encrypted 24 GiB gp3 volumes and their network interfaces, both temporary IAM roles and OIDC providers, and all four dedicated S3 buckets. No EC2 worker, EBS disk, snapshot or bucket from these two pilots remains. Encrypted offline forensic archives of 23,452,138 and 451,414 bytes were retained outside Git. The prior M1 fixtures are independent of these two pilots. [Sanitized run evidence and resource audit](acceptance/evidence/2026-11-24/)

## Reproduce and decide

The [guest guide](README.md) builds the packaged executable and exercises its four offline cases. The [dev harness](acceptance/README.md) provisions two isolated runners, an exact-subject web-identity role, S3 replicas and a receive-only TLS mailbox. The [integration pins](https://github.com/pocketcontext-labs/drop-m1-proof/blob/main/components.lock) identify the components on main. Ordinary Crew CLI, browser authentication, protected uploads and public route policies remain covered by the inherited product tests.

1. Agree on an acceptable lost-write interval. If an acknowledged note must survive loss of the whole host, wait for remote durability before acknowledging those operations and measure the write-latency cost. Repeat the active-stream test at representative write rates, database sizes and S3 object counts.
2. Give the fleet controller a durable placement generation and fencing procedure across runner restarts. Add a guest-aware planned stop via the existing host–guest control channel and verify guest flush and VMM exit. The dev pilot used an explicit EC2 fence. A restarted supervisor can write on a retained volume.
3. Measure real Crew on-request snapshot wake separately on production-like snapshot filesystems. Test cold replay without the diagnostic hold, across representative database and attachment sizes, and set separate targets for local activation, empty-volume recovery and reauthentication.
4. Before moving an existing hosted Crew workspace, validate a full restorable backup, owner binding, private webhook key, legacy archives and backfill of all attachment originals. Rehearse sign-in and webhook re-enrollment. Define retention and deletion authority for S3 history and orphaned files.
