# Run Crew on Drop

This derivative starts at upstream Crew v0.45.4 (14fbf093ef493bb2ff92eaa256b73c28dad73864). The Drop executable contains the real Crew reader, PocketBase server, hosted email sign-in, MCP adapter and Litestream 0.5.18. It runs in one Linux/amd64 NibRunner guest per placement.

The [measured development results](RESULTS.md) include the restart regression, acknowledged-write durability and a fenced empty-host recovery.

## Build and stage

On Linux/amd64 install Go from go.mod, a musl C compiler, binutils, Node and pnpm from the reader toolchain, plus curl, tar and gzip. From the repository root run:

```sh
drop/build.sh
CGO_ENABLED=1 go test -tags "$(cat build-tags.txt),crew_drop" ./internal/client ./internal/dropfiles ./internal/dropsupervisor ./internal/crewruntime
python3 drop/check.py
python3 drop/idle.py
```

`drop/idle.py` starts the built guest with a file replica, waits 90 seconds after replication is ready (`--settle`), sends no requests for six minutes (`--idle`) and fails if the live database or its WAL changed or Litestream uploaded new WAL data (a level-0 file). It lists compaction files of earlier data separately. It takes about eight minutes.

Create a fresh app with `drop app create NAME --slug SLUG --kind crew` as its verified hosted owner. Upload `drop/build/pocketcontext-crew-drop-linux-amd64` as a Drop release and verify its SHA-256 against the build output. The build checks the archive and executable digests of the embedded Litestream child, verifies static linking and enforces the 90 MiB release limit. No arguments also starts the guest supervisor. Normal Crew CLI subcommands are available.

The executable smoke test uses temporary volumes and a file replica on loopback. It checks protected route registration after restart, shared cold recovery, the original owner and database identity, and refusal to reinitialize an established app with missing replica data.

The hosted origin is the exact public Drop hostname, with TLS terminated upstream. NibRunner must preserve the Host header. NibRunner starts or wakes the VM for an incoming request, checks for a TCP connection to port 8080, and forwards the original request there. NibRunner calls the private HTTP hook listener on the configured guest port with its per-app bearer. Drop renders the Crew app environment. The supervisor forwards business traffic to Crew’s hosted route allowlist on 127.0.0.1:18080. Forwarded client-address headers are replaced with the actual peer address. A shared ingress peer can therefore share one Crew IP rate-limit budget.

## Configuration

| Setting | Purpose |
| --- | --- |
| `NIBRUN_DATA_DIR` | Persistent per-app mount, normally `/app/data`. |
| `CREW_DROP_APP_ID` | The literal NibRunner ID supplied by Drop, for example `d-000000000000001`. |
| `CREW_DROP_BUCKET`, `AWS_REGION` | Shared replica bucket and its region. |
| `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE` | Shared role and rotating app-scoped guest token supplied by NibRunner. |
| `AWS_ENDPOINT_URL_S3`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Host services mode instead of the two above. NibRunner sets them when the node serves S3 (`hostServices.s3`): a loopback endpoint (`http://127.0.0.1:9000`, relayed to the host over vsock 51007) and placeholder keys. Litestream, attachments and enrollment use the endpoint path-style. The host drops the guest's signature, confines requests to `<AppId>/` and signs with the app's own session. The guest holds no AWS credential and waits for no token. |
| `NIBRUN_HOOK_PORT`, `NIBRUN_HOOK_TOKEN` | NibRunner supplies the private hook port (defaults to 8081) and required per-app bearer. |
| `POCKETCONTEXT_DOMAIN`, `DROP_OWNER_EMAIL` | Public Crew hostname and the verified owner address Drop recorded at app creation. Drop injects the reserved owner variable. |
| `CREW_DROP_BOOTSTRAP=1` | Allow an explicitly new app to claim an empty Crew replica namespace. Remove after initial enrollment. |
| `DROP_REPLICA_SYNC_INTERVAL` | Litestream upload interval, a Go duration from 1s to 300s, default 10s. Drop passes it as a per-app setting. A missing value uses the default; an invalid or out-of-range value is logged at startup and replaced by the default, so the guest still opens its port. |
| `CREW_DROP_WAIT`, `CREW_DROP_HOOK_TIMEOUT` | Data admission wait and hook deadline, Go durations defaulting to 10s and 9s. Suspend uses at most 9s to allow a reply within NibRunner’s default 10s deadline. |

Crew sends its six-digit sign-in code to the NibRunner host over vsock port 51006. The host addresses and submits the email using its own SMTP credential. Request, expiry and redemption remain in Crew. The guest needs no SMTP settings.

In host services mode the node, not the guest, exchanges the app's token. The same role, prefix and object grants apply to the host's requests. Trust the node issuer and expected audience and constrain the app subject in AWS. Grant ListBucket only under `<AppId>/` and grant the object operations required by Litestream and attachment storage only for that prefix. Drop hosts use one shared role scoped by the signed app ID session tag. The app stores its enrollment at `<AppId>/crew-drop/enrollment`, SQLite under `<AppId>/crew-drop/sqlite/`, and immutable file originals under `<AppId>/files/<sha256>`. A mounted token may arrive after local serving starts. Both consumers exchange it for scoped temporary credentials when they need AWS.

## Startup, shutdown and recovery

| Evidence on the destination | How the first data request is served |
| --- | --- |
| Existing private `crew/private/live/data.db`, including its local WAL | Bind both listeners, open the real Crew database and forward requests when hosted Crew accepts them. Hook delivery, token availability, STS, replica catch-up and attachment enumeration are off this path. |
| New app and explicit enrollment | Reserve the new app namespace, initialize the hosted owner and schema, and persist its app identity. The pending enrollment nonce belongs to this volume so a fresh replacement cannot claim an existing app. |
| Missing live database for an established app | Admit up to 64 waiting requests behind one recovery worker. Restore to a temporary file, validate full SQLite integrity and the app binding, then publish the canonical project snapshot and start Crew in hosted-restore mode. Timeout, overload and unavailable replicas return HTTP 503 with Retry-After. The worker retries. |

Core owns the private working copy and its atomic project snapshot at `crew/project/crew.db`. Each backend extracts its executable hooks, migrations and reader into a fresh process directory. Only the generated code directory points to the temporary bundle. The private socket directory, SQLite, keys and OAuth state keep their real persistent paths on every local restart. The supervisor checks that the real protected Crew read route requires authentication before forwarding business traffic. Litestream reads the live database and WAL. A local restart checks the app ID and recovers interrupted write-back through Core. A fresh host receives the verified SQLite snapshot before Core opens a new private working copy. Measure first service with an authenticated note read or write through 8080.

Lifecycle calls use the node-private listener on `NIBRUN_HOOK_PORT` and require the per-app bearer. Starting the process opens ingress automatically.

| Hook | Effect |
| --- | --- |
| `POST /hooks/suspend` | Drain requests, stop the Crew process (including its background jobs), confirm a Litestream sync and flush the guest filesystem, then continue Crew and reopen ingress for snapshot wake. |
| `POST /hooks/terminate` | The same drain, stop, confirmed sync and flush, keeping Crew stopped and ingress closed for handoff. A repeated terminate reuses the confirmed cut. |
| `POST /hooks/resume` | Continue a Crew process stopped by terminate and reopen ingress. It never starts a second backend. A normal start or snapshot wake can serve before this hook completes. |

A confirmed sync runs `litestream sync -wait -json` against the live database through Litestream's local control socket and requires the reported replica TXID to reach the local TXID. Its client timeout ends before the hook deadline. A hook answers 200 only after the drain, the confirmed sync and the flush; otherwise it answers 503 and stays within the deadline. Both bodies carry the outcome: `replication` is `confirmed` or `unconfirmed`, with the failed `stage` (`drain`, `freeze`, `sync` or `flush`), the error and the TXIDs. On failure, Crew continues and ingress reopens. NibRunner records the status as its hook outcome; `/status` keeps the last outcome as `last_hook`.

Authenticated `GET /status` reports data and replication states, the sync interval, the last hook outcome, recovery counts, waiters and UTC phase timestamps. The node is responsible for the actual source fence before granting a destination write authority. `POST /control/sync` requests an explicit synchronized checkpoint. The acceptance harness can deliberately hold or release the hooks, AWS or recovery worker through the authenticated control API.

The Drop supervisor holds the complete successful response to a potentially mutating request until the guest kernel has flushed the filesystem that holds the live database. This covers the database, WAL, local attachment files and directory updates on the same guest filesystem. A failed flush produces a retryable 503; use the original idempotency key. Known read-only Crew API calls can complete independently. Explicit sync, suspend and terminate acknowledgments include a local flush. The writer seal from terminate survives only while its supervisor runs. Confirm a fence at the node or EC2 layer before destination activation.

A verified file PUT precedes acknowledgment of its SQLite metadata. Protected downloads, archived records restored by the owner, signed previews and published snapshot images retain Crew's route-level authorization. The first authorized read on an empty volume verifies size and SHA-256 while restoring the original; subsequent reads can use the local file. Credential acquisition for an upload happens before the writer transaction. Its S3 PUT can still occupy the writer during the upload. A successful PUT whose transaction later rolls back can leave an unreferenced immutable object. WAL replication remains asynchronous; an acknowledged note or attachment reference newer than the replicated cut can be lost on abrupt host failure.

## Replication cost and loss window

The supervisor writes this Litestream 0.5.18 configuration (keys checked against `cmd/litestream/main.go` at v0.5.18), with the socket, paths, URL and region filled in:

```yaml
levels:
  - interval: 5m
  - interval: 4h
snapshot:
  interval: 24h
  retention: 72h
l0-retention-check-interval: 4h
socket:
  enabled: true
  path: ...
dbs:
  - path: .../crew/private/live/data.db
    replica:
      url: s3://BUCKET/APP/crew-drop/sqlite
      sync-interval: 10s   # DROP_REPLICA_SYNC_INTERVAL
      region: ...
```

- **PUTs follow writes.** Litestream uploads one L0 file per sync interval only when the WAL changed. A Crew that keeps writing for an hour costs at most 360 L0 PUTs at 10s, against 3,600 at 1s. Compaction adds at most one L1 file per 5 minutes and one L2 file per 4 hours of activity, and a daily snapshot of the whole database when it changed. An idle guest uploads nothing. About a minute after the last write, Litestream checkpoints the WAL into the database once and uploads one small L0 file for its own `_litestream_seq` bookkeeping. The next level boundaries compact it once each. Litestream does not repeat this without new writes, and Crew itself writes nothing while idle: `drop/idle.py` measured no database or WAL change and no new L0 upload over six idle minutes.
- **Restore egress is bounded.** A restore reads the latest daily snapshot plus the compacted files after it, not hours of per-second L0 files. Traffic from S3 to a node outside AWS is billed as egress, so a smaller restore is cheaper.
- **History.** Snapshots are kept for 72 hours, which is the point-in-time restore window. The bucket keeps noncurrent versions for 30 days in addition.
- **Loss window.** With the default 10s interval, up to about 10 seconds of acknowledged writes (plus the upload time) can be lost if the host disappears suddenly. A shorter interval narrows the window and costs more PUTs. A planned suspend or terminate loses nothing once its hook answered 200, because the answer requires a confirmed sync of every write committed before Crew was stopped. A 503 means the cut is unconfirmed; the data stays on the local volume and replicates when Litestream catches up.

An established cold restore revokes previous sessions and OTPs and suspends old webhook delivery. Sign in again as the same owner. The app has two recovery stores for its SQLite data and original attachment bytes. Crew's complete-backup process also covers operator material such as the private webhook encryption key, OAuth files and historical archives. Preserve the full backup and its secrets for an existing installation. Importing an existing non-Drop Crew dataset additionally requires securely backfilling every referenced file, including archived originals, and validating the owner and app binding before cutover. The development pilot starts with a newly enrolled app.

The default branch is main. The public Crew repository is the upstream source. This derivative's Drop CI builds and tests the guest. Upstream hosted image publication, deployment and release jobs are scoped to the public Crew repository. The [M1 source and host proofs](https://github.com/pocketcontext-labs/drop-m1-proof) document the underlying runner and identity path.
