# AWS Crew Drop acceptance, 24 November 2026

The completed pilot ran the static, 83,078,960-byte Linux/amd64 binary built from `57c4280eff6e5bd663dce53501d71986883810a4` in `eu-north-1`. Its SHA-256 was `c8ce601c75e051ae04886f2ffb2a7f43e296761a7e56415cc94150f628fe30a2`.

| Receipt | Contents |
| --- | --- |
| [Results](results.json) | Timing boundaries, guest phases, authenticated read and write results, attachment checksum, IAM decisions and recovery point for the acknowledged tail |
| [Observation journal](observations.jsonl) | 146 ordered controller measurements and observed guest phases, including HTTP attempts, the held hook, the SIGKILL, the EC2 fence and tail write acknowledgements |
| [Earlier pilot](earlier-pilot.json) | Read-only evidence of corrupt cached hooks and rolled-back local state, plus independent historical S3 restores that recover the original note and attachment |
| [Resource audit](resource-audit.json) | Source/destination instance and disk IDs for both pilots, verified AWS removals, and sizes and SHA-256 digests of the private encrypted forensic archives |

The final same-volume restart served a note read in 39.239 ms and a note write in 41.641 ms after an observer confirmed that the private resume hook was pending. AWS STS denied the workload role throughout those requests. A separate reader that began with the Drop start call needed 1.227 s to its first successful signed read. After an abrupt VM kill, the next successful signed read took 3.780 s including retries. A physical SQLite copy contained the newly acknowledged note while an independently restored S3 replica still lacked it.

After the source EC2 instance stopped, the fresh destination restored all five explicitly synced notes and the authenticated attachment. It recovered the first 14 of 20 subsequent acknowledged writes. Six acknowledgements were lost across a 1.086 s interval. One additional write attempt received no acknowledgement and was absent. The measured S3 restore itself took 812.744 ms. Measured placement-to-business times include an intentional recovery hold and, for the first authorized read, a new synthetic owner login.

Both pilots used disposable development workers, node-private HTTP forwarding, a private synthetic email sink and real AWS STS and S3. Four workers were terminated and their four encrypted 24 GiB gp3 disks were deleted. The app and issuer buckets, roles, OIDC providers, network interfaces and security groups were removed. The encrypted forensic archives and their keys remain only in the controller's private operator storage. Both Drop app records are archived and all four development node records are disabled.
