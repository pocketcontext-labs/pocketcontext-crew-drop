# Managed Crew sign-in on development KVM, 24 November 2026

Two freshly created Crew apps ran on one isolated Linux/amd64 node with Firecracker, a hosted Drop control plane, the real node agent and a receive-only TLS SMTP fixture. Chromium drove both sign-ins through the NibRunner HTTPS proxy. All addresses and credentials were synthetic.

| Component | Revision |
| --- | --- |
| Drop | [9e0a2aa](https://github.com/pocketcontext-labs/pocketcontext-drop/commit/9e0a2aa4cba478854822d52524364f0dae5c0cf5) |
| NibRunner | [b3fe792](https://github.com/pocketcontext-labs/pocketcontext-nibrunner/commit/b3fe792dcad23e40450df2e385de6e6d95579df6), native development daemon |
| PocketContext Core | [v0.3.2](https://github.com/pocketcontext-labs/pocketcontext-core/releases/tag/v0.3.2) |
| Crew guest | [ad8ae1c](https://github.com/pocketcontext-labs/pocketcontext-crew-drop/commit/ad8ae1c763e6fac7e60011e7fd46d5ccd49ed29b), a static 83,091,344-byte executable with SHA-256 `e941489798d421334a0071be0b99e41448581eda75cc0c73b3fb60d84158e50e` |

## Observations

- Each owner verified their email with Drop before creating their app. Each guest received its app’s creation-time address and had zero SMTP settings. Requests for the other owner’s address produced zero submissions.
- With Drop offline and the NibRunner daemon restarted, both owners requested codes concurrently in Chromium and signed into their respective apps. The mailbox checked the SMTP envelope, sender and recipient headers, subject, and fixed plain-text body.
- The mailbox captured a direct Drop message and a managed Crew message, then closed each SMTP connection before the final DATA reply. Each request returned a neutral OTP response. Over a full resend interval each transport made one submission. The captured codes failed to sign in. Explicitly requesting another code succeeded, and reusing it failed.
- After Drop reconnected, it disabled delivery for one app. The node applied the replacement deployment. A new request reached the broker, which returned `disabled` and made zero SMTP submissions.
- The second app slept on its idle policy. With Drop offline again, a request through its public route woke the VM. The owner received a new code and redeemed it exactly once.

## Reproduction and regression coverage

Build the Crew guest with `GOTOOLCHAIN=go1.27.3 drop/build.sh`, then run `python3 drop/check.py` for fresh boot, local restart, fenced restore and missing-replica refusal. Use the [isolated KVM command](../README.md#managed-sign-in-on-a-development-kvm-host) to reproduce the end-to-end observations.

Drop’s hosted provisioning tests cover forbidden caller-supplied addresses and owner configuration overrides, an unverified owner, and an account email change followed by redeployment, movement and creation of another app. Core tests cover expiry, single use and equivalent send-error cleanup. The NibRunner broker tests exercise leading zeros, malformed frames, rate limits, stale deployments, SMTP rejection, timeout, disconnect and trace redaction. The independently hosted Crew browser tests exercise its direct SMTP flow.
