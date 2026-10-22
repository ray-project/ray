# Hive authentication integration tests

Run the dedicated entry from a Ray checkout with Docker and Compose available:

```bash
bash ci/ray_ci/data/run_hive_auth_integration.sh \
  --log-dir /tmp/hive-auth-<unique-run-id>
```

The entry builds one shared runtime image, starts isolated KDC, HiveServer2,
driver, and worker containers, and runs each selected profile once. Choose a
new log directory for every invocation. It doesn't retry failed suites.
The container entry maps pytest's own status 1 to RayCI's test-failure status 42.
The original pytest status is saved as `pytest-exit-code` in the profile's
event artifacts. Per-profile JSON retains the Docker command status. Docker
exec failures before pytest starts retain their exit codes, and RayCI controls
job retries.
CI logs use `/artifact-mount`, which the platform exposes as host artifacts.
To reuse an existing runtime, pass `--runtime-image <image>`. The entry leaves
that caller-provided image in place.

Select profiles explicitly for a diagnostic run:

```bash
bash ci/ray_ci/data/run_hive_auth_integration.sh \
  --runtime-image <image> --profiles G \
  --log-dir /tmp/hive-auth-g-<unique-run-id>
```

`--prepare-only` checks real Impyla SQL and the remote worker without running
the Dataset cases. Preparation doesn't count as a passing security profile.
Avoid repeating a completed suite unless relevant code, fixtures, or runtime
configuration changed, or its previous result failed or was interrupted.

The case lists in `cases.json` define the expected execution scope:

| Profile | Server and client | Cases |
| --- | --- | --- |
| G | Kerberos and binary GSSAPI | Valid reads, missing or invalid caches, wrong service, expired cache |
| T | NONE and PLAIN over verified TLS | Valid reads, wrong CA, hostname mismatch, missing worker CA |
| GT | Kerberos and binary GSSAPI over verified TLS | Valid table and explicit-schema query reads |
| N | NOSASL | Valid table and explicit-schema query reads |

The result validator requires the exact case list, with no skipped, missing,
duplicate, or extra cases. A zero exit code with no tests isn't a pass.
The manual Bazel target registers the tests but doesn't use the generic CI
runner's manual-tag filtering. The dedicated entry invokes the pytest module
inside the driver container.

## Runtime and credentials

The local default uses the fixed Ray 2.59.0 Python 3.12 image and Hive 4.2.0.
CI supplies its current Python 3.10 Core installation through the existing
integration image and installs `data-hive-auth-ci_depset_py3.10.lock`. To build
against another prepared image, pass `--ray-image <image>` and the appropriate
`--requirements <lock-file>`.

The source checkout is read-only in the clients. Both nodes link its Data and
test packages into the installed Core environment. CI uses `RAYCI_CHECKOUT_DIR`
for Docker daemon bind paths. Verify the source and runtime evidence before
using a wheel-backed local result as evidence for another Core build.

All identities, keytabs, ticket caches, certificates, and private keys are
generated for the Compose project. Each client has its own credential volume.
Only the server receives the server keytab and TLS private key. The public CA
volume contains certificates, and the separate event volume contains no
credentials, authentication messages, SQL text, or row values.

GSSAPI uses QOP `auth`. TLS provides the separately tested encryption layer.
The expired-cache case starts with a real valid cache and waits past its
actual end time plus the effective Kerberos tolerance and a two-second margin.
The native library and Hive JVM configuration probes require a one-second
tolerance. The JVM probe uses Hive's bundled Janino and doesn't require a JDK.
No client keytab is configured as an implicit fallback.

G and GT prepare deterministic TEXTFILE rows with Hive's `LOAD DATA LOCAL`.
The source data file is mounted only in HiveServer2. Clients still read every
row through the authenticated HS2 connection. This keeps a secure MapReduce
write deployment outside the reader test. T and N retain the existing INSERT
fixture path. Every positive read checks all 2,049 rows, nulls, schema, and
multiple fetch batches.

## Results and resource ownership

Each case records events before the factory and actual client ExecuteStatement
calls. The assertions validate producer identity, sequence, expected stages,
and Dataset termination before accepting any RPC count. They aggregate all
producers for one execution. These are client invocation attempts, not a
server-side exactly-once guarantee.

Logs and per-profile JSON results remain in the requested directory. The
shared event volume contains JSONL evidence, JUnit XML, and the original
pytest status in `pytest-exit-code`. Private
credential volumes aren't copied into artifacts. Cleanup handles normal exit,
errors, and interruption and removes only the unique Compose project's
containers, network, and volumes. An image built by the entry belongs to that
invocation. Shared images and global Docker caches aren't pruned.

The service readiness budget is 300 seconds, individual pytest cases have
180 seconds, and each profile has a 1,200-second overall budget followed by
up to 60 seconds for diagnostics and cleanup. See the command's `--help` for
the orchestration options. Keep the outer CI deadline large enough to cover
the selected profiles, the one image build, and cleanup.
