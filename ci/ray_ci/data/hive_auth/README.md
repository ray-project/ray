# Hive authentication integration tests

Prepare one reusable runtime from a Ray checkout with Docker and Compose:

```bash
docker build -t ray-hive-auth-runtime:local \
  -f ci/ray_ci/data/hive_auth/Dockerfile .
bash ci/ray_ci/data/run_hive_auth_integration.sh \
  --runtime-image ray-hive-auth-runtime:local \
  --profiles G T GT N --log-dir /tmp/hive-auth-<unique-run-id>
```

The runner consumes the prepared image and starts isolated KDC, HiveServer2,
driver and worker containers. It runs each selected profile once without
internal retries. Use a new log directory for every invocation. For diagnosis,
select one profile with `--profiles G`.

| Profile | Server and client | Cases |
| --- | --- | --- |
| G | Kerberos and binary GSSAPI | 12: reads, missing/invalid/expired caches, wrong service |
| T | NONE and PLAIN over verified TLS | 10: reads, wrong CA, hostname mismatch, missing CA |
| GT | Kerberos and binary GSSAPI over verified TLS | 2: table and explicit-schema query reads |
| N | NOSASL | 2: table and explicit-schema query reads |

Every positive read checks all 2,049 rows, nulls, schema and multiple fetches.
All profiles load deterministic TEXTFILE data only in HS2; clients read it
through HS2. Exact JUnit case-list validation rejects missing, duplicate,
extra, failed or skipped cases. The dedicated entry invokes pytest directly
in the driver container; it doesn't use generic manual-target filtering.

Local preparation defaults to Ray 2.59.0/Python 3.12 and Hive 4.2.0. CI uses a
Wanda security image with the Python 3.10 lock, then the existing tester installs
the checkout's Core before supplying the runtime. Both clients link the
read-only checkout's Data and test packages. Wheel-backed local results don't
validate another Core/Python baseline. See the [configuration guide](../../../../doc/source/data/loading-data.md#configure-authentication-and-tls)
for dependencies, per-node credentials and certificate requirements.

Artifacts retain stage/service logs, JUnit, `pytest-exit-code` and connection
summaries. Only the custom summaries exclude SQL, row values and credentials;
JUnit and raw logs can contain underlying exception details. Private credential
volumes aren't archived. Complete summaries, actual nodes and source paths
must be verified before accepting RPC counts across all connections.

Only after `pytest.main()` returns does the container map status 1 to RayCI's
status 42. `pytest-exit-code` retains the original status. Docker exec failures
before pytest starts retain their actual status, including 1; RayCI owns job
retries. CI artifacts use `/artifact-mount/hive-auth-<job-id>`.

Readiness has 300 seconds, each case 180 seconds and each profile 1,200 seconds,
followed by up to 60 seconds for diagnostics and cleanup. Cleanup preserves the
first failure and removes only the unique project's containers, network and
volumes. The runner never builds or removes images; prepared runtimes belong
to their caller or the native CI lifecycle. Shared images and caches remain.
