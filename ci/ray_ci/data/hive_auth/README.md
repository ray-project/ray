# Hive authentication validation

Run these explicit security tests from a Ray checkout with Docker and Compose:

```bash
docker build -t ray-hive-auth-runtime:local \
  -f ci/ray_ci/data/hive_auth/Dockerfile .
bash ci/ray_ci/data/run_hive_auth_integration.sh \
  --runtime-image ray-hive-auth-runtime:local \
  --profiles G T GT N --log-dir "$(mktemp -d)"
```

The prepared image is reused by isolated KDC, driver and worker containers;
HiveServer2 uses Hive 4.2.0. The runner executes each selected profile once,
without internal retries. Use a new log directory for each invocation. Select
one profile with `--profiles G` for diagnosis. These tests aren't automatically
added to premerge and don't provide a Buildkite manual-trigger switch.

| Profile | Server and client | Cases |
| --- | --- | --- |
| G | Kerberos and binary GSSAPI | 8: reads, missing/invalid caches, wrong service |
| T | NONE and PLAIN over verified TLS | 7: reads, wrong CA, hostname mismatch, missing CA |
| GT | Kerberos and binary GSSAPI over verified TLS | 2: table and explicit-schema query reads |
| N | NOSASL | 2: table and explicit-schema query reads |

Positive table and query reads check all 2,049 rows, nulls, schema and multiple
fetches on a separate worker. Driver metadata failures and worker-query
failures use successful connection controls. Expired-ticket behavior and
worker-table failure cases are outside this matrix. Data is loaded only in
HS2; clients still read it through HS2. The exact JUnit case list rejects
missing, duplicate, extra, failed or skipped cases.

The default runtime uses Ray 2.59.0/Python 3.12 and the standalone Linux/Python
3.12 client/test dependency lock. Clients link the read-only checkout's Data
and test packages into that Core environment. Record image identities and
actual runtime versions with results. See the [configuration guide](../../../../doc/source/data/loading-data.md#configure-authentication-and-tls)
for dependencies, per-node credentials and certificate requirements.

Artifacts retain stage/service logs, JUnit and connection summaries. Only the
summaries exclude SQL, row values and credentials; raw logs and JUnit can
contain underlying exception details. Private credential volumes aren't
archived. Complete summaries, nodes and source paths must be checked before
accepting RPC counts across all connections. These are client RPC attempts,
not a server-side exactly-once guarantee.

The runner returns the original Docker/pytest status. Stage logs identify
where it failed. Readiness has 300 seconds, each case 180 seconds and each
profile 1,200 seconds, followed by up to 60 seconds for diagnostics and cleanup.
Cleanup preserves the first failure and removes only the unique project's
containers, network and volumes. Prepared images belong to the caller; the
runner never builds or removes images, shared caches or other projects.
