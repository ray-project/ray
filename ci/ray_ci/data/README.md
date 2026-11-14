# HiveServer2 integration CI

The Data pipeline runs `run_hive_integration.sh` on premerge builds. It starts
`apache/hive:4.2.0` (pinned by digest so a republished tag cannot change the
fixture) with an embedded Derby metastore and local MapReduce, waits
for a successful Beeline `SELECT 1`, then runs
`//python/ray/data:test_hive_integration` in the Data integration image. The
service and test containers share an isolated Docker network; no host ports or
external credentials are needed.

The client uses `PLAIN` and the server uses `NONE`. The `hive` password is a
non-secret placeholder required by the client. This covers table/query reads,
limits, scalar types, and timestamps. The TLS-specific test remains skipped
because this fixture does not configure TLS.

The script preserves a failing test command's exit status, saves server and readiness
logs under `/artifact-mount/hive` (the RayCI artifact mount, so a failing run's
files are retained as downloadable build artifacts), and removes the container, its anonymous
volumes, and network on success or failure. If the container or network remains,
or cleanup cannot be verified, an otherwise successful run fails. Resources
already absent count as clean. Startup defaults to a 300-second
deadline with a 30-second limit per SQL probe. `RAY_HIVE_STARTUP_TIMEOUT` and
`RAY_HIVE_LOG_DIR` can override the deadline and log location for debugging.

Run the orchestration regression tests without Docker:

```bash
python -m pytest -v ci/ray_ci/data/test_run_hive_integration.py
```

To run the integration suite against an existing local HiveServer2 configured
with `NONE`, use a Ray development environment with `impyla==0.23.0` installed:

```bash
RAY_HIVE_TEST_HOST=127.0.0.1 RAY_HIVE_TEST_PORT=10000 \
  RAY_HIVE_TEST_AUTH=PLAIN HIVE_TEST_PASSWORD=hive \
  python -m pytest -v python/ray/data/tests/datasource/test_hive_integration.py
```

To run the explicit TLS-only security check, use a RayCI-compatible Linux
environment with Docker, Bazel, a checkout available as `/ray`, and
`RAYCI_CHECKOUT_DIR` set to the checkout path on the Docker host. The RayCI
runner also needs AWS credentials for its Amazon Elastic Container Registry (ECR)
login and access to the
`datamongobuild-py3.10`, Ray Core, and dashboard build images. This command
uses the RayCI image build and isn't a standalone local `pytest` invocation:

```bash
export RAY_HIVE_LOG_DIR="$(mktemp -d)"
echo "$RAY_HIVE_LOG_DIR"
bash ci/ray_ci/data/run_hive_integration.sh --tls
```

This uses the same Hive image and Data test environment with an ephemeral CA.
It checks verified table reads, rejection of an untrusted CA on the driver, and
rejection of a hostname mismatch on a read worker. The Ray nodes are separate
processes in one test container and share its filesystem. This manual security
check doesn't run as part of the default checks before merging. It doesn't
validate `GSSAPI`, `NOSASL`, or zero data RPCs after a TLS failure.
