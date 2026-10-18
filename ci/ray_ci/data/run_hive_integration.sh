#!/bin/bash
# Run the real-server suite on an isolated, disposable HiveServer2 service.
set -euo pipefail

service="ray-hive-${BUILDKITE_JOB_ID:-$$}"
network="${service}-network"
log_dir="${RAY_HIVE_LOG_DIR:-/tmp/artifacts/hive}"
mkdir -p "${log_dir}"

cleanup() {
  status=$?
  trap - EXIT
  docker logs "${service}" > "${log_dir}/hiveserver2.log" 2>&1 || true
  if [[ ${status} -ne 0 ]]; then
    cat "${log_dir}/hiveserver2.log"
    cat "${log_dir}/readiness.log" 2>/dev/null || true
  fi
  docker rm -f -v "${service}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker network create "${network}"
# NONE still uses SASL PLAIN; the password is only a non-secret placeholder.
# Local MapReduce and Derby keep the fixture independent of HDFS/YARN services.
docker run -d --name "${service}" --network "${network}" \
  --network-alias hiveserver2 \
  -e SERVICE_NAME=hiveserver2 \
  -e 'SERVICE_OPTS=-Dhive.server2.authentication=NONE -Dhive.server2.enable.doAs=false -Dhive.execution.engine=mr -Dmapreduce.framework.name=local -Dfs.defaultFS=file:///' \
  apache/hive:4.2.0

# A listening TCP socket is insufficient: require an authenticated SQL query.
# Bound both each probe and the total wait, and detect a crashed service early.
deadline=$((SECONDS + ${RAY_HIVE_STARTUP_TIMEOUT:-300}))
until docker exec "${service}" timeout 30s beeline \
  -u jdbc:hive2://localhost:10000/default -n hive -p hive \
  -e 'SELECT 1' > "${log_dir}/readiness.log" 2>&1; do
  if [[ $(docker inspect -f '{{.State.Running}}' "${service}") != true ]] || \
      (( SECONDS >= deadline )); then
    echo "HiveServer2 did not become ready" >&2
    exit 1
  fi
  sleep 5
done

bazel run //ci/ray_ci:test_in_docker -- \
  //python/ray/data:test_hive_integration data \
  --build-name datamongobuild-py3.10 --python-version 3.10 \
  --exact-targets --network "${network}" \
  --test-env RAY_HIVE_TEST_HOST=hiveserver2 \
  --test-env RAY_HIVE_TEST_PORT=10000 \
  --test-env RAY_HIVE_TEST_AUTH=PLAIN \
  --test-env RAY_HIVE_TEST_USER=hive \
  --test-env HIVE_TEST_PASSWORD=hive \
  --test-env RAY_HIVE_TEST_USE_SSL=false
