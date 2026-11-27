#!/bin/bash
# Run the real-server suite on an isolated, disposable HiveServer2 service.
set -euo pipefail

if [[ $# -gt 0 ]]; then
  [[ $# -eq 1 && $1 == --tls ]] || { echo "Usage: $0 [--tls]" >&2; exit 2; }
fi

service="ray-hive-${BUILDKITE_JOB_ID:-$$}"
network="${service}-network"
log_dir="${RAY_HIVE_LOG_DIR:-/artifact-mount/hive}"
mkdir -p "${log_dir}"

# Listing resources distinguishes an absent resource from a Docker daemon error.
check_removed() {
  local name=$1
  shift
  local names remaining
  if ! names=$(docker "$@" --filter "name=${name}"); then
    echo "Could not verify cleanup of ${name}" >&2
    return 1
  fi
  # Docker's name filter can also match other resources with similar names.
  while IFS= read -r remaining; do
    if [[ "${remaining}" == "${name}" ]]; then
      echo "Resource remains after cleanup: ${name}" >&2
      return 1
    fi
  done <<< "${names}"
}

cleanup() {
  local status=$? cleanup_failed=0
  trap - EXIT
  docker logs "${service}" > "${log_dir}/hiveserver2.log" 2>&1 || true
  if ! docker rm -f -v "${service}" >/dev/null; then
    echo "Failed to remove ${service}; checking whether it still exists" >&2
  fi
  if ! docker network rm "${network}" >/dev/null; then
    echo "Failed to remove ${network}; checking whether it still exists" >&2
  fi
  check_removed "${service}" container ls --all --format '{{.Names}}' || cleanup_failed=1
  check_removed "${network}" network ls --format '{{.Name}}' || cleanup_failed=1
  if [[ ${status} -eq 0 && ${cleanup_failed} -ne 0 ]]; then
    status=1
  fi
  if [[ ${status} -ne 0 ]]; then
    cat "${log_dir}/hiveserver2.log" || true
    cat "${log_dir}/readiness.log" 2>/dev/null || true
  fi
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker network create "${network}"
# NONE still uses SASL PLAIN; the password is only a non-secret placeholder.
# Local MapReduce and Derby keep the fixture independent of HDFS/YARN services.
export HIVE_TLS_PASSWORD
HIVE_TLS_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_hex(24))')
docker run -d --name "${service}" --network "${network}" \
  --network-alias hiveserver2 --network-alias wrong-hiveserver2 \
  -e SERVICE_NAME=hiveserver2 -e HIVE_TLS_PASSWORD \
  -e 'SERVICE_OPTS=-Dhive.server2.authentication=NONE -Dhive.server2.enable.doAs=false -Dhive.execution.engine=mr -Dmapreduce.framework.name=local -Dfs.defaultFS=file:///' \
  -v "${RAYCI_CHECKOUT_DIR:?RAYCI_CHECKOUT_DIR must be set}/ci/ray_ci/data/hive_tls_entry.sh:/tmp/hive_tls_entry.sh:ro" \
  --entrypoint bash \
  apache/hive:4.2.0@sha256:b0d2842722567a89ac35da8af9002b6bba0fc2d0eb272664954a1fa46d694472 \
  /tmp/hive_tls_entry.sh

# The test fixture checks a verified SQL query after TCP becomes available.
deadline=$((SECONDS + ${RAY_HIVE_STARTUP_TIMEOUT:-300}))
until docker exec "${service}" timeout 30s bash -c 'echo > /dev/tcp/localhost/10000' \
  > "${log_dir}/readiness.log" 2>&1; do
  if [[ $(docker inspect -f '{{.State.Running}}' "${service}") != true ]] || \
      (( SECONDS >= deadline )); then
    echo "HiveServer2 did not become ready" >&2
    exit 1
  fi
  sleep 5
done

# Only the public CA crosses into the test container; the keystore stays in Hive.
ca_pem=$(docker exec "${service}" cat /tmp/ray-hive-tls/ca.pem | base64 | tr -d '\n')
[[ -n "$ca_pem" ]] || { echo "Missing Hive TLS CA" >&2; exit 1; }
bazel run //ci/ray_ci:test_in_docker -- \
  //python/ray/data:test_hive_integration data \
  --build-name datamongobuild-py3.10 --python-version 3.10 \
  --exact-targets --network "${network}" \
  --test-env RAY_HIVE_TEST_HOST=hiveserver2 \
  --test-env RAY_HIVE_TEST_WRONG_HOST=wrong-hiveserver2 \
  --test-env RAY_HIVE_TEST_PORT=10000 \
  --test-env RAY_HIVE_TEST_AUTH=PLAIN \
  --test-env RAY_HIVE_TEST_USER=hive \
  --test-env HIVE_TEST_PASSWORD=hive \
  --test-env RAY_HIVE_TEST_USE_SSL=true \
  --test-env "RAY_HIVE_CA_PEM_B64=${ca_pem}"
