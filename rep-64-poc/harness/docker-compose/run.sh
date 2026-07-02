#!/usr/bin/env bash
# One-command Phase 1 Redis baseline runner.
#
# Usage: ./run.sh [output_path]
#   default output_path: ./baseline-redis.json
#
# Brings up a Redis container via docker compose (with fsync-per-write
# durability), runs bench.py against it, tears the container down, and
# writes the result JSON to the given path.
#
# Exit codes:
#   0  success
#   1  generic failure
#   2  prerequisite missing (docker compose, python deps)
#   3  redis healthcheck timed out

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
OUTPUT="${1:-${HERE}/baseline-redis.json}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found in PATH" >&2
  exit 2
fi

# Detect docker compose v1 (legacy "docker-compose") or v2 ("docker compose").
if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "neither 'docker compose' nor 'docker-compose' is available" >&2
  exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found in PATH" >&2
  exit 2
fi

# Use a venv to avoid polluting the host site-packages.
VENV="${HERE}/.venv"
if [[ ! -d "${VENV}" ]]; then
  python3 -m venv "${VENV}"
fi
# shellcheck source=/dev/null
source "${VENV}/bin/activate"
python3 -m pip install --quiet --disable-pip-version-check \
    -r "${HERE}/requirements.txt"

cleanup() {
  ( cd "${HERE}" && "${COMPOSE[@]}" down --volumes >/dev/null 2>&1 ) || true
}
trap cleanup EXIT

cd "${HERE}"
"${COMPOSE[@]}" up --detach --wait

# Resolve the actual image digest currently in use, for reproducibility.
REDIS_IMAGE_TAG="$(docker inspect rep64-poc-redis \
    --format='{{index .Config.Image}}@{{index .Image}}' 2>/dev/null || echo unknown)"
export REDIS_IMAGE_TAG
export HARNESS_RUNTIME="docker"

python3 "${HERE}/bench.py" \
    --host 127.0.0.1 \
    --port 6379 \
    --output "${OUTPUT}"

echo "Result written to: ${OUTPUT}" >&2
