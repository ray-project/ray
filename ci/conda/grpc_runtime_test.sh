#!/bin/bash
# Runs a Ray wheel next to conda-forge's gRPC in one process.
#
# Ray links its own gRPC, protobuf and abseil statically into _raylet.so. On
# PyPI, grpcio does the same, so the two copies never meet. On conda-forge,
# grpcio instead loads a shared libgrpc.so (and libprotobuf, libabseil) that
# exports its symbols. If _raylet.so ever exports its own copies too, the
# dynamic linker binds some calls across the two versions, and Ray hangs or
# crashes (conda-forge/ray-packages-feedstock#136).
#
# This test puts a Ray wheel in a conda-forge environment and exercises the
# gRPC paths where both copies are active. It needs no conda build of Ray.
#
# Usage: ci/conda/grpc_runtime_test.sh <ray wheel, directory holding one, or URL>
#
# Environment:
#   PYTHON_VERSION      Python for the conda env (default 3.12). Must match
#                       the wheel.
#   MICROMAMBA_VERSION  micromamba release to download.
#   CONDA_GRPC_TEST_DIR Scratch directory (default: a new temp directory).

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <ray wheel, directory holding one, or URL>" >&2
  exit 2
fi

WHEEL="$1"
PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
MICROMAMBA_VERSION="${MICROMAMBA_VERSION:-2.10.0-0}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK_DIR="${CONDA_GRPC_TEST_DIR:-$(mktemp -d)}"
ENV_NAME="ray-conda-grpc"

case "$(uname -m)" in
  x86_64) MAMBA_ARCH="linux-64" ;;
  aarch64) MAMBA_ARCH="linux-aarch64" ;;
  *)
    echo "Unsupported architecture: $(uname -m)" >&2
    exit 2
    ;;
esac

mkdir -p "${WORK_DIR}/bin" "${WORK_DIR}/wheel"
export MAMBA_ROOT_PREFIX="${WORK_DIR}/mamba"
MAMBA="${WORK_DIR}/bin/micromamba"

echo "--- Install micromamba ${MICROMAMBA_VERSION}"
curl -fsSL -o "${MAMBA}" \
  "https://github.com/mamba-org/micromamba-releases/releases/download/${MICROMAMBA_VERSION}/micromamba-${MAMBA_ARCH}"
chmod +x "${MAMBA}"

if [[ "${WHEEL}" == http://* || "${WHEEL}" == https://* ]]; then
  echo "--- Download ${WHEEL}"
  # Keep the file name: pip reads the version and platform tags from it.
  (cd "${WORK_DIR}/wheel" && curl -fsSL -O "${WHEEL}")
  WHEEL="${WORK_DIR}/wheel"
fi

if [[ -d "${WHEEL}" ]]; then
  WHEELS=("${WHEEL}"/ray-*.whl)
  if [[ ${#WHEELS[@]} -ne 1 || ! -f "${WHEELS[0]}" ]]; then
    echo "Expected exactly one ray-*.whl in ${WHEEL}, found: ${WHEELS[*]}" >&2
    exit 2
  fi
  WHEEL="${WHEELS[0]}"
fi
# pip treats a relative path with extras as a package name.
WHEEL="$(realpath "${WHEEL}")"

echo "--- Create conda-forge env (python ${PYTHON_VERSION}, grpcio, protobuf)"
"${MAMBA}" create -y -n "${ENV_NAME}" --override-channels -c conda-forge \
  "python=${PYTHON_VERSION}" pip grpcio protobuf
"${MAMBA}" list -n "${ENV_NAME}" | grep -E "grpc|protobuf|abseil" || true

run() {
  "${MAMBA}" run -n "${ENV_NAME}" "$@"
}

# Pin grpcio and protobuf to what conda installed. Without this, pip is free
# to replace them with PyPI wheels that bundle their own gRPC, and the test
# would pass without testing anything.
CONSTRAINTS="${WORK_DIR}/conda-pins.txt"
run python - > "${CONSTRAINTS}" <<'EOF'
from importlib import metadata

for name in ("grpcio", "protobuf"):
    print(f"{name}=={metadata.version(name)}")
EOF
echo "--- pip constraints"
cat "${CONSTRAINTS}"

echo "--- Install ${WHEEL}"
run python -m pip install -c "${CONSTRAINTS}" "${WHEEL}[default,client,serve-grpc]"

echo "--- Run smoke test"
run python "${SCRIPT_DIR}/grpc_runtime_smoke_test.py"
