#!/usr/bin/env bash
set -euo pipefail


# MARK: - 1. Input
INPUT="${1}"
SOURCE_ROOT="${2}"
BUILD_ROOT="${3}"
OUTPUT_PB2_PY="${4}"
OUTPUT_PB2_GRPC_PY="${5}"
OUTPUTS=(
  "${OUTPUT_PB2_PY}"
  "${OUTPUT_PB2_GRPC_PY}"
)


# MARK: - 2. Python Files Generation
python3 -m grpc_tools.protoc \
  --proto_path="${SOURCE_ROOT}" \
  -I"${SOURCE_ROOT}/eugo/include" \
  --python_out="${BUILD_ROOT}" \
  --grpc_python_out="${BUILD_ROOT}" \
  "${INPUT}"


# MARK: - 3. Python Files Patching
for output in "${OUTPUTS[@]}"; do
    sed -i -E 's/from src.ray.protobuf/from ./' "${output}"

    sed -i -E 's/from opencensus.proto.metrics.v1 import/from . import/' "${output}"
    sed -i -E 's/from opencensus.proto.resource.v1 import/from . import/' "${output}"

    sed -i -E 's/from ..experimental/from ./' "${output}"

    sed -i -E 's/from ..export_api/from ./' "${output}"
done
