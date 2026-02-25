#!/usr/bin/env bash


# MARK: - 1. Input Definitions
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
eugo_log "2. Python Files Generation"

EUGO_PROTOC_PATH="$(which protoc)"
eugo_log "Using system protoc: '${EUGO_PROTOC_PATH}'"

EUGO_GRPC_PYTHON_PLUGIN_PATH="$(which grpc_python_plugin)"
eugo_log "Using system grpc_python_plugin: '${EUGO_GRPC_PYTHON_PLUGIN_PATH}'"

eugo_log "Generating '*_pb2_grpc.py' and '*_pb2.py' files"

"${EUGO_PROTOC_PATH}" \
  --plugin=protoc-gen-grpc_python="${EUGO_GRPC_PYTHON_PLUGIN_PATH}" \
  --proto_path="${SOURCE_ROOT}" \
  -I"${SOURCE_ROOT}/eugo/include" \
  --python_out="${BUILD_ROOT}" \
  --grpc_python_out="${BUILD_ROOT}" \
  --grpc_python_opt="grpc_2_0" \
  "${INPUT}"

eugo_log "Successfully generated '*_pb2_grpc.py' and '*_pb2.py' files!"


# MARK: - 3. Python Files Imports Patching
eugo_log "3. Python Files Imports Patching"
# @RODO: Whenever new folders added into Python files generation from `.proto`, we need to introduce the patch for that below to avoid breaking imports at runtime.
# As of now these are:
# 1. `@/src/ray/protobuf/prublic` -> protoc generates `from ..public import` -> we replace with `from . import`
#
# How to test it:
# 1. All `ray/core/generated/*.py` files should only import from the same directory `.` or from other packages (`google.protobuf`) but not for example from `..public`
for output in "${OUTPUTS[@]}"; do
    # MARK: - 1. Eugo-Vendored
    sed -i -E 's/from opencensus.proto.metrics.v1 import/from . import/' "${output}"
    sed -i -E 's/from opencensus.proto.resource.v1 import/from . import/' "${output}"

    # MARK: - 2. Ray-Provided
    # MARK: - 2.1. Primary
    sed -i -E 's/from src.ray.protobuf/from ./' "${output}"

    # MARK: - 2.2. Secondary
    sed -i -E 's/from ..public/from ./' "${output}"
done
