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




# cat << 'EOF' > "${EUGO_PROTO_CODEGEN_SH_PATH}"
# #!/usr/bin/env bash

# EUGO_PROTOC_PATH="$(which protoc)"
# eugo_log info "Using system protoc: '${EUGO_PROTOC_PATH}'"

# EUGO_GRPC_PYTHO_PLUGIN_PATH="$(which grpc_python_plugin)"
# eugo_log info "Using system grpc_python_plugin: '${EUGO_GRPC_PYTHO_PLUGIN_PATH}'"


# EUGO_OPENTELEMETRY_PROTO_SRC_DIR="$(pwd)/opentelemetry-proto/src"

# # Clean up pre-generated code
# find "${EUGO_OPENTELEMETRY_PROTO_SRC_DIR}" -regex ".*_pb2.*\.pyi?" -delete


# # Collect `.proto` files and all `.proto` files that are used by gRPC services
# EUGO_ALL_NON_SERVICE_PROTO_FILE_PATHS="$(find "${EUGO_OPEN_TELEMETRY_PROTO_FILES_PATH}" -iname "*.proto" ! -iname "*_service.proto")"
# eugo_log info "Found proto files: \n${EUGO_ALL_NON_SERVICE_PROTO_FILE_PATHS}"

# EUGO_ALL_SERVICE_PROTO_FILE_PATHS="$(find "${EUGO_OPEN_TELEMETRY_PROTO_FILES_PATH}" -iname "*_service.proto")"
# eugo_log info "Found gRPC service proto files: \n${EUGO_ALL_SERVICE_PROTO_FILE_PATHS}"


# eugo_log info "Generating '*_pb2.py' files ..."
# "${EUGO_PROTOC_PATH}" \
#   --proto_path="${EUGO_OPEN_TELEMETRY_PROTO_FILES_PATH}" \
#   --python_out="${EUGO_OPENTELEMETRY_PROTO_SRC_DIR}" \
#   ${EUGO_ALL_NON_SERVICE_PROTO_FILE_PATHS}
# eugo_log info "Successfully generated '*_pb2.py' files!"


# eugo_log info "Generating '*_pb2_grpc.py' and '*_pb2.py' files"
# "${EUGO_PROTOC_PATH}" \
#   --plugin=protoc-gen-grpc_python="${EUGO_GRPC_PYTHO_PLUGIN_PATH}" \
#   --proto_path="${EUGO_OPEN_TELEMETRY_PROTO_FILES_PATH}" \
#   --python_out="${EUGO_OPENTELEMETRY_PROTO_SRC_DIR}" \
#   --grpc_python_out="${EUGO_OPENTELEMETRY_PROTO_SRC_DIR}" \
#   --grpc_python_opt="grpc_2_0" \
#   ${EUGO_ALL_SERVICE_PROTO_FILE_PATHS}
# eugo_log info "Successfully generated '*_pb2_grpc.py' and '*_pb2.py' files!"
# EOF
