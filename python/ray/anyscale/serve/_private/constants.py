# Copyright (2023 and onwards) Anyscale, Inc.

import os

ANYSCALE_RAY_SERVE_ENABLE_PROPRIETARY_DEPLOYMENT_SCHEDULER = (
    os.environ.get("ANYSCALE_RAY_SERVE_ENABLE_PROPRIETARY_DEPLOYMENT_SCHEDULER", "1")
    == "1"
)

ANYSCALE_RAY_SERVE_DEFAULT_DRAINING_TIMEOUT_S = float(
    os.environ.get("ANYSCALE_RAY_SERVE_DEFAULT_DRAINING_TIMEOUT_S", 300.0)
)

# Default to 30 minutes
ANYSCALE_RAY_SERVE_COMPACTION_TIMEOUT_S = float(
    os.environ.get("ANYSCALE_RAY_SERVE_COMPACTION_TIMEOUT_S", 1800.0)
)

DEFAULT_TRACING_EXPORTER_IMPORT_PATH = (
    "ray.anyscale.serve._private.tracing_utils:default_tracing_exporter"
)
# Path to tracing exporter function
# If None, then use default tracing exporter
# If empty string, then tracing is disabled
ANYSCALE_TRACING_EXPORTER_IMPORT_PATH = os.environ.get(
    "ANYSCALE_TRACING_EXPORTER_IMPORT_PATH", DEFAULT_TRACING_EXPORTER_IMPORT_PATH
)

ANYSCALE_TRACING_SAMPLING_RATIO = float(
    os.environ.get("ANYSCALE_TRACING_SAMPLING_RATIO", 1)
)

# For now, this is used only for testing. In the suite of tests that
# use gRPC to send requests, we flip this flag on.
ANYSCALE_RAY_SERVE_USE_GRPC_BY_DEFAULT = (
    os.environ.get("ANYSCALE_RAY_SERVE_USE_GRPC_BY_DEFAULT", "0") == "1"
)

ANYSCALE_RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH = int(
    # Default max message length in gRPC is 4MB, we keep that default
    os.environ.get(
        "ANYSCALE_RAY_SERVE_REPLICA_GRPC_MAX_MESSAGE_LENGTH", 4 * 1024 * 1024
    )
)
