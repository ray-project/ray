import os
from typing import Any, Callable


def _env_var(
    name: str,
    *,
    default: Any,
    cast_type: Callable,
) -> Any:
    return cast_type(os.getenv(name, default))


DEFAULT_SINGLE_STREAM_BANDWIDTH_MB_S = _env_var(
    "ANYTENSOR_SINGLE_STREAM_BANDWITH_MB_S",
    default=64,
    cast_type=float,
)

DEFAULT_TARGET_BANDWIDTH_MB_S = _env_var(
    "ANYTENSOR_TARGET_BANDWITH_MB_S",
    default=5 * 1024,
    cast_type=float,
)

DEFAULT_STREAMS_PER_PROCESS = _env_var(
    "ANYTENSOR_DEFAULT_STREAMS_PER_PROCESS",
    default=4,
    cast_type=int,
)

DEFAULT_BYTES_PER_SHARD = _env_var(
    "ANYTENSOR_DEFAULT_BYTES_PER_SHARD", default=32 * 1024 * 1024, cast_type=int
)

CURL_BUFFERSIZE_BYTES = _env_var(
    "ANYTENSOR_CURL_BUFFERSIZE_BYTES", default=512 * 1024, cast_type=int
)

MAX_RETRIES = _env_var("ANYTENSOR_MAX_RETRIES", default=5, cast_type=int)

PROCESS_SHUTDOWN_TIMEOUT_S = _env_var(
    "ANYTENSOR_PROCESS_SHUTDOWN_TIMEOUT_S", default=15, cast_type=float
)

LOG_LEVEL = _env_var("ANYTENSOR_LOG_LEVEL", default="INFO", cast_type=str).upper()

GPU_SINK_NUM_THREADS = _env_var(
    "ANYTENSOR_GPU_SINK_NUM_THREADS", default=2, cast_type=int
)
GPU_SINK_NUM_PINNED_BUFFERS = _env_var(
    "ANYTENSOR_GPU_SINK_NUM_PINNED_BUFFERS", default=4, cast_type=int
)

CPU_SINK_NUM_THREADS = _env_var(
    "ANYTENSOR_CPU_SINK_NUM_THREADS", default=8, cast_type=int
)
