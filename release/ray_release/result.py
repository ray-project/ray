import enum
from dataclasses import dataclass
from typing import Optional, Dict, Tuple

from ray_release.exception import (
    TestCommandTimeout,
    PrepareCommandTimeout,
    ClusterStartupTimeout,
    PrepareCommandError,
    ClusterEnvBuildError,
    ClusterEnvBuildTimeout,
    ClusterStartupError,
)


@dataclass
class Result:
    results: Optional[Dict] = None

    status: str = "invalid"
    return_code: int = 0
    last_logs: Optional[str] = None

    runtime: Optional[float] = None
    stable: bool = True

    buildkite_url: Optional[str] = None
    wheels_url: Optional[str] = None
    cluster_url: Optional[str] = None


class ExitCode(enum.Enum):
    # If you change these, also change the `retry` section
    # in `build_pipeline.py` and the `reason()` function in `run_e2e.sh`
    UNSPECIFIED = 2
    UNKNOWN = 3
    RUNTIME_ERROR = 4
    COMMAND_ERROR = 5
    COMMAND_TIMEOUT = 6
    PREPARE_TIMEOUT = 7
    # FILESYNC_TIMEOUT = 8
    SESSION_TIMEOUT = 9
    PREPARE_ERROR = 10
    APPCONFIG_BUILD_ERROR = 11
    INFRA_ERROR = 12


def handle_exception(e: Exception) -> Tuple[ExitCode, str, Optional[int]]:
    if isinstance(e, TestCommandTimeout):
        error_type = "timeout"
        runtime = 0
        exit_code = ExitCode.COMMAND_TIMEOUT
    elif isinstance(e, PrepareCommandTimeout):
        error_type = "infra_timeout"
        runtime = None
        exit_code = ExitCode.PREPARE_TIMEOUT
    elif isinstance(e, ClusterStartupTimeout):
        error_type = "infra_timeout"
        runtime = None
        exit_code = ExitCode.SESSION_TIMEOUT
    elif isinstance(e, PrepareCommandError):
        error_type = "infra_timeout"
        runtime = None
        exit_code = ExitCode.PREPARE_ERROR
    elif isinstance(e, (ClusterEnvBuildError, ClusterEnvBuildTimeout)):
        error_type = "infra_timeout"
        runtime = None
        exit_code = ExitCode.APPCONFIG_BUILD_ERROR
    elif isinstance(e, ClusterStartupError):
        error_type = "infra_error"
        exit_code = ExitCode.INFRA_ERROR
    elif isinstance(e, RuntimeError):
        error_type = "runtime_error"
        runtime = 0
        exit_code = ExitCode.RUNTIME_ERROR
    else:
        error_type = "unknown timeout"
        runtime = None
        exit_code = ExitCode.UNKNOWN

    return exit_code, error_type, runtime
