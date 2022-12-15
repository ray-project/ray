import enum
from dataclasses import dataclass
from typing import Optional, Dict, Tuple


@dataclass
class Result:
    results: Optional[Dict] = None

    status: str = "invalid"
    return_code: int = 0
    last_logs: Optional[str] = None

    runtime: Optional[float] = None
    stable: bool = True
    smoke_test: bool = False

    buildkite_url: Optional[str] = None
    wheels_url: Optional[str] = None
    cluster_url: Optional[str] = None

    prometheus_metrics: Optional[Dict] = None


class ExitCode(enum.Enum):
    # If you change these, also change the `retry` section
    # in `build_pipeline.py` and the `reason()` function in `run_e2e.sh`
    SUCCESS = 0  # Do not set/return this manually
    UNCAUGHT = 1  # Do not set/return this manually

    UNSPECIFIED = 2
    UNKNOWN = 3

    # Hard infra errors (non-retryable)
    CLI_ERROR = 10
    CONFIG_ERROR = 11
    SETUP_ERROR = 12
    CLUSTER_RESOURCE_ERROR = 13
    CLUSTER_ENV_BUILD_ERROR = 14
    CLUSTER_STARTUP_ERROR = 15
    LOCAL_ENV_SETUP_ERROR = 16
    REMOTE_ENV_SETUP_ERROR = 17
    # ANYSCALE_SDK_ERROR = 19

    # Infra timeouts (retryable)
    RAY_WHEELS_TIMEOUT = 30
    CLUSTER_ENV_BUILD_TIMEOUT = 31
    CLUSTER_STARTUP_TIMEOUT = 32
    CLUSTER_WAIT_TIMEOUT = 33

    # Command errors
    COMMAND_ERROR = 40
    COMMAND_ALERT = 41
    COMMAND_TIMEOUT = 42
    PREPARE_ERROR = 43


def handle_exception(e: Exception) -> Tuple[ExitCode, str, Optional[int]]:
    from ray_release.exception import ReleaseTestError

    if isinstance(e, ReleaseTestError):
        exit_code = e.exit_code
        # Legacy reporting
        if 1 <= exit_code.value < 10:
            error_type = "runtime_error"
            runtime = None
        elif 10 <= exit_code.value < 20:
            error_type = "infra_error"
            runtime = None
        elif 30 <= exit_code.value < 40:
            error_type = "infra_timeout"
            runtime = None
        elif exit_code == ExitCode.COMMAND_TIMEOUT:
            error_type = "timeout"
            runtime = 0
        elif 40 <= exit_code.value < 50:
            error_type = "error"
            runtime = 0
        else:
            error_type = "error"
            runtime = 0
    else:
        exit_code = ExitCode.UNKNOWN
        error_type = "unknown error"
        runtime = 0

    return exit_code, error_type, runtime
