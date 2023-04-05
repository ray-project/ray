import enum
import os
from dataclasses import dataclass
from typing import Optional, Dict, Tuple

class BuildkiteExitCode(enum.Enum):
    """ 
    Final exit code the test runner passes to buildkite-agent. This exit code is used
    to determine job policies, such as automatic retries
    """ 
    SUCCESS = 0
    UNKNOWN = 1
    TRANSIENT_INFRA_ERROR = 10
    INFRA_ERROR = 11
    INFRA_TIMEOUT = 30
    ERROR = 40 
    TIMEOUT = 42 

@dataclass
class Result:
    results: Optional[Dict] = None

    status: str = "invalid"
    return_code: int = 0
    buildkite_exit_code: int = BuildkiteExitCode.SUCCESS.value
    last_logs: Optional[str] = None

    runtime: Optional[float] = None
    stable: bool = True
    smoke_test: bool = False

    buildkite_url: Optional[str] = None
    wheels_url: Optional[str] = None
    cluster_url: Optional[str] = None

    # Anyscale Jobs specific
    job_url: Optional[str] = None
    job_id: Optional[str] = None

    buildkite_job_id: Optional[str] = None
    cluster_id: Optional[str] = None

    prometheus_metrics: Optional[Dict] = None
    extra_tags: Optional[Dict] = None


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
    FETCH_RESULT_ERROR = 18
    ANYSCALE_ERROR = 19

    # Infra timeouts (retryable)
    RAY_WHEELS_TIMEOUT = 30
    CLUSTER_ENV_BUILD_TIMEOUT = 31
    CLUSTER_STARTUP_TIMEOUT = 32
    CLUSTER_WAIT_TIMEOUT = 33

    # Command errors - these are considered application errors
    COMMAND_ERROR = 40
    COMMAND_ALERT = 41
    COMMAND_TIMEOUT = 42
    PREPARE_ERROR = 43

def handle_exception(e: Exception) -> Tuple[ExitCode, BuildkiteExitCode, Optional[int]]:
    from ray_release.exception import ReleaseTestError

    if not isinstance(e, ReleaseTestError):
      return ExitCode.UNKNOWN, BuildkiteExitCode.UNKNOWN, 0
    exit_code = e.exit_code
    if 1 <= exit_code.value < 10:
        error_type = BuildkiteExitCode.UNKNOWN
        runtime = None
    elif 10 <= exit_code.value < 20:
        retry_count = int(os.environ.get("BUILDKITE_RETRY_COUNT", "0"))
        # Retry at least once of transient infra error
        if retry_count == 0:
          error_type = BuildkiteExitCode.TRANSIENT_INFRA_ERROR
        else:
          error_type = BuildkiteExitCode.INFRA_ERROR
        runtime = None
    elif 30 <= exit_code.value < 40:
        error_type = BuildkiteExitCode.INFRA_TIMEOUT
        runtime = None
    elif exit_code == ExitCode.COMMAND_TIMEOUT:
        error_type = BuildkiteExitCode.TIMEOUT
        runtime = 0
    elif 40 <= exit_code.value:
        error_type = BuildkiteExitCode.ERROR
        runtime = 0

    return exit_code, error_type, runtime
