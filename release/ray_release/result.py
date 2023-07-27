import enum
import os
from dataclasses import dataclass
from typing import Optional, Dict, Tuple


class ResultStatus(enum.Enum):
    """
    Overall status of the result test run
    """

    SUCCESS = "success"
    UNKNOWN = "unknown"
    RUNTIME_ERROR = "runtime_error"
    TRANSIENT_INFRA_ERROR = "transient_infra_error"
    INFRA_ERROR = "infra_error"
    INFRA_TIMEOUT = "infra_timeout"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass
class Result:
    results: Optional[Dict] = None

    status: str = ResultStatus.UNKNOWN.value
    return_code: int = 0
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


def _is_transient_error(result_status: ResultStatus, runtime: int) -> bool:
    """
    Classify whether an infra-failure issue is a transient issue. This is based on
    the status of its previous retries, and its runtime.
    """
    retry_count = int(os.environ.get("BUILDKITE_RETRY_COUNT", 0))
    max_retry = int(os.environ.get("BUILDKITE_MAX_RETRIES", 1))
    if retry_count >= max_retry:
        # Already reach retry limit
        return False
    if runtime > int(os.environ.get("BUILDKITE_TIME_LIMIT_FOR_RETRY", 0)):
        # Take too long to run
        return False
    return True


def handle_exception(
    e: Exception, run_duration: int
) -> Tuple[ExitCode, ResultStatus, Optional[int]]:
    from ray_release.exception import ReleaseTestError

    if not isinstance(e, ReleaseTestError):
        return ExitCode.UNKNOWN, ResultStatus.UNKNOWN, 0
    exit_code = e.exit_code
    if 1 <= exit_code.value < 10:
        result_status = ResultStatus.RUNTIME_ERROR
        runtime = None
    elif 10 <= exit_code.value < 20:
        result_status = ResultStatus.INFRA_ERROR
        runtime = None
    elif 30 <= exit_code.value < 40:
        result_status = ResultStatus.INFRA_TIMEOUT
        runtime = None
    elif exit_code == ExitCode.COMMAND_TIMEOUT:
        result_status = ResultStatus.TIMEOUT
        runtime = 0
    elif 40 <= exit_code.value:
        result_status = ResultStatus.ERROR
        runtime = 0

    # if this result is to be retried, mark its status as transient
    # this logic should be in-sync with run_release_test.sh
    if _is_transient_error(result_status, run_duration):
        result_status = ResultStatus.TRANSIENT_INFRA_ERROR

    return exit_code, result_status, runtime
