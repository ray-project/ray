import enum
import os
import traceback
from dataclasses import dataclass
from typing import Dict, Optional

from ray_release.exception import ExitCode, ReleaseTestError


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

    # Anyscale Jobs specific
    job_url: Optional[str] = None
    job_id: Optional[str] = None

    buildkite_job_id: Optional[str] = None

    prometheus_metrics: Optional[Dict] = None
    extra_tags: Optional[Dict] = None


def _is_transient_error(runtime: int) -> bool:
    """
    Classify whether an infra-failure issue is a transient issue. This is based on
    the status of its previous retries, and its runtime.
    """
    retry_count = int(os.environ.get("BUILDKITE_RETRY_COUNT", 0))
    max_retry = int(os.environ.get("BUILDKITE_MAX_RETRIES", 1))
    if retry_count >= max_retry:
        # Already reach retry limit
        return False
    return runtime <= int(os.environ.get("BUILDKITE_TIME_LIMIT_FOR_RETRY", 0))


def update_result_from_exception(
    result: Result, e: Exception, with_last_logs: bool = False
):
    if with_last_logs and result.last_logs is None:
        result.last_logs = "".join(traceback.format_exception(e))

    if not isinstance(e, ReleaseTestError):
        result.return_code = ExitCode.UNKNOWN.value
        result.status = ResultStatus.UNKNOWN.value
        result.runtime = 0
        return

    # Used for transient error detection.
    # The logic does not really make sense.. but it is the same as the logic
    # before refactoring.. and there are tests depends on the behavior..a
    # TODO(aslonnie): clean up the logic...
    original_runtime = result.runtime or 0

    exit_code = e.exit_code
    if 1 <= exit_code.value < 10:
        result.status = ResultStatus.RUNTIME_ERROR.value
    elif 10 <= exit_code.value < 20:
        result.status = ResultStatus.INFRA_ERROR.value
    elif 30 <= exit_code.value < 40:
        result.status = ResultStatus.INFRA_TIMEOUT.value
    elif exit_code == ExitCode.COMMAND_TIMEOUT:
        result.status = ResultStatus.TIMEOUT.value
        result.runtime = 0
    elif 40 <= exit_code.value:
        result.status = ResultStatus.ERROR.value
        result.runtime = 0

    # if this result is to be retried, mark its status as transient
    # this logic should be in-sync with run_release_test.sh
    if _is_transient_error(original_runtime):
        result.status = ResultStatus.TRANSIENT_INFRA_ERROR.value

    result.return_code = exit_code.value
