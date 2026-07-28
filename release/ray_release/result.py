import enum
import os
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from ray_release.exception import RETRYABLE_EXIT_CODES, ExitCode, ReleaseTestError

# run_release_test.sh owns this path and re-reads it after the test process exits to
# decide whether to ask Buildkite for another attempt.
RETRY_MARKER_ENV_VAR = "RELEASE_TEST_RETRY_MARKER"


class ResultStatus(enum.Enum):
    """
    Overall status of the result test run
    """

    SUCCESS = "success"
    UNKNOWN = "unknown"
    RUNTIME_ERROR = "runtime_error"
    # Deprecated: no longer assigned. Retryability now lives on Result.will_retry.
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

    # Whether Buildkite will run this test again. Consumed by RayTestDBReporter, so an
    # attempt that is about to be superseded is not recorded, and -- via the retry
    # marker -- by run_release_test.sh.
    will_retry: bool = False


def should_retry(result: Result) -> bool:
    """
    Whether this attempt is worth running again.

    An attempt is retried only when the test never got a fair chance to run, the
    Buildkite retry budget is not yet exhausted, and the attempt was cheap enough that
    re-running it is worth the compute.
    """
    try:
        exit_code = ExitCode(result.return_code)
    except ValueError:
        # An exit code we do not recognize. Fail closed and surface the failure.
        return False
    if exit_code not in RETRYABLE_EXIT_CODES:
        return False
    retry_count = int(os.environ.get("BUILDKITE_RETRY_COUNT", 0))
    max_retry = int(os.environ.get("BUILDKITE_MAX_RETRIES", 1))
    if retry_count >= max_retry:
        # Already reach retry limit
        return False
    return (result.runtime or 0) <= int(
        os.environ.get("BUILDKITE_TIME_LIMIT_FOR_RETRY", 0)
    )


def write_retry_marker(result: Result) -> None:
    """Tell run_release_test.sh that Buildkite should be asked for another attempt."""
    marker = os.environ.get(RETRY_MARKER_ENV_VAR)
    if marker and result.will_retry:
        Path(marker).touch()


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

    exit_code = e.exit_code
    result.return_code = exit_code.value

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

    # Retryability keys off the exit code, so the runtimes zeroed out above cannot
    # affect the decision: neither TIMEOUT nor ERROR is retryable.
    result.will_retry = should_retry(result)
