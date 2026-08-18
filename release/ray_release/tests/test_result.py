import os
import sys
from unittest import mock

import pytest

from ray_release.exception import (
    RETRYABLE_EXIT_CODES,
    ClusterStartupTimeout,
    ExitCode,
    ReleaseTestConfigError,
    ReleaseTestError,
    ReleaseTestSetupError,
    TestCommandError,
    TestCommandTimeout,
)
from ray_release.result import (
    RETRY_MARKER_ENV_VAR,
    Result,
    ResultStatus,
    update_result_from_exception,
    write_retry_marker,
)


@pytest.fixture(autouse=True)
def clean_retry_env():
    """Retry decisions read these env vars; none may leak in from the runner."""
    with mock.patch.dict(os.environ, {}, clear=False):
        for var in (
            "BUILDKITE_RETRY_COUNT",
            "BUILDKITE_MAX_RETRIES",
            "BUILDKITE_TIME_LIMIT_FOR_RETRY",
            RETRY_MARKER_ENV_VAR,
        ):
            os.environ.pop(var, None)
        yield


def test_update_result_from_exception_config_error():
    result = Result()
    update_result_from_exception(result, ReleaseTestConfigError())
    assert result.return_code == ExitCode.CONFIG_ERROR.value
    assert result.status == ResultStatus.INFRA_ERROR.value
    assert result.last_logs is None
    # Deterministic: a retry would fail in exactly the same way.
    assert result.will_retry is False


def test_update_result_from_exception_release_test_error():
    result = Result()
    result.runtime = 10
    try:
        raise ReleaseTestError()
    except ReleaseTestError as e:
        update_result_from_exception(result, e, with_last_logs=True)
    assert result.return_code == ExitCode.UNSPECIFIED.value
    assert result.status == ResultStatus.RUNTIME_ERROR.value
    assert result.runtime == 10
    assert result.will_retry is False
    assert "ReleaseTestError" in result.last_logs
    assert __file__ in result.last_logs


def test_update_result_from_exception_unknown_error():
    result = Result()
    update_result_from_exception(result, Exception("generic"))
    assert result.return_code == ExitCode.UNKNOWN.value
    assert result.status == ResultStatus.UNKNOWN.value
    assert result.runtime == 0
    assert result.will_retry is False
    assert result.last_logs is None


def test_update_result_from_exception_infra_error_is_retried():
    with mock.patch.dict(os.environ, {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "100"}):
        result = Result()
        result.runtime = 10
        update_result_from_exception(result, ReleaseTestSetupError())
    assert result.return_code == ExitCode.SETUP_ERROR.value
    # The real classification survives; it is no longer overwritten.
    assert result.status == ResultStatus.INFRA_ERROR.value
    assert result.runtime == 10
    assert result.will_retry is True


def test_update_result_from_exception_infra_timeout_is_retried():
    with mock.patch.dict(os.environ, {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "100"}):
        result = Result()
        result.runtime = 10
        update_result_from_exception(result, ClusterStartupTimeout())
    assert result.return_code == ExitCode.CLUSTER_STARTUP_TIMEOUT.value
    assert result.status == ResultStatus.INFRA_TIMEOUT.value
    assert result.will_retry is True


def test_update_result_from_exception_command_error_is_not_retried():
    with mock.patch.dict(os.environ, {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "100"}):
        result = Result()
        result.runtime = 10
        update_result_from_exception(result, TestCommandError())
    assert result.return_code == ExitCode.COMMAND_ERROR.value
    assert result.status == ResultStatus.ERROR.value
    assert result.runtime == 0
    assert result.will_retry is False


def test_update_result_from_exception_command_timeout_is_not_retried():
    with mock.patch.dict(os.environ, {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "100"}):
        result = Result()
        result.runtime = 10
        update_result_from_exception(result, TestCommandTimeout())
    assert result.return_code == ExitCode.COMMAND_TIMEOUT.value
    assert result.status == ResultStatus.TIMEOUT.value
    assert result.runtime == 0
    assert result.will_retry is False


def test_update_result_from_exception_retry_limit_reached():
    with mock.patch.dict(
        os.environ,
        {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "100", "BUILDKITE_RETRY_COUNT": "1"},
    ):
        result = Result()
        result.runtime = 10
        update_result_from_exception(result, ReleaseTestSetupError())
    assert result.status == ResultStatus.INFRA_ERROR.value
    assert result.will_retry is False


def test_update_result_from_exception_too_expensive_to_retry():
    with mock.patch.dict(os.environ, {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "1"}):
        result = Result()
        result.runtime = 3600
        update_result_from_exception(result, ReleaseTestSetupError())
    assert result.status == ResultStatus.INFRA_ERROR.value
    assert result.runtime == 3600
    assert result.will_retry is False


def test_update_result_from_exception_honours_higher_retry_limit():
    """A test configured with num_retries: 3 keeps retrying past attempt 1."""
    with mock.patch.dict(
        os.environ,
        {
            "BUILDKITE_TIME_LIMIT_FOR_RETRY": "100",
            "BUILDKITE_RETRY_COUNT": "1",
            "BUILDKITE_MAX_RETRIES": "3",
        },
    ):
        result = Result()
        result.runtime = 10
        update_result_from_exception(result, ReleaseTestSetupError())
    assert result.will_retry is True


def test_write_retry_marker_creates_file_when_retrying(tmp_path):
    marker = tmp_path / "retry"
    with mock.patch.dict(os.environ, {RETRY_MARKER_ENV_VAR: str(marker)}):
        write_retry_marker(Result(will_retry=True))
    assert marker.exists()


def test_write_retry_marker_noop_when_not_retrying(tmp_path):
    marker = tmp_path / "retry"
    with mock.patch.dict(os.environ, {RETRY_MARKER_ENV_VAR: str(marker)}):
        write_retry_marker(Result(will_retry=False))
    assert not marker.exists()


def test_write_retry_marker_noop_without_env_var():
    # Local runs outside run_release_test.sh must not blow up.
    write_retry_marker(Result(will_retry=True))


def test_retryable_exit_codes_are_infra_only():
    assert RETRYABLE_EXIT_CODES == {
        ExitCode.SETUP_ERROR,
        ExitCode.CLUSTER_RESOURCE_ERROR,
        ExitCode.CLUSTER_ENV_BUILD_ERROR,
        ExitCode.CLUSTER_STARTUP_ERROR,
        ExitCode.ANYSCALE_ERROR,
        ExitCode.RAY_WHEELS_TIMEOUT,
        ExitCode.CLUSTER_ENV_BUILD_TIMEOUT,
        ExitCode.CLUSTER_STARTUP_TIMEOUT,
        ExitCode.CLUSTER_WAIT_TIMEOUT,
    }


@pytest.mark.parametrize(
    "exit_code",
    [
        ExitCode.SUCCESS,
        ExitCode.UNCAUGHT,
        ExitCode.UNSPECIFIED,
        ExitCode.UNKNOWN,
        ExitCode.CLI_ERROR,
        ExitCode.CONFIG_ERROR,
        ExitCode.FETCH_RESULT_ERROR,
        ExitCode.COMMAND_ERROR,
        ExitCode.COMMAND_ALERT,
        ExitCode.COMMAND_TIMEOUT,
        ExitCode.PREPARE_ERROR,
    ],
)
def test_non_infra_exit_codes_are_not_retryable(exit_code):
    assert exit_code not in RETRYABLE_EXIT_CODES


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
