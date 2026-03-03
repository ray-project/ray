import os
import sys
from unittest import mock

import pytest

from ray_release.exception import (
    ExitCode,
    ReleaseTestConfigError,
    ReleaseTestError,
    ReleaseTestSetupError,
)
from ray_release.result import Result, ResultStatus, update_result_from_exception


def test_update_result_from_exception():
    # config error
    result = Result()
    update_result_from_exception(result, ReleaseTestConfigError())
    assert result.return_code == ExitCode.CONFIG_ERROR.value
    assert result.last_logs is None

    # release test error
    result = Result()
    result.runtime = 10
    try:
        raise ReleaseTestError()
    except ReleaseTestError as e:
        update_result_from_exception(result, e, with_last_logs=True)
    assert result.return_code == ExitCode.UNSPECIFIED.value
    assert result.status == ResultStatus.RUNTIME_ERROR.value
    assert result.runtime == 10
    assert "ReleaseTestError" in result.last_logs
    assert __file__ in result.last_logs

    # unknown error
    result = Result()
    update_result_from_exception(result, Exception("generic"))
    assert result.return_code == ExitCode.UNKNOWN.value
    assert result.status == ResultStatus.UNKNOWN.value
    assert result.runtime == 0
    assert result.last_logs is None

    # retriable
    with mock.patch.dict(os.environ, {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "100"}):
        result = Result()
        result.runtime = 10
        update_result_from_exception(result, ReleaseTestSetupError())
        assert result.return_code == ExitCode.SETUP_ERROR.value
        assert result.status == ResultStatus.TRANSIENT_INFRA_ERROR.value
        assert result.runtime == 10
    # retry limit reached, not retriable
    with mock.patch.dict(os.environ, {"BUILDKITE_RETRY_COUNT": "1"}):
        result = Result()
        result.runtime = 10
        update_result_from_exception(result, ReleaseTestSetupError())
        assert result.return_code == ExitCode.SETUP_ERROR.value
        assert result.status == ResultStatus.INFRA_ERROR.value
        assert result.runtime == 10
    # too long to run, not retriable
    with mock.patch.dict(os.environ, {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "1"}):
        result = Result()
        result.runtime = 3600
        update_result_from_exception(result, ReleaseTestSetupError())
        assert result.return_code == ExitCode.SETUP_ERROR.value
        assert result.status == ResultStatus.INFRA_ERROR.value
        assert result.runtime == 3600


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
