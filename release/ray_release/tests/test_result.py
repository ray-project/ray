import os
import sys
from unittest import mock

import pytest

from ray_release.exception import ExitCode, ReleaseTestError, ReleaseTestSetupError
from ray_release.result import ResultStatus, handle_exception


def test_handle_exception():
    """
    Unit test for ray_release.result.handle_exception
    """
    assert handle_exception(ReleaseTestError(), 10) == (
        ExitCode.UNSPECIFIED,
        ResultStatus.RUNTIME_ERROR,
        None,
    )
    # retriable
    with mock.patch.dict(os.environ, {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "100"}):
        assert handle_exception(ReleaseTestSetupError(), 10) == (
            ExitCode.SETUP_ERROR,
            ResultStatus.TRANSIENT_INFRA_ERROR,
            None,
        )
    # retry limit reached, not retriable
    with mock.patch.dict(os.environ, {"BUILDKITE_RETRY_COUNT": "1"}):
        assert handle_exception(ReleaseTestSetupError(), 10) == (
            ExitCode.SETUP_ERROR,
            ResultStatus.INFRA_ERROR,
            None,
        )
    # too long to run, not retriable
    with mock.patch.dict(os.environ, {"BUILDKITE_TIME_LIMIT_FOR_RETRY": "1"}):
        assert handle_exception(ReleaseTestSetupError(), 3600) == (
            ExitCode.SETUP_ERROR,
            ResultStatus.INFRA_ERROR,
            None,
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
