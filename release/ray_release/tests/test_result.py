import os
from unittest import mock
from ray_release.result import handle_exception, ExitCode, BuildkiteExitCode
from ray_release.exception import ReleaseTestError, ReleaseTestSetupError


def test_handle_exception():
    """
    Unit test for ray_release.result.handle_exception
    """
    assert handle_exception(ReleaseTestError(), 10) == (
        ExitCode.UNSPECIFIED,
        BuildkiteExitCode.UNKNOWN,
        None,
    )
    assert handle_exception(ReleaseTestSetupError(), 10) == (
        ExitCode.SETUP_ERROR,
        BuildkiteExitCode.TRANSIENT_INFRA_ERROR,
        None,
    )
    with mock.patch.dict(os.environ, {"BUILDKITE_RETRY_COUNT": "1"}):
        assert handle_exception(ReleaseTestSetupError(), 10) == (
            ExitCode.SETUP_ERROR,
            BuildkiteExitCode.INFRA_ERROR,
            None,
        )
    assert handle_exception(ReleaseTestSetupError(), 3600) == (
        ExitCode.SETUP_ERROR,
        BuildkiteExitCode.INFRA_ERROR,
        None,
    )
