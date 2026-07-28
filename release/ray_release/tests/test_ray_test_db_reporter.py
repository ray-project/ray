import os
import sys
from unittest import mock

import pytest

from ray_release.bazel import bazel_runfile
from ray_release.configs.global_config import get_global_config, init_global_config
from ray_release.reporter.ray_test_db import RayTestDBReporter
from ray_release.result import Result, ResultStatus
from ray_release.test import Test
from ray_release.test_automation.release_state_machine import ReleaseTestStateMachine
from ray_release.test_automation.state_machine import TestStateMachine

init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

# Pre-populate these so the state machine constructor short-circuits instead of
# reaching for real GitHub and Buildkite tokens out of AWS Secrets Manager.
TestStateMachine.ray_repo = mock.MagicMock()
TestStateMachine.ray_buildkite = mock.MagicMock()


@pytest.fixture
def postmerge_env():
    with mock.patch.dict(
        os.environ,
        {
            "BUILDKITE_BRANCH": "master",
            "BUILDKITE_PIPELINE_ID": get_global_config()["ci_pipeline_postmerge"][0],
        },
    ):
        yield


def test_skips_recording_an_attempt_that_will_be_retried(postmerge_env):
    test = Test({"name": "test_x"})
    result = Result(status=ResultStatus.INFRA_ERROR.value, will_retry=True)

    with mock.patch.object(Test, "persist_result_to_s3") as persist:
        RayTestDBReporter().report_result(test, result)

    persist.assert_not_called()


@pytest.mark.parametrize(
    "status",
    [
        ResultStatus.ERROR.value,
        ResultStatus.RUNTIME_ERROR.value,
        ResultStatus.INFRA_ERROR.value,
        ResultStatus.SUCCESS.value,
    ],
)
def test_records_a_final_attempt_whatever_its_status(postmerge_env, status):
    test = Test({"name": "test_x"})
    result = Result(status=status, will_retry=False)

    with mock.patch.object(Test, "persist_result_to_s3") as persist, mock.patch.object(
        Test, "update_from_s3"
    ), mock.patch.object(Test, "get_test_results", return_value=[]), mock.patch.object(
        Test, "persist_to_s3"
    ), mock.patch.object(
        ReleaseTestStateMachine, "move"
    ):
        RayTestDBReporter().report_result(test, result)

    persist.assert_called_once_with(result)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
