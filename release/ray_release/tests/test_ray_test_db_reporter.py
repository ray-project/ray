import sys
from unittest.mock import patch

import pytest

from ray_release.bazel import bazel_runfile
from ray_release.configs.global_config import init_global_config
from ray_release.reporter.ray_test_db import RayTestDBReporter
from ray_release.result import Result, ResultStatus
from ray_release.test import Test

init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

# From ci_pipeline.postmerge in that config.
_POSTMERGE_PIPELINE = "0189e759-8c96-4302-b6b5-b4274406bf89"


def _env(branch: str, pipeline: str = _POSTMERGE_PIPELINE) -> dict:
    return {"BUILDKITE_BRANCH": branch, "BUILDKITE_PIPELINE_ID": pipeline}


@pytest.mark.parametrize(
    "branch",
    ["releases/2.58.0", "releases/1.0.0", "sai-miduthuri/some-branch", ""],
    ids=["release_branch", "old_release_branch", "feature_branch", "unset"],
)
def test_nothing_is_recorded_off_master(branch) -> None:
    """Enabling the agent on release branches is only safe because of this.

    The guard holds independently of REPORT_TO_RAY_TEST_DB ever being set.
    """
    test = Test({"name": "test_name"})
    result = Result()
    result.status = ResultStatus.ERROR.value

    with patch.dict("os.environ", _env(branch), clear=True), patch.object(
        Test, "persist_result_to_s3"
    ) as persist_result, patch.object(Test, "persist_to_s3") as persist_test, patch(
        "ray_release.reporter.ray_test_db.ReleaseTestStateMachine"
    ) as state_machine:
        RayTestDBReporter().report_result(test, result)

    persist_result.assert_not_called()
    persist_test.assert_not_called()
    state_machine.assert_not_called()


def test_a_master_postmerge_result_is_recorded() -> None:
    """Without this the guards below cannot be told from a reporter that does
    nothing at all."""
    test = Test({"name": "test_name"})
    result = Result()
    result.status = ResultStatus.ERROR.value

    with patch.dict("os.environ", _env("master"), clear=True), patch.object(
        Test, "persist_result_to_s3"
    ) as persist_result, patch.object(
        Test, "persist_to_s3"
    ) as persist_test, patch.object(
        Test, "update_from_s3"
    ), patch.object(
        Test, "get_test_results", return_value=[]
    ), patch(
        "ray_release.reporter.ray_test_db.ReleaseTestStateMachine"
    ) as state_machine:
        RayTestDBReporter().report_result(test, result)

    persist_result.assert_called_once_with(result)
    state_machine.assert_called_once_with(test)
    state_machine.return_value.move.assert_called_once()
    persist_test.assert_called_once()


def test_nothing_is_recorded_outside_the_postmerge_pipeline() -> None:
    test = Test({"name": "test_name"})
    result = Result()
    result.status = ResultStatus.ERROR.value

    with patch.dict(
        "os.environ", _env("master", pipeline="not-a-postmerge-pipeline"), clear=True
    ), patch.object(Test, "persist_result_to_s3") as persist_result, patch(
        "ray_release.reporter.ray_test_db.ReleaseTestStateMachine"
    ) as state_machine:
        RayTestDBReporter().report_result(test, result)

    persist_result.assert_not_called()
    state_machine.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
