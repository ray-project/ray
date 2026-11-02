import sys
from contextlib import contextmanager
from typing import Optional
from unittest.mock import patch

import pytest

from ray_release.bazel import bazel_runfile
from ray_release.configs.global_config import get_global_config, init_global_config
from ray_release.reporter.ray_test_db import RayTestDBReporter
from ray_release.result import Result, ResultStatus
from ray_release.test import Test

init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))

_POSTMERGE_PIPELINE = get_global_config()["ci_pipeline_postmerge"][0]


def _env(branch: Optional[str], pipeline: str = _POSTMERGE_PIPELINE) -> dict:
    """None omits the key, so `unset` means unset rather than empty."""
    env = {"BUILDKITE_PIPELINE_ID": pipeline}
    if branch is not None:
        env["BUILDKITE_BRANCH"] = branch
    return env


def _failed_result() -> Result:
    result = Result()
    result.status = ResultStatus.ERROR.value
    return result


@contextmanager
def _sinks():
    """Patch every s3 path report_result touches, not just the ones asserted.

    An unpatched one turns a regression into a real boto3 call and an opaque
    timeout, rather than the assertion failure the test is there to produce.
    """
    with patch.object(Test, "persist_result_to_s3") as persist_result, patch.object(
        Test, "persist_to_s3"
    ) as persist_test, patch.object(Test, "update_from_s3"), patch.object(
        Test, "get_test_results", return_value=[]
    ), patch(
        "ray_release.reporter.ray_test_db.ReleaseTestStateMachine"
    ) as state_machine:
        yield persist_result, persist_test, state_machine


@pytest.mark.parametrize(
    "branch",
    ["releases/2.58.0", "releases/1.0.0", "sai-miduthuri/some-branch", None],
    ids=["release_branch", "old_release_branch", "feature_branch", "unset"],
)
def test_nothing_is_recorded_off_master(branch) -> None:
    """Enabling the agent on release branches is only safe because of this.

    The guard holds independently of REPORT_TO_RAY_TEST_DB ever being set.
    """
    with patch.dict("os.environ", _env(branch), clear=True), _sinks() as (
        persist_result,
        persist_test,
        state_machine,
    ):
        RayTestDBReporter().report_result(Test({"name": "test_name"}), _failed_result())

    persist_result.assert_not_called()
    persist_test.assert_not_called()
    state_machine.assert_not_called()


def test_nothing_is_recorded_outside_the_postmerge_pipeline() -> None:
    with patch.dict(
        "os.environ", _env("master", pipeline="not-a-postmerge-pipeline"), clear=True
    ), _sinks() as (persist_result, persist_test, state_machine):
        RayTestDBReporter().report_result(Test({"name": "test_name"}), _failed_result())

    persist_result.assert_not_called()
    persist_test.assert_not_called()
    state_machine.assert_not_called()


def test_a_master_postmerge_result_is_recorded() -> None:
    """Without this the guards above cannot be told from a reporter that does
    nothing at all."""
    test = Test({"name": "test_name"})
    result = _failed_result()

    with patch.dict("os.environ", _env("master"), clear=True), _sinks() as (
        persist_result,
        persist_test,
        state_machine,
    ):
        RayTestDBReporter().report_result(test, result)

    persist_result.assert_called_once_with(result)
    state_machine.assert_called_once_with(test)
    state_machine.return_value.move.assert_called_once()
    persist_test.assert_called_once()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
