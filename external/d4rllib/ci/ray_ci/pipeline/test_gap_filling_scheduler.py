import sys
from unittest import mock

import pytest

from ci.ray_ci.pipeline.gap_filling_scheduler import GapFillingScheduler, BLOCK_STEP_KEY


@mock.patch(
    "ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._get_latest_commits"
)
@mock.patch("ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._get_builds")
def test_get_latest_commit_for_build_state(mock_get_builds, mock_get_latest_commits):
    mock_get_builds.return_value = [
        {
            "state": "failed",
            "commit": "000",
        },
        {
            "state": "passed",
            "commit": "111",
        },
        {
            "state": "failed",
            "commit": "222",
        },
    ]
    mock_get_latest_commits.return_value = [
        "222",
        "111",
        "000",
        "/ray",
    ]
    scheduler = GapFillingScheduler("org", "pipeline", "token", "/ray")
    assert scheduler._get_latest_commit_for_build_state("failed") == "222"
    assert scheduler._get_latest_commit_for_build_state("passed") == "111"
    assert scheduler._get_latest_commit_for_build_state("something") is None


@mock.patch("subprocess.check_output")
@mock.patch(
    "ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._get_latest_commits"
)
@mock.patch("ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._get_builds")
def test_get_gap_commits(mock_get_builds, mock_get_latest_commits, mock_check_output):
    def _mock_check_output_side_effect(cmd: str, cwd: str) -> str:
        assert " ".join(cmd) == "git rev-list --reverse ^111 444~"
        return b"222\n333\n"

    mock_get_builds.return_value = [
        {
            "state": "passed",
            "commit": "111",
        },
        {
            "state": "failed",
            "commit": "444",
        },
    ]
    mock_get_latest_commits.return_value = [
        "444",
        "111",
    ]
    mock_check_output.side_effect = _mock_check_output_side_effect
    scheduler = GapFillingScheduler("org", "pipeline", "token", "/ray")
    assert scheduler.get_gap_commits() == ["222", "333"]


@mock.patch(
    "ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._trigger_build"
)
@mock.patch(
    "ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler.get_gap_commits"
)
def test_run(mock_get_gap_commits, mock_trigger_build):
    scheduler = GapFillingScheduler("org", "pipeline", "token", "/ray")

    # no builds are triggered
    mock_get_gap_commits.return_value = []
    scheduler.run()
    mock_trigger_build.assert_not_called()

    # builds are triggered
    mock_get_gap_commits.return_value = ["222", "333"]
    scheduler.run()
    mock_trigger_build.assert_has_calls(
        [
            mock.call("222"),
            mock.call("333"),
        ],
    )


@mock.patch("ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._get_builds")
def test_find_blocked_build_and_job(mock_get_builds):
    scheduler = GapFillingScheduler("org", "pipeline", "token", "/ray")

    mock_get_builds.return_value = [
        # build 100 is blocked on job 3
        {
            "state": "blocked",
            "number": "100",
            "commit": "hi",
            "jobs": [
                {"id": "1"},
                {"id": "2", "step_key": "not_block"},
                {"id": "3", "step_key": BLOCK_STEP_KEY},
            ],
        },
        # step is blocked but build is not blocked
        {
            "state": "passed",
            "number": "200",
            "commit": "w00t",
            "jobs": [
                {"id": "1"},
                {"id": "3", "step_key": BLOCK_STEP_KEY},
            ],
        },
        # build is blocked but step is not blocked
        {
            "state": "blocked",
            "number": "200",
            "commit": "bar",
            "jobs": [
                {"id": "1"},
            ],
        },
    ]
    scheduler._find_blocked_build_and_job("hi") == (100, 3)
    scheduler._find_blocked_build_and_job("w00t") == (None, None)
    scheduler._find_blocked_build_and_job("bar") == (None, None)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
