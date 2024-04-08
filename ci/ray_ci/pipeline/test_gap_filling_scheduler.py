import sys
from unittest import mock

import pytest

from ci.ray_ci.pipeline.gap_filling_scheduler import GapFillingScheduler


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


@mock.patch("subprocess.check_output")
@mock.patch(
    "ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._get_latest_commits"
)
@mock.patch("ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._get_builds")
def test_get_gap_commits(mock_get_builds, mock_get_latest_commits, mock_check_output):
    def _mock_check_output_side_effect(cmd: str, shell: bool) -> str:
        assert cmd == "git rev-list --reverse ^111 444~"
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
    scheduler = GapFillingScheduler("org", "pipeline", "token")
    assert scheduler.get_gap_commits() == ["222", "333"]


@mock.patch(
    "ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._trigger_build"
)
@mock.patch(
    "ci.ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler.get_gap_commits"
)
def test_run(mock_get_gap_commits, mock_trigger_build):
    mock_get_gap_commits.return_value = ["222", "333"]
    scheduler = GapFillingScheduler("org", "pipeline", "token")
    scheduler.run()
    mock_trigger_build.assert_has_calls(
        [
            mock.call("222"),
            mock.call("333"),
        ]
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
