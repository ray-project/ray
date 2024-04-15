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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
