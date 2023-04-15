from unittest import mock
from typing import List, Dict
from ray_release.scripts.ray_bisect import _bisect
from ray_release.config import Test


def test_bisect():
    test_cases = {
        "c3": {
            "c0": "passed",
            "c1": "passed",
            "c3": "hard_failed",
            "c4": "soft_failed",
        },
        "c1": {
            "c0": "passed",
            "c1": "hard_failed",
            "c2": "hard_failed",
        },
        "cc1": {
            "cc0": "passed",
            "cc1": "hard_failed",
        },
    }

    for output, input in test_cases.items():

        def _mock_run_test(test: Test, commit: List[str]) -> Dict[str, str]:
            return input

        with mock.patch(
            "ray_release.scripts.ray_bisect._run_test",
            side_effect=_mock_run_test,
        ):
            assert _bisect("test", list(input.keys())) == output

def test_bisect():
    commit_to_test_result = {
        "c0": True,
        "c1": True,
        "c2": True,
        "c3": False,
        "c4": False,
    }

    def _mock_run_test(test_name: str, commit: str) -> bool:
        return commit_to_test_result[commit]

    with mock.patch(
        "ray_release.scripts.ray_bisect._run_test",
        side_effect=_mock_run_test,
    ):
        blamed_commit = _bisect("test", list(commit_to_test_result.keys()))
        assert blamed_commit == "c3"

def test_bisect():
    commit_to_test_result = {
        "c0": True,
        "c1": True,
        "c2": True,
        "c3": False,
        "c4": False,
    }

        def _mock_run_test(test_name: str, commit: str) -> bool:
            return input[commit]

    with mock.patch(
        "ray_release.scripts.ray_bisect._run_test",
        side_effect=_mock_run_test,
    ):
        blamed_commit = _bisect("test", list(commit_to_test_result.keys()))
        assert blamed_commit == "c3"

def test_bisect():
    commit_to_test_result = {
        "c0": True,
        "c1": True,
        "c2": True,
        "c3": False,
        "c4": False,
    }

    def _mock_run_test(test_name: str, commit: str) -> bool:
        return commit_to_test_result[commit]

    with mock.patch(
        "ray_release.scripts.ray_bisect._run_test",
        side_effect=_mock_run_test,
    ):
        blamed_commit = _bisect("test", list(commit_to_test_result.keys()))
        assert blamed_commit == "c3"

def test_bisect():
    commit_to_test_result = {
        "c0": True,
        "c1": True,
        "c2": True,
        "c3": False,
        "c4": False,
    }
    def _mock_get_commit_lists(passing_commit: str, failing_commit: str) -> List[str]:
        commits = []
        in_range = False
        for commit in commit_to_test_result:
            if commit == failing_commit:
                break
            if in_range:
                commits.append(commit)
            if commit == passing_commit:
                commits.append(commit)
                in_range = True
        return commits
    def _mock_run_test(test_name: str, commit: str) -> bool:
        return commit_to_test_result[commit]

    with mock.patch(
        "ray_release.scripts.ray_bisect._run_test",
        side_effect=_mock_run_test,
    ):
        blamed_commit = _bisect("test", list(commit_to_test_result.keys()))
        assert blamed_commit == "c3"
