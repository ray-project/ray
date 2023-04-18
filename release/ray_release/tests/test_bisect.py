from unittest import mock
from typing import List
from ray_release.scripts.ray_bisect import _bisect


def test_bisect():
    test_cases = {
        "c3": {
            "c0": True,
            "c1": True,
            "c3": False,
            "c4": False,
        },
        "c1": {
            "c0": True,
            "c1": False,
        },
    }

    for output, input in test_cases.items():

        def _mock_run_test(test_name: str, commit: str) -> bool:
            return input[commit]

        with mock.patch(
            "ray_release.scripts.ray_bisect._run_test",
            side_effect=_mock_run_test,
        ), mock.patch(
            "ray_release.scripts.ray_bisect._get_test",
            return_value={},
        ):
            assert _bisect("test", list(input.keys())) == output

def test_bisect():
    commit_to_test_result = {
      'c0': True, 
      'c1': True, 
      'c2': True, 
      'c3': False, 
      'c4': False, 
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
        return commit_to_test_result[commit]

    with mock.patch(
        "ray_release.scripts.ray_bisect._run_test",
        side_effect=_mock_run_test,
    ):
        blamed_commit = _bisect("test", list(commit_to_test_result.keys()))
        assert blamed_commit == "c3"

def test_bisect():
    commit_to_test_result = {
        "c0": "passed",
        "c1": "passed",
        "c2": "passed",
        "c3": "hard_failed",
        "c4": "soft_failed",
    }

    def _mock_run_test(test_name: str, commit: List[str]) -> dict[str, str]:
        return commit_to_test_result

    with mock.patch(
        "ray_release.scripts.ray_bisect._run_test",
        side_effect=_mock_run_test,
    ):
        blamed_commit = _bisect({}, list(commit_to_test_result.keys()))
        assert blamed_commit == "c3"
