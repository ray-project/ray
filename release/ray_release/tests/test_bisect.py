from unittest import mock
from ray_release.scripts.ray_bisect import _bisect


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
