import sys
import pytest
from unittest import mock
from typing import List, Set, Dict

from ray_release.scripts.ray_bisect import _bisect, _obtain_test_result, _sanity_check
from ray_release.config import Test


def test_sanity_check():
    def _mock_run_test(
        test: Test, commit: Set[str], run_per_commit: int
    ) -> Dict[str, Dict[int, str]]:
        return {
            "passing_revision": {0: "passed", 1: "passed"},
            "failing_revision": {0: "failed", 1: "failed"},
            "flaky_revision": {0: "failed", 1: "passed"},
        }

    with mock.patch(
        "ray_release.scripts.ray_bisect._run_test",
        side_effect=_mock_run_test,
    ):
        assert _sanity_check({}, "passing_revision", "failing_revision", 2)
        assert _sanity_check({}, "passing_revision", "flaky_revision", 2)
        assert not _sanity_check({}, "failing_revision", "passing_revision", 2)
        assert not _sanity_check({}, "passing_revision", "passing_revision", 2)
        assert not _sanity_check({}, "failing_revision", "failing_revision", 2)
        assert not _sanity_check({}, "flaky_revision", "failing_revision", 2)


def test_obtain_test_result():
    test_cases = [
        {
            "c0": {0: "passed"},
        },
        {
            "c0": {0: "passed", 1: "passed"},
            "c1": {0: "hard_failed", 1: "hard_failed"},
        },
    ]

    def _mock_check_output(input: List[str]) -> str:
        commit, run = tuple(input[-1].split("-"))
        return bytes(test_case[commit][int(run)], "utf-8")

    for test_case in test_cases:
        with mock.patch(
            "subprocess.check_output",
            side_effect=_mock_check_output,
        ):
            commits = set(test_case.keys())
            rerun_per_commit = len(test_case[list(commits)[0]])
            _obtain_test_result(commits, rerun_per_commit) == test_case


def test_bisect():
    test_cases = {
        "c3": {
            "c0": {0: "passed"},
            "c1": {0: "passed"},
            "c3": {0: "hard_failed"},
            "c4": {0: "soft_failed"},
        },
        "c1": {
            "c0": {0: "passed"},
            "c1": {0: "hard_failed"},
            "c2": {0: "hard_failed"},
        },
        "cc1": {
            "cc0": {0: "passed"},
            "cc1": {0: "hard_failed"},
        },
        "c2": {
            "c0": {0: "passed", 1: "passed"},
            "c2": {0: "passed", 1: "hard_failed"},
            "c3": {0: "hard_failed", 1: "passed"},
            "c4": {0: "soft_failed", 1: "soft_failed"},
        },
    }

    for output, input in test_cases.items():

        def _mock_run_test(
            test: Test, commit: List[str], rerun_per_commit
        ) -> Dict[str, str]:
            return input

        with mock.patch(
            "ray_release.scripts.ray_bisect._run_test",
            side_effect=_mock_run_test,
        ):
            for concurreny in range(1, 4):
                assert _bisect({}, list(input.keys()), concurreny, 1) == output


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
