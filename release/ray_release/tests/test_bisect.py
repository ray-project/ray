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
            for concurreny in range(1, 4):
                assert _bisect({}, list(input.keys()), concurreny) == output
