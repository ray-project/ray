from unittest import mock
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
