import os
import sys
from tempfile import TemporaryDirectory
from typing import List
from unittest import mock

import pytest

from ci.ray_ci.runner import (
    _get_all_test_targets,
    _get_all_test_query,
    _get_test_targets,
    _get_flaky_test_targets,
    _run_tests,
    _chunk_into_n,
)


def test_chunk_into_n() -> None:
    assert _chunk_into_n([1, 2, 3, 4, 5], 2) == [[1, 2, 3], [4, 5]]
    assert _chunk_into_n([1, 2], 3) == [[1], [2], []]
    assert _chunk_into_n([1, 2], 1) == [[1, 2]]


def test_run_tests() -> None:
    test_targets = [":target_01", ":target_02"]

    def _mock_check_call(input: List[str]) -> None:
        for test_target in test_targets:
            assert test_target in input

    with mock.patch("subprocess.check_call", side_effect=_mock_check_call), mock.patch(
        "subprocess.check_output",
        return_value=b"-v",
    ):
        _run_tests(test_targets)


def test_get_test_targets() -> None:
    _TEST_YAML = "flaky_tests: [//python/ray/tests:flaky_test_01]"

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)

        test_targets = [
            "//python/ray/tests:good_test_01",
            "//python/ray/tests:good_test_02",
            "//python/ray/tests:good_test_03",
            "//python/ray/tests:flaky_test_01",
            "",
        ]
        with mock.patch(
            "subprocess.check_output",
            return_value="\n".join(test_targets).encode("utf-8"),
        ):
            assert _get_all_test_targets("targets", "core", "small", yaml_dir=tmp) == [
                "//python/ray/tests:good_test_01",
                "//python/ray/tests:good_test_02",
                "//python/ray/tests:good_test_03",
            ]
            assert _get_test_targets(
                "targets", "core", 2, 0, "small", yaml_dir=tmp
            ) == [
                "//python/ray/tests:good_test_01",
                "//python/ray/tests:good_test_02",
            ]


def test_get_all_test_query() -> None:
    assert _get_all_test_query(["a", "b"], "core", "small,medium") == (
        "(attr(tags, team:core, tests(a) union tests(b)) intersect "
        "(attr(size, small, tests(a) union tests(b)) union "
        "attr(size, medium, tests(a) union tests(b)))) except "
        "(attr(tags, debug_tests, tests(a) union tests(b)) union "
        "attr(tags, asan_tests, tests(a) union tests(b)) union "
        "attr(tags, ray_ha, tests(a) union tests(b)))"
    )


def test_get_flaky_test_targets() -> None:
    _TEST_YAML = "flaky_tests: [//target]"

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)
        assert _get_flaky_test_targets("core", yaml_dir=tmp) == ["//target"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
