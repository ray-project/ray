import os
import sys
from tempfile import TemporaryDirectory
from unittest import mock

import pytest

from ci.ray_ci.runner import (
    _get_all_test_targets,
    _get_all_test_query,
    _get_test_targets,
    _get_flaky_test_targets,
)


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
        "attr(tags, xcommit, tests(a) union tests(b)))"
    )


def test_get_flaky_test_targets() -> None:
    _TEST_YAML = "flaky_tests: [//target]"

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)
        assert _get_flaky_test_targets("core", yaml_dir=tmp) == ["//target"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
