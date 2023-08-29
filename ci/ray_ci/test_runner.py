import os
import sys
from tempfile import TemporaryDirectory
from unittest import mock
from typing import List

import pytest

from ci.ray_ci.runner import (
    _get_all_test_targets,
    _get_all_test_query,
    _get_test_targets,
    _get_flaky_test_targets,
)
from ci.ray_ci.utils import chunk_into_n


def test_get_test_targets() -> None:
    def _mock_shard_tests(tests: List[str], workers: int, worker_id: int) -> List[str]:
        return chunk_into_n(tests, workers)[worker_id]

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
            "ci.ray_ci.runner.shard_tests", side_effect=_mock_shard_tests
        ), mock.patch(
            "subprocess.check_output",
            return_value="\n".join(test_targets).encode("utf-8"),
        ):
            assert _get_all_test_targets("targets", "core", "core", yaml_dir=tmp) == [
                "//python/ray/tests:good_test_01",
                "//python/ray/tests:good_test_02",
                "//python/ray/tests:good_test_03",
            ]
            assert _get_test_targets("targets", "core", "core", 2, 0, yaml_dir=tmp) == [
                "//python/ray/tests:good_test_01",
                "//python/ray/tests:good_test_02",
            ]


def test_get_all_test_query() -> None:
    assert _get_all_test_query(["a", "b"], "core", "") == (
        "attr(tags, 'team:core\\\\b', tests(a) union tests(b))"
    )
    assert _get_all_test_query(["a"], "core", "tag") == (
        "attr(tags, 'team:core\\\\b', tests(a)) except (attr(tags, tag, tests(a)))"
    )


def test_get_flaky_test_targets() -> None:
    _TEST_YAML = "flaky_tests: [//target]"

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)
        assert _get_flaky_test_targets("core", yaml_dir=tmp) == ["//target"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
