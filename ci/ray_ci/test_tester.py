import os
import sys
from typing import List
from tempfile import TemporaryDirectory
from unittest import mock

import pytest

from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.tester import (
    _get_container,
    _get_test_by_tag_query,
    _get_tests,
    _get_flaky_tests,
    TestType,
)


def test_get_container() -> None:
    with mock.patch(
        "ci.ray_ci.tester_container.TesterContainer.install_ray",
        return_value=None,
    ):
        container = _get_container("core", 3, 1, 2)
        assert container.docker_tag == "corebuild"
        assert container.shard_count == 6
        assert container.shard_ids == [2, 3]


def test_get_test_targets() -> None:
    _TEST_YAML = "flaky_tests: [//python/ray/tests/team:core_flaky_test_01]"

    def _mock_get_tests_by_tag(
        _: TesterContainer,
        targets: List[str],
        tag: str,
    ) -> List[str]:
        return [target for target in targets if tag.replace("\\\\b", "") in target]

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)

        test_targets = [
            "//python/ray/tests/team:core_xcommit_test",
            "//python/ray/tests/team:core_manual_test",
            "//python/ray/tests/team:core_debug_tests",
            "//python/ray/tests/team:core_asan_tests",
            "//python/ray/tests/team:core_test",
            "//python/ray/tests/team:data_test",
            "//python/ray/tests/team:core_flaky_test_01",
        ]
        with mock.patch(
            "ci.ray_ci.tester._get_tests_by_tag",
            side_effect=_mock_get_tests_by_tag,
        ), mock.patch(
            "ci.ray_ci.tester_container.TesterContainer.install_ray",
            return_value=None,
        ):
            # get debug tests
            assert _get_tests(
                TesterContainer("core"),
                test_targets,
                "core",
                test_type=TestType.DEBUG.value,
                yaml_dir=tmp,
            ) == ["//python/ray/tests/team:core_debug_tests"]

            # get normal tests
            assert _get_tests(
                TesterContainer("core"),
                test_targets,
                "core",
                yaml_dir=tmp,
            ) == ["//python/ray/tests/team:core_test"]


def test_get_test_by_tag_query() -> None:
    team = "core"
    assert _get_test_by_tag_query(["a", "b"], f"team:{team}\\\\b") == (
        "attr(tags, 'team:core\\\\b', tests(a) union tests(b))"
    )


def test_get_flaky_test_targets() -> None:
    _TEST_YAML = "flaky_tests: [//target]"

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)
        assert _get_flaky_tests("core", yaml_dir=tmp) == ["//target"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
