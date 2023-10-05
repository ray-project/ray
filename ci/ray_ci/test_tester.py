import os
import sys
from tempfile import TemporaryDirectory
from unittest import mock

import pytest

from ci.ray_ci.tester_container import TesterContainer
from ci.ray_ci.tester import (
    _get_container,
    _get_all_test_query,
    _get_test_targets,
    _get_flaky_test_targets,
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
    _TEST_YAML = "flaky_tests: [//python/ray/tests:flaky_test_01]"

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)

        test_targets = [
            "//python/ray/tests:good_test_01",
            "//python/ray/tests:good_test_02",
            "//python/ray/tests:good_test_03",
            "//python/ray/tests:flaky_test_01",
        ]
        with mock.patch(
            "subprocess.check_output",
            return_value="\n".join(test_targets).encode("utf-8"),
        ), mock.patch(
            "ci.ray_ci.tester_container.TesterContainer.install_ray",
            return_value=None,
        ):
            assert set(
                _get_test_targets(
                    TesterContainer("core"),
                    "targets",
                    "core",
                    yaml_dir=tmp,
                )
            ) == {
                "//python/ray/tests:good_test_01",
                "//python/ray/tests:good_test_02",
                "//python/ray/tests:good_test_03",
            }

            assert _get_test_targets(
                TesterContainer("core"),
                "targets",
                "core",
                yaml_dir=tmp,
                get_flaky_tests=True,
            ) == [
                "//python/ray/tests:flaky_test_01",
            ]


def test_get_all_test_query() -> None:
    assert _get_all_test_query(["a", "b"], "core") == (
        "attr(tags, 'team:core\\\\b', tests(a) union tests(b))"
    )
    assert _get_all_test_query(["a"], "core", except_tags="tag") == (
        "attr(tags, 'team:core\\\\b', tests(a)) except (attr(tags, tag, tests(a)))"
    )
    assert _get_all_test_query(["a"], "core", only_tags="tag") == (
        "attr(tags, 'team:core\\\\b', tests(a)) intersect (attr(tags, tag, tests(a)))"
    )
    assert _get_all_test_query(["a"], "core", except_tags="tag1", only_tags="tag2") == (
        "attr(tags, 'team:core\\\\b', tests(a)) "
        "intersect (attr(tags, tag2, tests(a))) "
        "except (attr(tags, tag1, tests(a)))"
    )


def test_get_flaky_test_targets() -> None:
    _TEST_YAML = "flaky_tests: [//target]"

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)
        assert _get_flaky_test_targets("core", yaml_dir=tmp) == ["//target"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
