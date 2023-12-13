import os
import re
import sys
from tempfile import TemporaryDirectory
from unittest import mock

import pytest

from ci.ray_ci.linux_tester_container import LinuxTesterContainer
from ci.ray_ci.windows_tester_container import WindowsTesterContainer
from ci.ray_ci.tester import (
    _add_default_except_tags,
    _get_container,
    _get_all_test_query,
    _get_test_targets,
    _get_flaky_test_targets,
    _get_tag_matcher,
)


def test_get_tag_matcher() -> None:
    assert re.match(
        # simulate shell character escaping
        bytes(_get_tag_matcher("tag"), "utf-8").decode("unicode_escape"),
        "tag",
    )
    assert not re.match(
        # simulate shell character escaping
        bytes(_get_tag_matcher("tag"), "utf-8").decode("unicode_escape"),
        "atagb",
    )


def test_get_container() -> None:
    with mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer.install_ray",
        return_value=None,
    ), mock.patch(
        "ci.ray_ci.windows_tester_container.WindowsTesterContainer.install_ray",
        return_value=None,
    ):
        container = _get_container(
            team="core",
            operating_system="linux",
            workers=3,
            worker_id=1,
            parallelism_per_worker=2,
            gpus=0,
        )
        assert isinstance(container, LinuxTesterContainer)
        assert container.docker_tag == "corebuild"
        assert container.shard_count == 6
        assert container.shard_ids == [2, 3]

        container = _get_container(
            team="serve",
            operating_system="windows",
            workers=3,
            worker_id=1,
            parallelism_per_worker=2,
            gpus=0,
        )
        assert isinstance(container, WindowsTesterContainer)


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
            "ci.ray_ci.linux_tester_container.LinuxTesterContainer.install_ray",
            return_value=None,
        ):
            assert set(
                _get_test_targets(
                    LinuxTesterContainer("core"),
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
                LinuxTesterContainer("core"),
                "targets",
                "core",
                yaml_dir=tmp,
                get_flaky_tests=True,
            ) == [
                "//python/ray/tests:flaky_test_01",
            ]


def test_add_default_except_tags() -> None:
    assert set(_add_default_except_tags("tag1,tag2").split(",")) == {
        "tag1",
        "tag2",
        "manual",
    }
    assert _add_default_except_tags("") == "manual"
    assert _add_default_except_tags("manual") == "manual"


def test_get_all_test_query() -> None:
    assert _get_all_test_query(["a", "b"], "core") == (
        "attr(tags, '\\\\bteam:core\\\\b', tests(a) union tests(b))"
    )
    assert _get_all_test_query(["a"], "core", except_tags="tag") == (
        "attr(tags, '\\\\bteam:core\\\\b', tests(a)) "
        "except (attr(tags, '\\\\btag\\\\b', tests(a)))"
    )
    assert _get_all_test_query(["a"], "core", only_tags="tag") == (
        "attr(tags, '\\\\bteam:core\\\\b', tests(a)) "
        "intersect (attr(tags, '\\\\btag\\\\b', tests(a)))"
    )
    assert _get_all_test_query(["a"], "core", except_tags="tag1", only_tags="tag2") == (
        "attr(tags, '\\\\bteam:core\\\\b', tests(a)) "
        "intersect (attr(tags, '\\\\btag2\\\\b', tests(a))) "
        "except (attr(tags, '\\\\btag1\\\\b', tests(a)))"
    )


def test_get_flaky_test_targets() -> None:
    _TEST_YAML = "flaky_tests: [//target]"

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)
        assert _get_flaky_test_targets("core", yaml_dir=tmp) == ["//target"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
