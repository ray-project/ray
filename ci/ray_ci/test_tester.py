import os
import re
import sys
from tempfile import TemporaryDirectory
from unittest import mock

import pytest

from ci.ray_ci.linux_tester_container import LinuxTesterContainer
from ci.ray_ci.tester import (
    _add_default_except_tags,
    _get_all_test_query,
    _get_container,
    _get_flaky_test_targets,
    _get_new_tests,
    _get_tag_matcher,
    _get_test_targets,
)
from ci.ray_ci.windows_tester_container import WindowsTesterContainer

from ray_release.test import Test, TestState


def _stub_test(val: dict) -> Test:
    test = Test(
        {
            "name": "test",
            "cluster": {},
        }
    )
    test.update(val)
    return test


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


def test_linux_privileged() -> None:
    with mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer.install_ray",
        return_value=None,
    ):
        container = _get_container(
            team="core",
            operating_system="linux",
            workers=3,
            worker_id=1,
            parallelism_per_worker=2,
            network=None,
            gpus=0,
            tmp_filesystem=None,
            privileged=True,
        )
        assert (
            container.privileged
            and "--privileged" in container.get_run_command_extra_args()
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
            network=None,
            gpus=0,
            tmp_filesystem=None,
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
            network=None,
            gpus=0,
        )
        assert isinstance(container, WindowsTesterContainer)


def test_get_empty_test_targets() -> None:
    with mock.patch(
        "subprocess.check_output",
        return_value="\n".encode("utf-8"),
    ), mock.patch(
        "ci.ray_ci.linux_tester_container.LinuxTesterContainer.install_ray",
        return_value=None,
    ), mock.patch(
        "ray_release.test.Test.gen_from_s3",
        return_value=set(),
    ), mock.patch(
        "ci.ray_ci.tester._get_new_tests",
        return_value=set(),
    ), mock.patch(
        "ray_release.test.Test.gen_microcheck_tests",
        return_value=set(),
    ):
        # Test that the set of test target is empty, rather than a set of empty string
        assert (
            set(
                _get_test_targets(
                    LinuxTesterContainer("core"),
                    "targets",
                    "core",
                    operating_system="linux",
                )
            )
            == set()
        )


def test_get_test_targets() -> None:
    _TEST_YAML = "flaky_tests: [//python/ray/tests:flaky_test_01]"

    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)

        test_targets = [
            "//python/ray/tests:high_impact_test_01",
            "//python/ray/tests:good_test_01",
            "//python/ray/tests:good_test_02",
            "//python/ray/tests:good_test_03",
            "//python/ray/tests:flaky_test_01",
            "//python/ray/tests:flaky_test_02",
        ]
        test_objects = [
            _stub_test(
                {
                    "name": "linux://python/ray/tests:high_impact_test_01",
                    "team": "core",
                    "state": TestState.PASSING,
                    Test.KEY_IS_HIGH_IMPACT: "true",
                }
            ),
            _stub_test(
                {
                    "name": "linux://python/ray/tests:flaky_test_01",
                    "team": "core",
                    "state": TestState.FLAKY,
                    Test.KEY_IS_HIGH_IMPACT: "true",
                }
            ),
            _stub_test(
                {
                    "name": "linux://python/ray/tests:flaky_test_02",
                    "team": "core",
                    "state": TestState.FLAKY,
                    Test.KEY_IS_HIGH_IMPACT: "true",
                }
            ),
        ]
        with mock.patch(
            "subprocess.check_output",
            return_value="\n".join(test_targets).encode("utf-8"),
        ), mock.patch(
            "ci.ray_ci.linux_tester_container.LinuxTesterContainer.install_ray",
            return_value=None,
        ), mock.patch(
            "ray_release.test.Test.gen_from_s3",
            return_value=test_objects,
        ), mock.patch(
            "ci.ray_ci.tester._get_new_tests",
            return_value=set(),
        ), mock.patch(
            "ray_release.test.Test.gen_microcheck_tests",
            return_value={test.get_target() for test in test_objects},
        ):
            assert set(
                _get_test_targets(
                    LinuxTesterContainer("core"),
                    "targets",
                    "core",
                    operating_system="linux",
                    yaml_dir=tmp,
                )
            ) == {
                "//python/ray/tests:high_impact_test_01",
                "//python/ray/tests:good_test_01",
                "//python/ray/tests:good_test_02",
                "//python/ray/tests:good_test_03",
            }

            assert set(
                _get_test_targets(
                    LinuxTesterContainer("core"),
                    "targets",
                    "core",
                    operating_system="linux",
                    yaml_dir=tmp,
                    lookup_test_database=False,
                )
            ) == {
                "//python/ray/tests:high_impact_test_01",
                "//python/ray/tests:good_test_01",
                "//python/ray/tests:good_test_02",
                "//python/ray/tests:good_test_03",
                "//python/ray/tests:flaky_test_02",
            }

            assert _get_test_targets(
                LinuxTesterContainer("core"),
                "targets",
                "core",
                operating_system="linux",
                yaml_dir=tmp,
                get_flaky_tests=True,
            ) == [
                "//python/ray/tests:flaky_test_01",
                "//python/ray/tests:flaky_test_02",
            ]

            assert _get_test_targets(
                LinuxTesterContainer("core"),
                "targets",
                "core",
                operating_system="linux",
                yaml_dir=tmp,
                get_flaky_tests=False,
                get_high_impact_tests=True,
            ) == [
                "//python/ray/tests:high_impact_test_01",
            ]

            assert _get_test_targets(
                LinuxTesterContainer("core"),
                "targets",
                "core",
                operating_system="linux",
                yaml_dir=tmp,
                get_flaky_tests=True,
                get_high_impact_tests=True,
            ) == [
                "//python/ray/tests:flaky_test_01",
                "//python/ray/tests:flaky_test_02",
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


@mock.patch("ci.ray_ci.tester_container.TesterContainer.run_script_with_output")
@mock.patch("ray_release.test.Test.gen_from_s3")
def test_get_new_tests(mock_gen_from_s3, mock_run_script_with_output) -> None:
    mock_gen_from_s3.return_value = [
        _stub_test({"name": "linux://old_test_01"}),
        _stub_test({"name": "linux://old_test_02"}),
    ]
    mock_run_script_with_output.return_value = "//old_test_01\n//new_test"
    assert _get_new_tests(
        "linux", LinuxTesterContainer("test", skip_ray_installation=True)
    ) == {"//new_test"}


def test_get_flaky_test_targets() -> None:
    test_harness = [
        {
            "input": {
                "core_test_yaml": "flaky_tests: [//t1, windows://t2]",
                "s3": [
                    _stub_test(
                        {
                            "name": "windows://t1_s3",
                            "team": "core",
                            "state": TestState.FLAKY,
                        }
                    ),
                    _stub_test(
                        {
                            "name": "linux://t2_s3",
                            "team": "ci",
                            "state": TestState.FLAKY,
                        }
                    ),
                    _stub_test(
                        {
                            "name": "linux://t3_s3",
                            "team": "core",
                            "state": TestState.FLAKY,
                        }
                    ),
                ],
            },
            "output": {
                "linux": ["//t1", "//t3_s3"],
                "windows": ["//t1_s3", "//t2"],
            },
        },
        {
            "input": {
                "core_test_yaml": "flaky_tests: [//t1, windows://t2]",
                "s3": [],
            },
            "output": {
                "linux": ["//t1"],
                "windows": ["//t2"],
            },
        },
        {
            "input": {
                "core_test_yaml": "flaky_tests: []",
                "s3": [
                    _stub_test(
                        {
                            "name": "windows://t1_s3",
                            "team": "core",
                            "state": TestState.FLAKY,
                        }
                    ),
                    _stub_test(
                        {
                            "name": "linux://t2_s3",
                            "team": "ci",
                            "state": TestState.FLAKY,
                        }
                    ),
                    _stub_test(
                        {
                            "name": "linux://t3_s3",
                            "team": "core",
                            "state": TestState.FLAKY,
                        }
                    ),
                    _stub_test(
                        {
                            "name": "linux://t4_s3",
                            "team": "core",
                            "state": TestState.PASSING,
                        }
                    ),
                ],
            },
            "output": {
                "linux": ["//t3_s3"],
                "windows": ["//t1_s3"],
            },
        },
        {
            "input": {
                "core_test_yaml": "flaky_tests: []",
                "s3": [],
            },
            "output": {
                "linux": [],
                "windows": [],
            },
        },
    ]
    for test in test_harness:
        with TemporaryDirectory() as tmp, mock.patch(
            "ray_release.test.Test.gen_from_s3",
            return_value=test["input"]["s3"],
        ):
            with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
                f.write(test["input"]["core_test_yaml"])
            for os_name in ["linux", "windows"]:
                assert (
                    _get_flaky_test_targets(
                        "core",
                        os_name,
                        yaml_dir=tmp,
                        lookup_test_database=True,
                    )
                    == test["output"][os_name]
                )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
