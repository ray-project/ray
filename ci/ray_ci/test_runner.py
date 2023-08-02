import os
import sys
from tempfile import TemporaryDirectory
from typing import List
from unittest import mock

import pytest

from ci.ray_ci.runner import (
    _get_all_test_targets,
    _get_test_targets,
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

    with mock.patch(
        "subprocess.check_call",
        side_effect=_mock_check_call,
    ), mock.patch(
        "subprocess.check_output",
        return_value=b"-v",
    ):
        _run_tests(test_targets)


_TEST_YAML = """
flaky_tests:
  - //python/ray/tests:test_runtime_env_working_dir_3
  - //python/ray/tests:test_placement_group_3
  - //python/ray/tests:test_memory_pressure
  - //python/ray/tests:test_placement_group_5
  - //python/ray/tests:test_runtime_env_2
  - //python/ray/tests:test_gcs_fault_tolerance
  - //python/ray/tests:test_gcs_ha_e2e
  - //python/ray/tests:test_plasma_unlimited
  - //python/ray/tests:test_scheduling_performance
  - //python/ray/tests:test_object_manager
  - //python/ray/tests:test_tensorflow
  - //python/ray/tests:test_threaded_actor
  - //python/ray/tests:test_unhandled_error
"""


def test_get_test_targets() -> None:
    with TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "core.tests.yml"), "w") as f:
            f.write(_TEST_YAML)

        test_targets = [
            "//python/ray/tests:good_test_01",
            "//python/ray/tests:good_test_02",
            "//python/ray/tests:good_test_03",
            "//python/ray/tests:test_runtime_env_2",
            "",
        ]
        targets = "python/ray/tests"
        with mock.patch(
            "subprocess.check_output",
            return_value="\n".join(test_targets).encode("utf-8"),
        ):
            assert _get_all_test_targets(targets, "core", yaml_dir=tmp) == [
                "//python/ray/tests:good_test_01",
                "//python/ray/tests:good_test_02",
                "//python/ray/tests:good_test_03",
            ]
            assert _get_test_targets(targets, "core", 2, 0, yaml_dir=tmp) == [
                "//python/ray/tests:good_test_01",
                "//python/ray/tests:good_test_02",
            ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
