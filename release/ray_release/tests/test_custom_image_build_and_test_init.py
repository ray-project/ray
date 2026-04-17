import json
import os
import sys
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from ray_release.buildkite.filter import filter_tests
from ray_release.buildkite.settings import get_frequency, get_test_filters
from ray_release.config import read_and_validate_release_test_collection
from ray_release.configs.global_config import init_global_config
from ray_release.custom_byod_build_init_helper import (
    build_short_gpu_map,
    collect_rayci_select_keys,
)
from ray_release.scripts.custom_image_build_and_test_init import main

_bazel_workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")


def _expected_rayci_select_keys(sample_yaml: str) -> set:
    """Mirror the script's filter + key computation so expectations can't drift."""
    init_global_config(
        os.path.join(
            _bazel_workspace_dir,
            "release/ray_release/configs/oss_config.yaml",
        )
    )
    gpu_map = build_short_gpu_map(os.path.join(_bazel_workspace_dir, "ray-images.json"))
    test_collection = read_and_validate_release_test_collection(
        [os.path.join(_bazel_workspace_dir, sample_yaml)]
    )
    filtered = filter_tests(
        test_collection,
        frequency=get_frequency("nightly"),
        test_filters=get_test_filters("prefix:hello_world"),
        run_jailed_tests=True,
        run_unstable_tests=True,
    )
    tests = [test for test, _ in filtered]
    return collect_rayci_select_keys(tests, gpu_map)


@patch.dict("os.environ", {"BUILDKITE": "1"})
@patch.dict("os.environ", {"RAYCI_BUILD_ID": "a1b2c3d4"})
@patch("ray_release.test.Test.update_from_s3", return_value=None)
@patch("ray_release.test.Test.is_jailed_with_open_issue", return_value=False)
def test_custom_image_build_and_test_init(
    mock_update_from_s3, mock_is_jailed_with_open_issue
):
    runner = CliRunner()
    custom_build_jobs_output_file = "custom_build_jobs.yaml"
    test_jobs_output_file = "test_jobs.json"
    rayci_select_output_file = "rayci_select.txt"
    result = runner.invoke(
        main,
        [
            "--test-collection-file",
            "release/ray_release/tests/sample_tests.yaml",
            "--global-config",
            "oss_config.yaml",
            "--frequency",
            "nightly",
            "--run-jailed-tests",
            "--run-unstable-tests",
            "--test-filters",
            "prefix:hello_world",
            "--custom-build-jobs-output-file",
            custom_build_jobs_output_file,
            "--test-jobs-output-file",
            test_jobs_output_file,
            "--rayci-select-output-file",
            rayci_select_output_file,
        ],
        catch_exceptions=False,
    )
    with open(
        os.path.join(_bazel_workspace_dir, custom_build_jobs_output_file), "r"
    ) as f:
        custom_build_jobs = yaml.safe_load(f)
        assert len(custom_build_jobs["steps"]) == 1  # 1 custom build job
    with open(os.path.join(_bazel_workspace_dir, test_jobs_output_file), "r") as f:
        test_jobs = json.load(f)
        assert len(test_jobs) == 1  # 1 group
        assert len(test_jobs[0]["steps"]) == 2  # 2 tests
        assert test_jobs[0]["steps"][0]["label"].startswith("hello_world.aws")
        assert test_jobs[0]["steps"][1]["label"].startswith("hello_world_custom.aws")

    with open(os.path.join(_bazel_workspace_dir, rayci_select_output_file), "r") as f:
        raw = f.read()
        keys = [k for k in raw.split(",") if k]
        expected = _expected_rayci_select_keys(
            "release/ray_release/tests/sample_tests.yaml"
        )
        assert set(keys) == expected
        assert len(keys) == len(set(keys))  # no duplicates
        assert keys == sorted(keys)  # stable ordering
        assert len(expected) == 3  # cpu base + gpu base + custom-BYOD

    assert result.exit_code == 0


@patch.dict("os.environ", {"BUILDKITE": "1"})
@patch.dict("os.environ", {"RAYCI_BUILD_ID": "a1b2c3d4"})
@patch("ray_release.test.Test.update_from_s3", return_value=None)
@patch("ray_release.test.Test.is_jailed_with_open_issue", return_value=False)
def test_custom_image_build_and_test_init_with_block_step(
    mock_update_from_s3, mock_is_jailed_with_open_issue
):
    num_tests_expected = 5
    runner = CliRunner()
    custom_build_jobs_output_file = "custom_build_jobs.yaml"
    test_jobs_output_file = "test_jobs.json"
    rayci_select_output_file = "rayci_select.txt"
    result = runner.invoke(
        main,
        [
            "--test-collection-file",
            "release/ray_release/tests/sample_5_tests.yaml",
            "--global-config",
            "oss_config.yaml",
            "--frequency",
            "nightly",
            "--run-jailed-tests",
            "--run-unstable-tests",
            "--test-filters",
            "prefix:hello_world",
            "--custom-build-jobs-output-file",
            custom_build_jobs_output_file,
            "--test-jobs-output-file",
            test_jobs_output_file,
            "--rayci-select-output-file",
            rayci_select_output_file,
        ],
        catch_exceptions=False,
    )
    with open(
        os.path.join(_bazel_workspace_dir, custom_build_jobs_output_file), "r"
    ) as f:
        custom_build_jobs = yaml.safe_load(f)
        assert len(custom_build_jobs["steps"]) == 1  # 1 custom build job
    with open(os.path.join(_bazel_workspace_dir, test_jobs_output_file), "r") as f:
        test_jobs = json.load(f)
        print(test_jobs)
        assert len(test_jobs) == 2  # 2 groups: block and hello_world
        assert len(test_jobs[0]["steps"]) == 1  # 1 block step
        assert test_jobs[0]["steps"][0]["block"] == "Run release tests"
        assert test_jobs[0]["steps"][0]["key"] == "block_run_release_tests"
        assert (
            test_jobs[0]["steps"][0]["prompt"]
            == f"You are triggering {num_tests_expected} tests. Do you want to proceed?"
        )
        assert len(test_jobs[1]["steps"]) == num_tests_expected  # 5 tests
        assert test_jobs[1]["steps"][0]["label"].startswith("hello_world.aws")
        assert test_jobs[1]["steps"][1]["label"].startswith("hello_world_custom.aws")

    with open(os.path.join(_bazel_workspace_dir, rayci_select_output_file), "r") as f:
        raw = f.read()
        keys = [k for k in raw.split(",") if k]
        expected = _expected_rayci_select_keys(
            "release/ray_release/tests/sample_5_tests.yaml"
        )
        assert set(keys) == expected
        assert len(keys) == len(set(keys))  # no duplicates
        assert keys == sorted(keys)  # stable ordering
        assert len(expected) == 3  # cpu base + gpu base + custom-BYOD (collapsed)

    assert result.exit_code == 0


@patch.dict("os.environ", {"AUTOMATIC": "1"})
@patch.dict("os.environ", {"BUILDKITE": "1"})
@patch.dict("os.environ", {"RAYCI_BUILD_ID": "a1b2c3d4"})
@patch("ray_release.test.Test.update_from_s3", return_value=None)
@patch("ray_release.test.Test.is_jailed_with_open_issue", return_value=False)
def test_custom_image_build_and_test_init_without_block_step_automatic(
    mock_update_from_s3, mock_is_jailed_with_open_issue
):
    num_tests_expected = 5
    runner = CliRunner()
    custom_build_jobs_output_file = "custom_build_jobs.yaml"
    test_jobs_output_file = "test_jobs.json"
    result = runner.invoke(
        main,
        [
            "--test-collection-file",
            "release/ray_release/tests/sample_5_tests.yaml",
            "--global-config",
            "oss_config.yaml",
            "--frequency",
            "nightly",
            "--run-jailed-tests",
            "--run-unstable-tests",
            "--test-filters",
            "prefix:hello_world",
            "--custom-build-jobs-output-file",
            custom_build_jobs_output_file,
            "--test-jobs-output-file",
            test_jobs_output_file,
        ],
        catch_exceptions=False,
    )
    with open(
        os.path.join(_bazel_workspace_dir, custom_build_jobs_output_file), "r"
    ) as f:
        custom_build_jobs = yaml.safe_load(f)
        assert len(custom_build_jobs["steps"]) == 1  # 1 custom build job
    with open(os.path.join(_bazel_workspace_dir, test_jobs_output_file), "r") as f:
        test_jobs = json.load(f)
        print(test_jobs)
        assert len(test_jobs) == 1  # 1 group: hello_world
        assert len(test_jobs[0]["steps"]) == num_tests_expected  # 5 tests
        assert test_jobs[0]["steps"][0]["label"].startswith("hello_world.aws")
        assert test_jobs[0]["steps"][1]["label"].startswith("hello_world_custom.aws")

    assert result.exit_code == 0


@patch.dict("os.environ", {"BUILDKITE": "1"})
@patch.dict("os.environ", {"RAYCI_BUILD_ID": "a1b2c3d4"})
@patch("ray_release.test.Test.update_from_s3", return_value=None)
@patch("ray_release.test.Test.is_jailed_with_open_issue", return_value=False)
def test_rayci_select_skipped_when_no_filter(
    mock_update_from_s3, mock_is_jailed_with_open_issue
):
    """Unfiltered runs (e.g. full nightly) skip RAYCI_SELECT so rayci runs everything."""
    runner = CliRunner()
    custom_build_jobs_output_file = "custom_build_jobs.yaml"
    test_jobs_output_file = "test_jobs.json"
    rayci_select_output_file = "rayci_select_no_filter.txt"
    abs_path = os.path.join(_bazel_workspace_dir, rayci_select_output_file)
    if os.path.exists(abs_path):
        os.remove(abs_path)
    result = runner.invoke(
        main,
        [
            "--test-collection-file",
            "release/ray_release/tests/sample_tests.yaml",
            "--global-config",
            "oss_config.yaml",
            "--frequency",
            "nightly",
            "--run-jailed-tests",
            "--run-unstable-tests",
            "--custom-build-jobs-output-file",
            custom_build_jobs_output_file,
            "--test-jobs-output-file",
            test_jobs_output_file,
            "--rayci-select-output-file",
            rayci_select_output_file,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert not os.path.exists(abs_path)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
