import sys
import os
from unittest import mock

import pytest
from unittest.mock import patch

from ray_release.bazel import bazel_runfile
from ray_release.configs.global_config import (
    init_global_config,
    get_global_config,
)
from ray_release.test import (
    Test,
    _convert_env_list_to_dict,
    DATAPLANE_ECR_REPO,
    DATAPLANE_ECR_ML_REPO,
)


init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))


def _stub_test(val: dict) -> Test:
    test = Test(
        {
            "name": "test",
            "cluster": {},
        }
    )
    test.update(val)
    return test


def test_is_byod_cluster():
    assert not _stub_test({}).is_byod_cluster()
    assert _stub_test({"cluster": {"byod": {}}}).is_byod_cluster()
    assert _stub_test({"cluster": {"byod": {"type": "gpu"}}}).is_byod_cluster()
    with mock.patch.dict(os.environ, {"BUILDKITE_PULL_REQUEST": "1"}):
        assert not _stub_test({"cluster": {"byod": {}}}).is_byod_cluster()
    with mock.patch.dict(os.environ, {"BUILDKITE_PULL_REQUEST": "false"}):
        assert _stub_test({"cluster": {"byod": {}}}).is_byod_cluster()


def test_convert_env_list_to_dict():
    with mock.patch.dict(os.environ, {"ENV": "env"}):
        assert _convert_env_list_to_dict(["a=b", "c=d=e", "ENV"]) == {
            "a": "b",
            "c": "d=e",
            "ENV": "env",
        }


def test_get_python_version():
    assert _stub_test({}).get_python_version() == "3.8"
    assert _stub_test({"python": "3.9"}).get_python_version() == "3.9"


def test_get_ray_image():
    os.environ["BUILDKITE_BRANCH"] = "master"
    os.environ["BUILDKITE_COMMIT"] = "1234567890"
    assert (
        _stub_test(
            {
                "python": "3.9",
                "cluster": {"byod": {}},
            }
        ).get_ray_image()
        == "rayproject/ray:123456-py39-cpu"
    )
    assert (
        _stub_test(
            {
                "python": "3.9",
                "cluster": {
                    "byod": {
                        "type": "gpu",
                    }
                },
            }
        ).get_ray_image()
        == "rayproject/ray-ml:123456-py39-gpu"
    )
    os.environ["BUILDKITE_BRANCH"] = "releases/1.0.0"
    assert (
        _stub_test({"cluster": {"byod": {}}}).get_ray_image()
        == "rayproject/ray:1.0.0.123456-py38-cpu"
    )


def test_get_anyscale_byod_image():
    os.environ["BUILDKITE_BRANCH"] = "master"
    os.environ["BUILDKITE_COMMIT"] = "1234567890"
    assert (
        _stub_test({"python": "3.7", "cluster": {"byod": {}}}).get_anyscale_byod_image()
        == f"{get_global_config()['byod_ecr']}/{DATAPLANE_ECR_REPO}:123456-py37-cpu"
    )
    assert _stub_test(
        {
            "python": "3.8",
            "cluster": {
                "byod": {
                    "type": "gpu",
                }
            },
        }
    ).get_anyscale_byod_image() == (
        f"{get_global_config()['byod_ecr']}/" f"{DATAPLANE_ECR_ML_REPO}:123456-py38-gpu"
    )
    assert _stub_test(
        {
            "python": "3.8",
            "cluster": {
                "byod": {
                    "type": "gpu",
                    "post_build_script": "foo.sh",
                }
            },
        }
    ).get_anyscale_byod_image() == (
        f"{get_global_config()['byod_ecr']}"
        f"/{DATAPLANE_ECR_ML_REPO}:123456-py38-gpu-"
        "ab7ed2b7a7e8d3f855a7925b0d296b0f9c75fac91882aba47854d92d27e13e53"
    )


@patch("github.Repository")
@patch("github.Issue")
def test_is_jailed_with_open_issue(mock_repo, mock_issue) -> None:
    assert not Test(state="passing").is_jailed_with_open_issue(mock_repo)
    mock_repo.get_issue.return_value = mock_issue
    mock_issue.state = "open"
    assert Test(state="jailed", github_issue_number="1").is_jailed_with_open_issue(
        mock_repo
    )
    mock_issue.state = "closed"
    assert not Test(state="jailed", github_issue_number="1").is_jailed_with_open_issue(
        mock_repo
    )


def test_is_stable() -> None:
    assert Test().is_stable()
    assert Test(stable=True).is_stable()
    assert not Test(stable=False).is_stable()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
