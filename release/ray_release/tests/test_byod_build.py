import os
import sys
import tempfile
from typing import List
from unittest.mock import patch

import pytest

from ray_release.bazel import bazel_runfile
from ray_release.byod.build import (
    _get_ray_commit,
    build_anyscale_base_byod_images,
    build_anyscale_custom_byod_image,
)
from ray_release.byod.build_context import BuildContext
from ray_release.configs.global_config import get_global_config, init_global_config
from ray_release.test import Test


def test_get_ray_commit() -> None:
    assert (
        _get_ray_commit(
            {
                "RAY_WANT_COMMIT_IN_IMAGE": "abc123",
                "COMMIT_TO_TEST": "def456",
                "BUILDKITE_COMMIT": "987789",
            }
        )
        == "abc123"
    )

    assert (
        _get_ray_commit(
            {
                "COMMIT_TO_TEST": "def456",
                "BUILDKITE_COMMIT": "987789",
            }
        )
        == "def456"
    )
    assert _get_ray_commit({"BUILDKITE_COMMIT": "987789"}) == "987789"
    assert _get_ray_commit({"PATH": "/usr/bin"}) == ""


init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))


def test_build_anyscale_custom_byod_image() -> None:
    cmds = []

    def _mock_check_call(
        cmd: List[str],
        *args,
        **kwargs,
    ) -> None:
        cmds.append(cmd)

    with patch("ray_release.byod.build._image_exist", return_value=False), patch.dict(
        "os.environ",
        {
            "BUILDKITE_COMMIT": "abc123",
            "RAYCI_BUILD_ID": "a1b2c3d4",
        },
    ), patch("subprocess.check_call", side_effect=_mock_check_call,), patch(
        "subprocess.check_output",
        return_value=b"abc123",
    ):
        test = Test(
            name="name",
            cluster={"byod": {"post_build_script": "foo.sh"}},
        )
        with tempfile.TemporaryDirectory() as byod_dir:
            with open(os.path.join(byod_dir, "foo.sh"), "wt") as f:
                f.write("echo foo")

            build_context: BuildContext = {
                "post_build_script": test.get_byod_post_build_script(),
            }
            build_anyscale_custom_byod_image(
                test.get_anyscale_byod_image(),
                test.get_anyscale_base_byod_image(),
                build_context,
                release_byod_dir=byod_dir,
            )
        assert (" ".join(cmds[0])).startswith(
            "docker build --progress=plain . "
            "--build-arg BASE_IMAGE=029272617770.dkr.ecr.us-west-2."
            "amazonaws.com/anyscale/ray:a1b2c3d4-py310-cpu "
            "-t 029272617770.dkr.ecr.us-west-2."
            "amazonaws.com/anyscale/ray:a1b2c3d4-py310-cpu-"
        )


def test_build_anyscale_base_byod_images() -> None:
    def _mock_image_exist(image: str) -> bool:
        return True

    with patch(
        "os.environ",
        {
            "BUILDKITE_COMMIT": "abc123",
            "RAYCI_BUILD_ID": "a1b2c3d4",
        },
    ), patch("subprocess.check_call", return_value=None), patch(
        "ray_release.byod.build._image_exist", side_effect=_mock_image_exist
    ):
        tests = [
            Test(name="aws", env="aws", cluster={"byod": {}}),
            Test(name="aws", env="aws", cluster={"byod": {"type": "gpu"}}),
            Test(
                # This is a duplicate of the default.
                name="aws",
                env="aws",
                python="3.10",
                cluster={"byod": {"type": "cpu"}},
            ),
            Test(name="aws", env="aws", python="3.11", cluster={"byod": {}}),
            Test(name="aws", env="aws", cluster={"byod": {"type": "cu121"}}),
            Test(
                name="aws",
                env="aws",
                python="3.10",
                cluster={"byod": {"type": "cu116"}},
            ),
            Test(
                name="aws",
                env="aws",
                python="3.11",
                cluster={"byod": {"type": "cu118"}},
            ),
            Test(name="gce", env="gce", cluster={"byod": {}}),
        ]
        images = build_anyscale_base_byod_images(tests)
        global_config = get_global_config()
        aws_cr = global_config["byod_aws_cr"]
        # Base images are always from AWS ECR (see get_anyscale_base_byod_image),
        # so GCE and AWS CPU tests resolve to the same image (6 unique, not 7).
        assert set(images) == {
            f"{aws_cr}/anyscale/ray:a1b2c3d4-py310-cpu",
            f"{aws_cr}/anyscale/ray:a1b2c3d4-py310-cu116",
            f"{aws_cr}/anyscale/ray:a1b2c3d4-py310-cu121",
            f"{aws_cr}/anyscale/ray:a1b2c3d4-py311-cu118",
            f"{aws_cr}/anyscale/ray-ml:a1b2c3d4-py310-gpu",
            f"{aws_cr}/anyscale/ray:a1b2c3d4-py311-cpu",
        }


def test_get_anyscale_base_byod_image_always_uses_aws_ecr() -> None:
    """Base images should always come from AWS ECR, regardless of test env."""
    global_config = get_global_config()
    aws_cr = global_config["byod_aws_cr"]

    with patch.dict(
        "os.environ",
        {
            "BUILDKITE_COMMIT": "abc123",
            "RAYCI_BUILD_ID": "a1b2c3d4",
        },
    ):
        expected_cpu = f"{aws_cr}/anyscale/ray:a1b2c3d4-py310-cpu"
        for env in ("aws", "gce", "azure", "kuberay"):
            test = Test(name="t", env=env, cluster={"byod": {}})
            assert test.get_anyscale_base_byod_image() == expected_cpu, env

        gpu_test = Test(name="t", env="gce", cluster={"byod": {"type": "gpu"}})
        assert gpu_test.get_anyscale_base_byod_image() == (
            f"{aws_cr}/anyscale/ray-ml:a1b2c3d4-py310-gpu"
        )

        # When ray_version is set, base image uses anyscale/ray prefix
        # instead of byod_aws_cr.
        versioned_cpu_test = Test(
            name="t",
            env="gce",
            cluster={"byod": {}, "ray_version": "2.50.0"},
        )
        versioned_gpu_test = Test(
            name="t",
            env="gce",
            cluster={"byod": {"type": "gpu"}, "ray_version": "2.50.0"},
        )
        assert versioned_cpu_test.get_anyscale_base_byod_image() == (
            "anyscale/ray:2.50.0-py310-cpu"
        )
        assert versioned_gpu_test.get_anyscale_base_byod_image() == (
            "anyscale/ray:2.50.0-py310-cu121"
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
