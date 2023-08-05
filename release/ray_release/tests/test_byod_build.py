import sys

import pytest
from unittest.mock import patch
from typing import List

from ray_release.bazel import bazel_runfile
from ray_release.configs.global_config import init_global_config, get_global_config
from ray_release.test import Test
from ray_release.byod.build import (
    build_anyscale_custom_byod_image,
    build_anyscale_base_byod_images,
    build_champagne_image,
    DATAPLANE_FILENAME,
)


def test_build_anyscale_champagne_image() -> None:
    cmds = []

    def _mock_check_call(
        cmd: List[str],
        *args,
        **kwargs,
    ) -> None:
        cmds.append(cmd)

    with patch.dict(
        "os.environ",
        {"BUILDKITE_COMMIT": "abc123", "BUILDKITE_BRANCH": "master"},
    ), patch(
        "ray_release.byod.build._download_dataplane_build_file",
        return_value=None,
    ), patch(
        "subprocess.check_call",
        side_effect=_mock_check_call,
    ), patch(
        "subprocess.check_output",
        return_value=b"abc123",
    ), open(
        DATAPLANE_FILENAME, "wb"
    ) as _:
        build_champagne_image("2.5.1", "py37", "cpu")
        assert "docker build --build-arg BASE_IMAGE=rayproject/ray:2.5.1-py37 -t "
        "029272617770.dkr.ecr.us-west-2.amazonaws.com/"
        "anyscale/ray:champagne-2.5.1 -" == " ".join(cmds[0])


init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))


def test_build_anyscale_custom_byod_image() -> None:
    cmds = []

    def _mock_check_call(
        cmd: List[str],
        *args,
        **kwargs,
    ) -> None:
        cmds.append(cmd)

    with patch(
        "ray_release.byod.build._byod_image_exist", return_value=False
    ), patch.dict(
        "os.environ",
        {"BUILDKITE_COMMIT": "abc123", "BUILDKITE_BRANCH": "master"},
    ), patch(
        "subprocess.check_call",
        side_effect=_mock_check_call,
    ), patch(
        "subprocess.check_output",
        return_value=b"abc123",
    ):
        test = Test(
            name="name",
            cluster={"byod": {"post_build_script": "foo.sh"}},
        )
        build_anyscale_custom_byod_image(test)
        assert "docker build --build-arg BASE_IMAGE=029272617770.dkr.ecr.us-west-2."
        "amazonaws.com/anyscale/ray:abc123-py37 -t 029272617770.dkr.ecr.us-west-2."
        "amazonaws.com/anyscale/ray:abc123-py37-c3fc5fc6d84cea4d7ab885c6cdc966542e"
        "f59e4c679b8c970f2f77b956bfd8fb" in " ".join(cmds[0])


def test_build_anyscale_base_byod_images() -> None:
    images = []

    def _mock_validate_and_push(image: str) -> None:
        images.append(image)

    with patch("ray_release.byod.build_ray.build_ray", return_value=None), patch(
        "ray_release.byod.build._download_dataplane_build_file", return_value=None
    ), patch(
        "os.environ",
        {"BUILDKITE_COMMIT": "abc123", "BUILDKITE_BRANCH": "master"},
    ), patch(
        "subprocess.check_call", return_value=None
    ), patch(
        "ray_release.byod.build._byod_image_exist", return_value=False
    ), patch(
        "ray_release.byod.build._ray_image_exist", return_value=True
    ), patch(
        "ray_release.byod.build._validate_and_push", side_effect=_mock_validate_and_push
    ):
        tests = [
            Test(name="aws", env="aws", cluster={"byod": {}}),
            Test(name="gce", env="gce", cluster={"byod": {}}),
        ]
        build_anyscale_base_byod_images(tests)
        assert images == [
            f"{get_global_config()['byod_aws_cr']}/anyscale/ray:abc123-py38-cpu",
            f"{get_global_config()['byod_gcp_cr']}/anyscale/ray:abc123-py38-cpu",
        ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
