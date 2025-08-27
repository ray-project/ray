import os
import tempfile
import sys
import pytest

from ray_release.custom_byod_build_init_helper import create_custom_build_yaml
from ray_release.configs.global_config import init_global_config
from ray_release.bazel import bazel_runfile


init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))


def test_create_custom_build_yaml():
    custom_byod_images = [
        (
            "ray-project/ray-ml:abc123-custom",
            "ray-project/ray-ml:abc123-base",
            "custom_script.sh",
        ),
        ("ray-project/ray-ml:abc123-custom", "ray-project/ray-ml:abc123-base", ""),
        (
            "ray-project/ray-ml:nightly-py37-cpu-custom-abcdef123456789abc123456789",
            "ray-project/ray-ml:nightly-py37-cpu-base",
            "custom_script.sh",
        ),  # longer than 40 chars
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        create_custom_build_yaml(
            os.path.join(tmpdir, "custom_byod_build.rayci.yml"), custom_byod_images
        )
        with open(os.path.join(tmpdir, "custom_byod_build.rayci.yml"), "r") as f, open(
            os.path.join(os.path.dirname(__file__), "custom_byod_build_expected.yaml"),
            "r",
        ) as expected:
            assert f.read() == expected.read()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
