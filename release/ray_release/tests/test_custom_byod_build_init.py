import os
import tempfile

from ray_release.scripts.custom_byod_build_init import (
    create_custom_build_yaml,
    DEFAULT_INSTALL_COMMANDS,
)


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
            os.path.join(os.path.dirname(__file__), "custom_byod_build_expected_yaml"),
            "r",
        ) as expected:
            assert f.read() == expected.read()
