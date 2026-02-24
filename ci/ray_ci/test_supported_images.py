"""
Validates that ray-images.yaml is well-formed and internally consistent.
"""

import pytest

from ci.ray_ci.supported_images import get_image_config, load_supported_images

IMAGE_TYPES = list(load_supported_images().keys())
REQUIRED_KEYS = ["defaults", "python", "platforms", "architectures"]
REQUIRED_DEFAULTS = ["python", "gpu_platform", "architecture"]


class TestRayImagesSchema:
    def test_has_image_types(self):
        assert len(IMAGE_TYPES) > 0, "ray-images.yaml has no image types defined"

    @pytest.mark.parametrize("image_type", IMAGE_TYPES)
    def test_required_keys(self, image_type):
        cfg = get_image_config(image_type)
        for key in REQUIRED_KEYS:
            assert key in cfg, f"{image_type}: missing required key '{key}'"

    @pytest.mark.parametrize("image_type", IMAGE_TYPES)
    def test_required_defaults(self, image_type):
        defaults = get_image_config(image_type)["defaults"]
        for key in REQUIRED_DEFAULTS:
            assert key in defaults, f"{image_type}: missing required default '{key}'"

    @pytest.mark.parametrize("image_type", IMAGE_TYPES)
    def test_defaults_in_supported(self, image_type):
        cfg = get_image_config(image_type)
        defaults = cfg["defaults"]

        assert defaults["python"] in cfg["python"], (
            f"{image_type}: default python '{defaults['python']}' "
            f"not in supported {cfg['python']}"
        )
        assert defaults["gpu_platform"] in cfg["platforms"], (
            f"{image_type}: default gpu_platform '{defaults['gpu_platform']}' "
            f"not in supported {cfg['platforms']}"
        )
        assert defaults["architecture"] in cfg["architectures"], (
            f"{image_type}: default architecture '{defaults['architecture']}' "
            f"not in supported {cfg['architectures']}"
        )

    @pytest.mark.parametrize("image_type", IMAGE_TYPES)
    def test_no_empty_lists(self, image_type):
        cfg = get_image_config(image_type)
        for key in ["python", "platforms", "architectures"]:
            assert len(cfg[key]) > 0, f"{image_type}: '{key}' list is empty"

    @pytest.mark.parametrize("image_type", IMAGE_TYPES)
    def test_python_versions_are_strings(self, image_type):
        for v in get_image_config(image_type)["python"]:
            assert isinstance(v, str), (
                f"{image_type}: python version {v!r} is {type(v).__name__}, "
                f"not str (missing quotes in YAML?)"
            )

    @pytest.mark.parametrize("image_type", IMAGE_TYPES)
    def test_platforms_are_strings(self, image_type):
        for v in get_image_config(image_type)["platforms"]:
            assert isinstance(
                v, str
            ), f"{image_type}: platform {v!r} is {type(v).__name__}, not str"

    @pytest.mark.parametrize("image_type", IMAGE_TYPES)
    def test_architectures_are_strings(self, image_type):
        for v in get_image_config(image_type)["architectures"]:
            assert isinstance(
                v, str
            ), f"{image_type}: architecture {v!r} is {type(v).__name__}, not str"
