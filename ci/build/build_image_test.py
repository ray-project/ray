#!/usr/bin/env python3
"""Tests for build_image.py"""

import unittest

from build_image import (
    SUPPORTED_IMAGE_TYPES,
    RayImage,
    RayImageError,
)


class TestRayImageName(unittest.TestCase):
    """Test RayImage.wanda_image_name property."""

    def test_ray_cpu(self):
        ri = RayImage("ray", "3.10", "cpu")
        self.assertEqual(ri.wanda_image_name, "ray-py3.10-cpu")

    def test_ray_cuda(self):
        ri = RayImage("ray", "3.10", "cu12.8.1-cudnn")
        self.assertEqual(ri.wanda_image_name, "ray-py3.10-cu12.8.1-cudnn")

    def test_ray_extra_cpu(self):
        ri = RayImage("ray-extra", "3.10", "cpu")
        self.assertEqual(ri.wanda_image_name, "ray-extra-py3.10-cpu")

    def test_ray_llm_cuda(self):
        ri = RayImage("ray-llm", "3.11", "cu12.8.1-cudnn")
        self.assertEqual(ri.wanda_image_name, "ray-llm-py3.11-cu12.8.1-cudnn")

    def test_aarch64_suffix(self):
        ri = RayImage("ray", "3.10", "cpu", "aarch64")
        self.assertEqual(ri.wanda_image_name, "ray-py3.10-cpu-aarch64")


class TestRayImageValidation(unittest.TestCase):
    """Test RayImage.validate()."""

    def test_valid_ray(self):
        RayImage("ray", "3.10", "cpu").validate()

    def test_unknown_type(self):
        with self.assertRaises(RayImageError):
            RayImage("bad-type", "3.10", "cpu").validate()

    def test_invalid_python(self):
        with self.assertRaises(RayImageError):
            RayImage("ray-llm", "3.10", "cu12.8.1-cudnn").validate()

    def test_invalid_platform(self):
        with self.assertRaises(RayImageError):
            RayImage("ray", "3.10", "nonexistent").validate()

    def test_invalid_architecture(self):
        with self.assertRaises(RayImageError):
            RayImage("ray-llm", "3.11", "cu12.8.1-cudnn", "aarch64").validate()


class TestGetWandaSpecPath(unittest.TestCase):
    """Test RayImage.get_wanda_spec_path()."""

    def _spec(self, image_type, platform):
        return RayImage(
            image_type=image_type, python_version="3.10", platform=platform
        ).get_wanda_spec_path()

    def test_ray_cpu(self):
        self.assertEqual(self._spec("ray", "cpu"), "ci/docker/ray-image-cpu.wanda.yaml")

    def test_ray_cuda(self):
        self.assertEqual(
            self._spec("ray", "cu12.8.1-cudnn"),
            "ci/docker/ray-image-cuda.wanda.yaml",
        )

    def test_ray_extra_cpu(self):
        self.assertEqual(
            self._spec("ray-extra", "cpu"),
            "ci/docker/ray-extra-image-cpu.wanda.yaml",
        )

    def test_ray_extra_cuda(self):
        self.assertEqual(
            self._spec("ray-extra", "cu12.1.1-cudnn8"),
            "ci/docker/ray-extra-image-cuda.wanda.yaml",
        )

    def test_ray_llm_cuda(self):
        self.assertEqual(
            self._spec("ray-llm", "cu12.8.1-cudnn"),
            "ci/docker/ray-llm-image-cuda.wanda.yaml",
        )

    def test_ray_llm_extra_cuda(self):
        self.assertEqual(
            self._spec("ray-llm-extra", "cu12.8.1-cudnn"),
            "ci/docker/ray-llm-extra-image-cuda.wanda.yaml",
        )

    def test_ray_llm_cpu_raises(self):
        with self.assertRaises(RayImageError):
            self._spec("ray-llm", "cpu")

    def test_ray_tpu(self):
        self.assertEqual(
            self._spec("ray", "tpu"),
            "ci/docker/ray-image-tpu.wanda.yaml",
        )


class TestSupportedImageTypes(unittest.TestCase):
    """Test SUPPORTED_IMAGE_TYPES covers all expected types."""

    def test_contains_all_types(self):
        for name in ("ray", "ray-extra", "ray-llm", "ray-llm-extra"):
            self.assertIn(name, SUPPORTED_IMAGE_TYPES)

    def test_all_plain_strings(self):
        for name in SUPPORTED_IMAGE_TYPES:
            self.assertIsInstance(name, str)
            self.assertNotIn(".", name)  # not "RayType.RAY"


if __name__ == "__main__":
    unittest.main()
