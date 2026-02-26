#!/usr/bin/env python3
"""Tests for build_image.py"""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from build_image import (
    REGISTRY_PREFIX,
    SUPPORTED_IMAGE_TYPES,
    BuildError,
    ImageBuildConfig,
    RayImage,
    main,
)


class TestGetWandaSpec(unittest.TestCase):
    """Test wanda_spec_path returns correct paths."""

    def _spec(self, image_type, platform):
        ri = RayImage(image_type=image_type, python_version="3.10", platform=platform)
        return ImageBuildConfig(ray_image=ri, ray_root=Path("/fake")).wanda_spec_path

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
        with self.assertRaises(BuildError):
            self._spec("ray-llm", "cpu")


class TestValidation(unittest.TestCase):
    """Test from_args() rejects invalid combinations."""

    def test_unknown_build_image_type(self):
        with self.assertRaises(BuildError) as ctx:
            ImageBuildConfig.from_args("ray-ml", "3.10", "cpu")
        self.assertIn("Unknown image type", str(ctx.exception))

    def test_invalid_python_raises_build_error(self):
        with self.assertRaises(BuildError):
            ImageBuildConfig.from_args("ray-llm", "3.10", "cu12.8.1-cudnn")


def _config(**kwargs):
    image_type = kwargs.pop("image_type", "ray")
    python_version = kwargs.pop("python_version", "3.10")
    image_platform = kwargs.pop("platform", "cpu")
    architecture = kwargs.pop("architecture", "x86_64")
    kwargs.pop("arch_suffix", None)

    ray_image = RayImage(
        image_type=image_type,
        python_version=python_version,
        platform=image_platform,
        architecture=architecture,
    )

    defaults = dict(
        ray_image=ray_image,
        ray_root=Path("/fake/ray"),
    )
    defaults.update(kwargs)
    return ImageBuildConfig(**defaults)


class TestConfigWandaImageTag(unittest.TestCase):
    """Test wanda_image_name and wanda_image_tag on ImageBuildConfig."""

    def test_ray_cpu(self):
        c = _config()
        self.assertEqual(c.ray_image.wanda_image_name, "ray-py3.10-cpu")
        self.assertEqual(c.wanda_image_tag, f"{REGISTRY_PREFIX}ray-py3.10-cpu")

    def test_ray_cuda(self):
        c = _config(platform="cu12.8.1-cudnn")
        self.assertEqual(c.ray_image.wanda_image_name, "ray-py3.10-cu12.8.1-cudnn")

    def test_ray_extra_cpu(self):
        c = _config(image_type="ray-extra")
        self.assertEqual(c.ray_image.wanda_image_name, "ray-extra-py3.10-cpu")

    def test_ray_llm_cuda(self):
        c = _config(
            image_type="ray-llm", python_version="3.11", platform="cu12.8.1-cudnn"
        )
        self.assertEqual(c.ray_image.wanda_image_name, "ray-llm-py3.11-cu12.8.1-cudnn")

    def test_aarch64_suffix(self):
        c = _config(architecture="aarch64")
        self.assertEqual(c.ray_image.wanda_image_name, "ray-py3.10-cpu-aarch64")


class TestConfigNightlyAlias(unittest.TestCase):
    """Test nightly_alias on ImageBuildConfig."""

    def test_ray_default_has_nightly(self):
        c = _config()
        self.assertEqual(c.nightly_alias, f"{REGISTRY_PREFIX}ray:nightly")

    def test_ray_extra_default_has_nightly_extra(self):
        c = _config(image_type="ray-extra")
        self.assertEqual(c.nightly_alias, f"{REGISTRY_PREFIX}ray:nightly-extra")

    def test_ray_llm_default_has_nightly(self):
        c = _config(
            image_type="ray-llm", python_version="3.11", platform="cu12.8.1-cudnn"
        )
        self.assertEqual(c.nightly_alias, f"{REGISTRY_PREFIX}ray-llm:nightly")

    def test_ray_llm_extra_default_has_nightly_extra(self):
        c = _config(
            image_type="ray-llm-extra",
            python_version="3.11",
            platform="cu12.8.1-cudnn",
        )
        self.assertEqual(c.nightly_alias, f"{REGISTRY_PREFIX}ray-llm:nightly-extra")

    def test_non_default_python_no_alias(self):
        c = _config(python_version="3.12")
        self.assertIsNone(c.nightly_alias)

    def test_non_default_platform_no_alias(self):
        c = _config(platform="cu12.8.1-cudnn")
        self.assertIsNone(c.nightly_alias)


def _make_ray_root(tmpdir):
    """Create a minimal ray root with config files for property tests."""
    root = Path(tmpdir)
    (root / ".rayciversion").write_text("0.31.0")
    (root / "rayci.env").write_text(
        "MANYLINUX_VERSION=260128.221a193\nRAY_VERSION=3.0.0.dev0\n"
    )
    return root


class TestBuildEnv(unittest.TestCase):
    """Test build_env returns correct environment variables."""

    def _config(self, **kwargs):
        kwargs.setdefault("ray_root", self._ray_root)
        return _config(**kwargs)

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._ray_root = _make_ray_root(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_cpu_env(self):
        env = self._config().build_env
        self.assertEqual(env["PYTHON_VERSION"], "3.10")
        self.assertEqual(env["HOSTTYPE"], "x86_64")
        self.assertEqual(env["ARCH_SUFFIX"], "")
        self.assertEqual(env["IS_LOCAL_BUILD"], "true")
        self.assertEqual(env["IMAGE_TYPE"], "ray")
        self.assertEqual(env["RAY_VERSION"], "3.0.0.dev0")
        self.assertNotIn("CUDA_VERSION", env)

    def test_cuda_env(self):
        env = self._config(platform="cu12.8.1-cudnn").build_env
        self.assertEqual(env["CUDA_VERSION"], "12.8.1-cudnn")

    def test_ray_extra_image_type(self):
        env = self._config(image_type="ray-extra").build_env
        self.assertEqual(env["IMAGE_TYPE"], "ray")

    def test_ray_llm_image_type(self):
        env = self._config(
            image_type="ray-llm", python_version="3.11", platform="cu12.8.1-cudnn"
        ).build_env
        self.assertEqual(env["IMAGE_TYPE"], "ray-llm")

    def test_ray_llm_extra_image_type(self):
        env = self._config(
            image_type="ray-llm-extra",
            python_version="3.11",
            platform="cu12.8.1-cudnn",
        ).build_env
        self.assertEqual(env["IMAGE_TYPE"], "ray-llm")


class TestPlatformDetection(unittest.TestCase):
    """Test _detect_host_arch() returns correct values."""

    def test_darwin_arm64(self):
        with mock.patch(
            "ci.build.build_common._platform.system", return_value="Darwin"
        ), mock.patch("ci.build.build_common._platform.machine", return_value="arm64"):
            arch = ImageBuildConfig._detect_host_arch()
        self.assertEqual(arch, "aarch64")

    def test_linux_x86_64(self):
        with mock.patch(
            "ci.build.build_common._platform.system", return_value="Linux"
        ), mock.patch("ci.build.build_common._platform.machine", return_value="x86_64"):
            arch = ImageBuildConfig._detect_host_arch()
        self.assertEqual(arch, "x86_64")

    def test_linux_aarch64(self):
        with mock.patch(
            "ci.build.build_common._platform.system", return_value="Linux"
        ), mock.patch(
            "ci.build.build_common._platform.machine", return_value="aarch64"
        ):
            arch = ImageBuildConfig._detect_host_arch()
        self.assertEqual(arch, "aarch64")

    def test_unsupported_platform_raises(self):
        with mock.patch(
            "ci.build.build_common._platform.system", return_value="Windows"
        ), mock.patch("ci.build.build_common._platform.machine", return_value="AMD64"):
            with self.assertRaises(BuildError):
                ImageBuildConfig._detect_host_arch()


class TestFromArgs(unittest.TestCase):
    """Test ImageBuildConfig.from_args() factory method."""

    def test_from_args_creates_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ray_root = _make_ray_root(tmpdir)

            with mock.patch(
                "ci.build.build_common._platform.system", return_value="Linux"
            ), mock.patch(
                "ci.build.build_common._platform.machine", return_value="x86_64"
            ), mock.patch(
                "build_image.find_ray_root", return_value=ray_root
            ), mock.patch(
                "build_image.get_git_commit", return_value="abc1234"
            ):
                config = ImageBuildConfig.from_args("ray", "3.10", "cpu")

            self.assertEqual(config.ray_image.image_type, "ray")
            self.assertEqual(config.ray_image.python_version, "3.10")
            self.assertEqual(config.ray_image.platform, "cpu")
            self.assertEqual(config.ray_image.architecture, "x86_64")
            self.assertEqual(config.ray_image.arch_suffix, "")
            self.assertEqual(config.raymake_version, "0.31.0")
            self.assertEqual(config.manylinux_version, "260128.221a193")
            self.assertEqual(config.ray_version, "3.0.0.dev0")

    def test_from_args_rejects_invalid(self):
        with self.assertRaises(BuildError):
            ImageBuildConfig.from_args("ray-llm", "3.10", "cpu")

    def test_from_args_rejects_unsupported_arch(self):
        """ray-llm only supports x86_64; aarch64 should be rejected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            ray_root = _make_ray_root(tmpdir)
            with mock.patch(
                "ci.build.build_common._platform.system", return_value="Linux"
            ), mock.patch(
                "ci.build.build_common._platform.machine", return_value="aarch64"
            ), mock.patch(
                "build_image.find_ray_root", return_value=ray_root
            ):
                with self.assertRaises(BuildError):
                    ImageBuildConfig.from_args("ray-llm", "3.11", "cu12.8.1-cudnn")


class TestSupportedImageTypes(unittest.TestCase):
    """Test SUPPORTED_IMAGE_TYPES covers all expected types."""

    def test_contains_all_types(self):
        for name in ("ray", "ray-extra", "ray-llm", "ray-llm-extra"):
            self.assertIn(name, SUPPORTED_IMAGE_TYPES)

    def test_all_plain_strings(self):
        for name in SUPPORTED_IMAGE_TYPES:
            self.assertIsInstance(name, str)
            self.assertNotIn(".", name)  # not "RayType.RAY"


class TestHelpOutput(unittest.TestCase):
    """Test that --help prints without error."""

    def test_no_args_prints_help_and_exits_zero(self):
        with mock.patch("build_image.sys.argv", ["build_image"]):
            with self.assertRaises(SystemExit) as ctx:
                main()
        self.assertEqual(ctx.exception.code, 0)


if __name__ == "__main__":
    unittest.main()
