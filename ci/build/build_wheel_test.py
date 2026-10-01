#!/usr/bin/env python3
"""Tests for build_wheel.py"""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from build_wheel import (
    BuildConfig,
    BuildError,
    main,
)


class TestBuildConfigPlatformDetection(unittest.TestCase):
    """Test _detect_platform() returns correct values for supported platforms."""

    def test_darwin_arm64(self):
        with mock.patch(
            "ci.build.build_common._platform.system", return_value="Darwin"
        ), mock.patch("ci.build.build_common._platform.machine", return_value="arm64"):
            hosttype, arch_suffix = BuildConfig._detect_platform()
        self.assertEqual((hosttype, arch_suffix), ("aarch64", "-aarch64"))

    def test_linux_x86_64(self):
        with mock.patch(
            "ci.build.build_common._platform.system", return_value="Linux"
        ), mock.patch("ci.build.build_common._platform.machine", return_value="x86_64"):
            hosttype, arch_suffix = BuildConfig._detect_platform()
        self.assertEqual((hosttype, arch_suffix), ("x86_64", ""))

    def test_linux_aarch64(self):
        with mock.patch(
            "ci.build.build_common._platform.system", return_value="Linux"
        ), mock.patch(
            "ci.build.build_common._platform.machine", return_value="aarch64"
        ):
            hosttype, arch_suffix = BuildConfig._detect_platform()
        self.assertEqual((hosttype, arch_suffix), ("aarch64", "-aarch64"))

    def test_unsupported_platform_raises(self):
        with mock.patch(
            "ci.build.build_common._platform.system", return_value="Windows"
        ), mock.patch("ci.build.build_common._platform.machine", return_value="AMD64"):
            with self.assertRaises(BuildError) as ctx:
                BuildConfig._detect_platform()
        self.assertIn("Unsupported platform", str(ctx.exception))


class TestBuildConfigProperties(unittest.TestCase):
    """Test computed properties of BuildConfig."""

    def setUp(self):
        self.config = BuildConfig(
            python_version="3.11",
            output_dir=Path(".whl"),
            ray_root=Path("/fake/ray"),
            hosttype="x86_64",
            arch_suffix="",
            raymake_version="0.28.0",
            manylinux_version="260128.221a193",
            commit="abc1234",
        )

    def test_build_env_contains_required_vars(self):
        env = self.config.build_env
        self.assertEqual(env["PYTHON_VERSION"], "3.11")
        self.assertEqual(env["MANYLINUX_VERSION"], "260128.221a193")
        self.assertEqual(env["HOSTTYPE"], "x86_64")
        self.assertEqual(env["ARCH_SUFFIX"], "")
        self.assertEqual(env["BUILDKITE_COMMIT"], "abc1234")
        self.assertEqual(env["IS_LOCAL_BUILD"], "true")


class TestBuildConfigFromEnv(unittest.TestCase):
    """Test BuildConfig.from_env() factory method."""

    def test_from_env_creates_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ray_root = Path(tmpdir)
            (ray_root / ".rayciversion").write_text("0.28.0")
            (ray_root / "rayci.env").write_text('MANYLINUX_VERSION="260128.221a193"')

            with mock.patch(
                "ci.build.build_common._platform.system", return_value="Linux"
            ), mock.patch(
                "ci.build.build_common._platform.machine", return_value="x86_64"
            ), mock.patch(
                "build_wheel.find_ray_root", return_value=ray_root
            ), mock.patch(
                "build_wheel.get_git_commit", return_value="abc1234"
            ):
                config = BuildConfig.from_env("3.11", ".whl")

            self.assertEqual(config.python_version, "3.11")
            self.assertTrue(config.output_dir.is_absolute())
            self.assertEqual(config.output_dir.name, ".whl")
            self.assertEqual(config.hosttype, "x86_64")
            self.assertEqual(config.raymake_version, "0.28.0")
            self.assertEqual(config.manylinux_version, "260128.221a193")


class TestHelpOutput(unittest.TestCase):
    """Test that --help prints without error."""

    def test_no_args_prints_help_and_exits_zero(self):
        with mock.patch("build_wheel.sys.argv", ["build_wheel"]):
            with self.assertRaises(SystemExit) as ctx:
                main()
        self.assertEqual(ctx.exception.code, 0)


if __name__ == "__main__":
    unittest.main()
