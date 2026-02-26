#!/usr/bin/env python3
"""Tests for build_wheel.py"""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from build_wheel import (
    BuildConfig,
    BuildError,
)


class TestBuildConfigPlatformDetection(unittest.TestCase):
    """Test _detect_platform() returns correct values for supported platforms."""

    def test_darwin_arm64(self):
        with mock.patch(
            "build_wheel.platform.system", return_value="Darwin"
        ), mock.patch("build_wheel.platform.machine", return_value="arm64"):
            hosttype, arch_suffix = BuildConfig._detect_platform()
        self.assertEqual((hosttype, arch_suffix), ("aarch64", "-aarch64"))

    def test_linux_x86_64(self):
        with mock.patch(
            "build_wheel.platform.system", return_value="Linux"
        ), mock.patch("build_wheel.platform.machine", return_value="x86_64"):
            hosttype, arch_suffix = BuildConfig._detect_platform()
        self.assertEqual((hosttype, arch_suffix), ("x86_64", ""))

    def test_linux_aarch64(self):
        with mock.patch(
            "build_wheel.platform.system", return_value="Linux"
        ), mock.patch("build_wheel.platform.machine", return_value="aarch64"):
            hosttype, arch_suffix = BuildConfig._detect_platform()
        self.assertEqual((hosttype, arch_suffix), ("aarch64", "-aarch64"))

    def test_unsupported_platform_raises(self):
        with mock.patch(
            "build_wheel.platform.system", return_value="Windows"
        ), mock.patch("build_wheel.platform.machine", return_value="AMD64"):
            with self.assertRaises(BuildError) as ctx:
                BuildConfig._detect_platform()
        self.assertIn("Unsupported platform", str(ctx.exception))


class TestBuildConfigParseFile(unittest.TestCase):
    """Test _parse_file() extracts values from config files."""

    def test_parses_quoted_value(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write('MANYLINUX_VERSION="260128.221a193"\n')
            f.flush()
            result = BuildConfig._parse_file(
                Path(f.name), r'MANYLINUX_VERSION=["\']?([^"\'\s]+)'
            )
        self.assertEqual(result, "260128.221a193")

    def test_missing_file_raises(self):
        with self.assertRaises(BuildError) as ctx:
            BuildConfig._parse_file(Path("/nonexistent"), r".*")
        self.assertIn("Missing", str(ctx.exception))

    def test_missing_pattern_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("OTHER_VAR=value\n")
            f.flush()
            with self.assertRaises(BuildError) as ctx:
                BuildConfig._parse_file(Path(f.name), r"MISSING_VAR=(\w+)")
        self.assertIn("not found in", str(ctx.exception))


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
                "build_wheel.platform.system", return_value="Linux"
            ), mock.patch(
                "build_wheel.platform.machine", return_value="x86_64"
            ), mock.patch.object(
                BuildConfig, "_find_ray_root", return_value=ray_root
            ), mock.patch.object(
                BuildConfig, "_get_git_commit", return_value="abc1234"
            ):
                config = BuildConfig.from_env("3.11", ".whl")

            self.assertEqual(config.python_version, "3.11")
            self.assertTrue(config.output_dir.is_absolute())
            self.assertEqual(config.output_dir.name, ".whl")
            self.assertEqual(config.hosttype, "x86_64")
            self.assertEqual(config.raymake_version, "0.28.0")
            self.assertEqual(config.manylinux_version, "260128.221a193")


if __name__ == "__main__":
    unittest.main()
