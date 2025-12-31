#!/usr/bin/env python3
"""Tests for build_targets.py"""

import os
from pathlib import Path
from unittest import mock

import pytest
from build_targets import (
    DEFAULT_MANYLINUX_VERSION,
    IMAGE_PREFIX,
    BuildContext,
    RayCore,
    RayCppCore,
    RayCppWheel,
    RayDashboard,
    RayJava,
    RayWheel,
    build_env,
    create_context,
    detect_platform,
    find_wanda,
    get_git_commit,
    normalize_arch,
)


@pytest.fixture
def ctx(tmp_path):
    """BuildContext with command recording."""
    wanda = tmp_path / "wanda"
    wanda.touch()
    wanda.chmod(0o755)
    repo = tmp_path / "ray"
    repo.mkdir()

    commands = []
    c = BuildContext(
        wanda_bin=wanda,
        repo_root=repo,
        env={"PYTHON_VERSION": "3.11", "ARCH_SUFFIX": ""},
        _run_cmd=lambda cmd, **kw: commands.append(cmd),
    )
    c._commands = commands
    return c


@pytest.fixture
def ctx_with_arch(tmp_path):
    """BuildContext with arch suffix set."""
    wanda = tmp_path / "wanda"
    wanda.touch()
    wanda.chmod(0o755)
    repo = tmp_path / "ray"
    repo.mkdir()

    commands = []
    c = BuildContext(
        wanda_bin=wanda,
        repo_root=repo,
        env={"PYTHON_VERSION": "3.11", "ARCH_SUFFIX": "-aarch64"},
        _run_cmd=lambda cmd, **kw: commands.append(cmd),
    )
    c._commands = commands
    return c


class TestBuildContext:
    def test_dry_run(self, capsys):
        ctx = BuildContext(dry_run=True)
        ctx.run(["echo", "test"])
        assert "[dry-run]" in capsys.readouterr().out

    def test_injected_runner(self, ctx):
        ctx.run(["test", "cmd"])
        assert ["test", "cmd"] in ctx._commands

    def test_wanda(self, ctx):
        ctx.wanda("spec.yaml")
        assert any("spec.yaml" in str(c) for c in ctx._commands)

    def test_docker_tag(self, ctx):
        ctx.docker_tag("src:tag", "dst:tag")
        assert ["docker", "tag", "src:tag", "dst:tag"] in ctx._commands

    def test_python_version_property(self):
        ctx = BuildContext(env={"PYTHON_VERSION": "3.12", "ARCH_SUFFIX": ""})
        assert ctx.python_version == "3.12"

    def test_python_version_default(self):
        ctx = BuildContext(env={})
        assert ctx.python_version == "3.10"

    def test_arch_suffix_property(self):
        ctx = BuildContext(env={"PYTHON_VERSION": "3.11", "ARCH_SUFFIX": "-aarch64"})
        assert ctx.arch_suffix == "-aarch64"

    def test_arch_suffix_default(self):
        ctx = BuildContext(env={})
        assert ctx.arch_suffix == ""


class TestRayCore:
    def test_spec(self):
        assert "ray-core.wanda.yaml" in RayCore.SPEC

    def test_build(self, ctx):
        RayCore(ctx).build()
        assert any("ray-core.wanda.yaml" in str(c) for c in ctx._commands)

    def test_image_name(self, ctx):
        assert RayCore(ctx).image_name() == f"{IMAGE_PREFIX}/ray-core-py3.11:latest"

    def test_local_name(self, ctx):
        assert RayCore(ctx).local_name() == "ray-core-py3.11:latest"

    def test_image_name_with_arch(self, ctx_with_arch):
        assert (
            RayCore(ctx_with_arch).image_name()
            == f"{IMAGE_PREFIX}/ray-core-py3.11-aarch64:latest"
        )

    def test_local_name_with_arch(self, ctx_with_arch):
        assert RayCore(ctx_with_arch).local_name() == "ray-core-py3.11-aarch64:latest"

    def test_docker_tag_called_after_build(self, ctx):
        RayCore(ctx).build()
        tag_cmds = [c for c in ctx._commands if c[0] == "docker" and c[1] == "tag"]
        assert len(tag_cmds) == 1
        assert tag_cmds[0] == [
            "docker",
            "tag",
            f"{IMAGE_PREFIX}/ray-core-py3.11:latest",
            "ray-core-py3.11:latest",
        ]


class TestRayDashboard:
    def test_spec(self):
        assert "ray-dashboard.wanda.yaml" in RayDashboard.SPEC

    def test_build(self, ctx):
        RayDashboard(ctx).build()
        assert any("ray-dashboard.wanda.yaml" in str(c) for c in ctx._commands)


class TestRayJava:
    def test_spec(self):
        assert "ray-java.wanda.yaml" in RayJava.SPEC

    def test_build(self, ctx):
        RayJava(ctx).build()
        assert any("ray-java.wanda.yaml" in str(c) for c in ctx._commands)


class TestRayWheel:
    def test_spec(self):
        assert "ray-wheel.wanda.yaml" in RayWheel.SPEC

    def test_deps(self):
        assert RayCore in RayWheel.DEPS
        assert RayDashboard in RayWheel.DEPS
        assert RayJava in RayWheel.DEPS

    def test_build_includes_deps(self, ctx):
        RayWheel(ctx).build()
        wanda_calls = [c for c in ctx._commands if "wanda" in str(c[0])]
        # Should build: ray-core, ray-dashboard, ray-java, ray-wheel
        assert len(wanda_calls) == 4
        assert any("ray-core" in str(c) for c in wanda_calls)
        assert any("ray-wheel" in str(c) for c in wanda_calls)

    def test_image_name(self, ctx):
        name = RayWheel(ctx).image_name()
        assert IMAGE_PREFIX in name
        assert "ray-wheel-py3.11" in name


class TestRayCppCore:
    def test_spec(self):
        assert "ray-cpp-core.wanda.yaml" in RayCppCore.SPEC

    def test_build(self, ctx):
        RayCppCore(ctx).build()
        assert any("ray-cpp-core.wanda.yaml" in str(c) for c in ctx._commands)


class TestRayCppWheel:
    def test_spec(self):
        assert "ray-cpp-wheel.wanda.yaml" in RayCppWheel.SPEC

    def test_deps(self):
        assert RayCppCore in RayCppWheel.DEPS

    def test_build_includes_all_deps(self, ctx):
        RayCppWheel(ctx).build()
        wanda_calls = [c for c in ctx._commands if "wanda" in str(c[0])]
        # Should build: ray-core, ray-dashboard, ray-java, ray-cpp-core, ray-cpp-wheel
        assert len(wanda_calls) == 5
        assert any("ray-core" in str(c) for c in wanda_calls)
        assert any("ray-dashboard" in str(c) for c in wanda_calls)
        assert any("ray-java" in str(c) for c in wanda_calls)
        assert any("ray-cpp-core" in str(c) for c in wanda_calls)
        assert any("ray-cpp-wheel" in str(c) for c in wanda_calls)

    def test_image_name(self, ctx):
        name = RayCppWheel(ctx).image_name()
        assert "ray-cpp-wheel-py3.11" in name


class TestNormalizeArch:
    def test_arm64_to_aarch64(self):
        assert normalize_arch("arm64") == "aarch64"

    def test_x86_64_unchanged(self):
        assert normalize_arch("x86_64") == "x86_64"

    def test_aarch64_unchanged(self):
        assert normalize_arch("aarch64") == "aarch64"

    def test_empty_uses_platform(self):
        with mock.patch("platform.machine", return_value="x86_64"):
            assert normalize_arch("") == "x86_64"


class TestDetectPlatform:
    def test_linux(self):
        with mock.patch("platform.system", return_value="Linux"):
            host, target = detect_platform()
            assert host == "linux"
            assert target is None

    def test_darwin_arm64(self):
        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch("platform.machine", return_value="arm64"):
                with mock.patch.dict(os.environ, {}, clear=True):
                    _, target = detect_platform()
                    assert target == "linux/arm64"

    def test_darwin_x86(self):
        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch("platform.machine", return_value="x86_64"):
                with mock.patch.dict(os.environ, {}, clear=True):
                    _, target = detect_platform()
                    assert target == "linux/amd64"

    def test_env_override(self):
        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch("platform.machine", return_value="arm64"):
                with mock.patch.dict(os.environ, {"WANDA_PLATFORM": "linux/amd64"}):
                    _, target = detect_platform()
                    assert target == "linux/amd64"


class TestFindWanda:
    def test_from_env(self, tmp_path):
        wanda = tmp_path / "wanda"
        wanda.touch()
        with mock.patch.dict(os.environ, {"WANDA_BIN": str(wanda)}):
            assert find_wanda() == wanda

    def test_from_path(self, tmp_path):
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch("shutil.which", return_value=str(tmp_path / "wanda")):
                assert find_wanda() == tmp_path / "wanda"

    def test_not_found(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch("shutil.which", return_value=None):
                with mock.patch.object(Path, "exists", return_value=False):
                    assert find_wanda() is None


class TestGetGitCommit:
    def test_returns_commit_hash(self):
        # In a git repo, should return a 40-char hex string
        commit = get_git_commit()
        assert commit != "unknown"
        assert len(commit) == 40

    def test_returns_unknown_on_failure(self):
        with mock.patch("subprocess.run", side_effect=FileNotFoundError):
            assert get_git_commit() == "unknown"


class TestBuildEnv:
    def test_returns_copy_with_build_vars(self):
        env = build_env(python_version="3.12", arch_suffix="-aarch64")
        assert env["PYTHON_VERSION"] == "3.12"
        assert env["MANYLINUX_VERSION"] == DEFAULT_MANYLINUX_VERSION
        assert env["ARCH_SUFFIX"] == "-aarch64"
        # Original os.environ should not be modified
        assert os.environ.get("PYTHON_VERSION") != "3.12"

    def test_does_not_modify_os_environ(self):
        original = os.environ.get("PYTHON_VERSION")
        build_env(python_version="3.99")
        assert os.environ.get("PYTHON_VERSION") == original

    def test_sets_buildkite_commit(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            # Remove BUILDKITE_COMMIT if it exists
            os.environ.pop("BUILDKITE_COMMIT", None)
            env = build_env()
            assert "BUILDKITE_COMMIT" in env
            assert len(env["BUILDKITE_COMMIT"]) > 0

    def test_preserves_existing_buildkite_commit(self):
        with mock.patch.dict(os.environ, {"BUILDKITE_COMMIT": "abc123"}):
            env = build_env()
            assert env["BUILDKITE_COMMIT"] == "abc123"

    def test_sets_hosttype(self):
        env = build_env(hosttype="aarch64")
        assert env["HOSTTYPE"] == "aarch64"

    def test_normalizes_hosttype(self):
        env = build_env(hosttype="arm64")
        assert env["HOSTTYPE"] == "aarch64"


class TestCreateContext:
    def test_creates_context(self, tmp_path):
        wanda = tmp_path / "wanda"
        wanda.touch()
        repo = tmp_path / "repo"
        repo.mkdir()

        ctx = create_context(
            python_version="3.12",
            repo_root=repo,
            wanda_bin=wanda,
            dry_run=True,
        )

        assert ctx.python_version == "3.12"  # via property
        assert ctx.env["PYTHON_VERSION"] == "3.12"
        assert ctx.repo_root == repo
        assert ctx.wanda_bin == wanda
        assert ctx.dry_run is True

    def test_env_is_isolated(self, tmp_path):
        wanda = tmp_path / "wanda"
        wanda.touch()
        repo = tmp_path / "repo"
        repo.mkdir()

        ctx = create_context(
            python_version="3.12",
            repo_root=repo,
            wanda_bin=wanda,
        )

        assert ctx.env["PYTHON_VERSION"] == "3.12"
        assert ctx.env["MANYLINUX_VERSION"] == DEFAULT_MANYLINUX_VERSION
        # Original os.environ should not be modified
        assert os.environ.get("PYTHON_VERSION") != "3.12"


class TestDependencyDeduplication:
    def test_shared_deps_built_once(self, ctx):
        """When building both RayWheel and RayCppWheel, shared deps are built once."""
        RayWheel(ctx).build()
        RayCppWheel(ctx).build()

        wanda_calls = [c for c in ctx._commands if "wanda" in str(c[0])]
        # Should build: ray-core, ray-dashboard, ray-java, ray-wheel, ray-cpp-core, ray-cpp-wheel
        # Shared deps (core, dashboard, java) should NOT be duplicated
        assert len(wanda_calls) == 6
        # Each spec should appear exactly once
        specs = [str(c[1]) for c in wanda_calls]
        assert specs.count("ci/docker/ray-core.wanda.yaml") == 1
        assert specs.count("ci/docker/ray-dashboard.wanda.yaml") == 1
        assert specs.count("ci/docker/ray-java.wanda.yaml") == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
