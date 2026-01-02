#!/usr/bin/env python3
"""Tests for wanda_targets.py"""

import os
from pathlib import Path
from unittest import mock

import pytest
from wanda_targets import (
    DEFAULT_MANYLINUX_VERSION,
    IMAGE_PREFIX,
    TARGETS,
    BuildContext,
    RayCore,
    RayCppCore,
    RayCppWheel,
    RayDashboard,
    RayJava,
    RayWheel,
    build_env,
    create_context,
    detect_target_platform,
    expand_env_vars,
    extract_env_var_names,
    find_wanda,
    get_git_commit,
    get_wanda_image_name,
    normalize_arch,
    parse_wanda_name,
)


@pytest.fixture
def repo_root():
    """Return the actual repo root for tests that need real wanda specs."""
    return Path(__file__).parent.parent.parent


@pytest.fixture
def ctx(tmp_path, repo_root):
    """BuildContext with command recording."""
    wanda = tmp_path / "wanda"
    wanda.touch()
    wanda.chmod(0o755)

    commands = []
    c = BuildContext(
        wanda_bin=wanda,
        repo_root=repo_root,
        env={"PYTHON_VERSION": "3.11", "ARCH_SUFFIX": ""},
        _run_cmd=lambda cmd, **kw: commands.append(cmd),
    )
    c._commands = commands
    return c


@pytest.fixture
def ctx_with_arch(tmp_path, repo_root):
    """BuildContext with arch suffix set."""
    wanda = tmp_path / "wanda"
    wanda.touch()
    wanda.chmod(0o755)

    commands = []
    c = BuildContext(
        wanda_bin=wanda,
        repo_root=repo_root,
        env={"PYTHON_VERSION": "3.11", "ARCH_SUFFIX": "-aarch64"},
        _run_cmd=lambda cmd, **kw: commands.append(cmd),
    )
    c._commands = commands
    return c


class TestBuildContext:
    def test_injected_runner(self, ctx):
        ctx.run(["test", "cmd"])
        assert ["test", "cmd"] in ctx._commands

    def test_wanda(self, ctx):
        ctx.wanda("spec.yaml")
        assert any("spec.yaml" in str(c) for c in ctx._commands)

    def test_docker_tag(self, ctx):
        ctx.docker_tag("src:tag", "dst:tag")
        assert ["docker", "tag", "src:tag", "dst:tag"] in ctx._commands

    def test_python_version_property(self, tmp_path):
        wanda = tmp_path / "wanda"
        wanda.touch()
        repo = tmp_path / "repo"
        repo.mkdir()
        ctx = BuildContext(
            wanda_bin=wanda,
            repo_root=repo,
            env={"PYTHON_VERSION": "3.12", "ARCH_SUFFIX": ""},
        )
        assert ctx.python_version == "3.12"

    def test_python_version_default(self, tmp_path):
        wanda = tmp_path / "wanda"
        wanda.touch()
        repo = tmp_path / "repo"
        repo.mkdir()
        ctx = BuildContext(wanda_bin=wanda, repo_root=repo, env={})
        assert ctx.python_version == "3.10"

    def test_arch_suffix_property(self, tmp_path):
        wanda = tmp_path / "wanda"
        wanda.touch()
        repo = tmp_path / "repo"
        repo.mkdir()
        ctx = BuildContext(
            wanda_bin=wanda,
            repo_root=repo,
            env={"PYTHON_VERSION": "3.11", "ARCH_SUFFIX": "-aarch64"},
        )
        assert ctx.arch_suffix == "-aarch64"

    def test_arch_suffix_default(self, tmp_path):
        wanda = tmp_path / "wanda"
        wanda.touch()
        repo = tmp_path / "repo"
        repo.mkdir()
        ctx = BuildContext(wanda_bin=wanda, repo_root=repo, env={})
        assert ctx.arch_suffix == ""


class TestRayCore:
    def test_spec(self):
        assert "ray-core.wanda.yaml" in RayCore.SPEC

    def test_build(self, ctx):
        RayCore(ctx).build()
        assert any("ray-core.wanda.yaml" in str(c) for c in ctx._commands)

    def test_remote_image(self, ctx):
        assert RayCore(ctx).remote_image == f"{IMAGE_PREFIX}/ray-core-py3.11:latest"

    def test_local_image(self, ctx):
        assert RayCore(ctx).local_image == "ray-core-py3.11:latest"

    def test_remote_image_with_arch(self, ctx_with_arch):
        assert (
            RayCore(ctx_with_arch).remote_image
            == f"{IMAGE_PREFIX}/ray-core-py3.11-aarch64:latest"
        )

    def test_local_image_with_arch(self, ctx_with_arch):
        assert RayCore(ctx_with_arch).local_image == "ray-core-py3.11-aarch64:latest"

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

    def test_remote_image(self, ctx):
        name = RayWheel(ctx).remote_image
        assert IMAGE_PREFIX in name
        assert "ray-wheel-py3.11" in name


class TestRayCppCore:
    def test_spec(self):
        assert "ray-cpp-core.wanda.yaml" in RayCppCore.SPEC

    def test_no_deps(self):
        # RayCppCore only depends on manylinux base, not other ray images
        assert RayCppCore.DEPS == ()

    def test_build(self, ctx):
        RayCppCore(ctx).build()
        wanda_calls = [c for c in ctx._commands if "wanda" in str(c[0])]
        # Should only build ray-cpp-core (no deps)
        assert len(wanda_calls) == 1
        assert any("ray-cpp-core.wanda.yaml" in str(c) for c in ctx._commands)


class TestRayCppWheel:
    def test_spec(self):
        assert "ray-cpp-wheel.wanda.yaml" in RayCppWheel.SPEC

    def test_deps(self):
        assert RayCore in RayCppWheel.DEPS
        assert RayCppCore in RayCppWheel.DEPS
        assert RayJava in RayCppWheel.DEPS
        assert RayDashboard in RayCppWheel.DEPS

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

    def test_remote_image(self, ctx):
        name = RayCppWheel(ctx).remote_image
        assert "ray-cpp-wheel-py3.11" in name


class TestExpandEnvVars:
    def test_dollar_var_syntax(self):
        env = {"FOO": "bar", "BAZ": "qux"}
        assert expand_env_vars("$FOO-$BAZ", env) == "bar-qux"

    def test_brace_var_syntax(self):
        env = {"FOO": "bar"}
        assert expand_env_vars("${FOO}", env) == "bar"

    def test_mixed_syntax(self):
        env = {"A": "1", "B": "2"}
        assert expand_env_vars("$A-${B}", env) == "1-2"

    def test_missing_var_becomes_empty(self):
        assert expand_env_vars("$MISSING", {}) == ""

    def test_adjacent_vars(self):
        env = {"PYTHON_VERSION": "3.11", "ARCH_SUFFIX": "-aarch64"}
        assert expand_env_vars("py$PYTHON_VERSION$ARCH_SUFFIX", env) == "py3.11-aarch64"

    def test_no_vars(self):
        assert expand_env_vars("plain-string", {}) == "plain-string"


class TestExtractEnvVarNames:
    def test_dollar_syntax(self):
        assert extract_env_var_names("$FOO") == {"FOO"}

    def test_brace_syntax(self):
        assert extract_env_var_names("${FOO}") == {"FOO"}

    def test_multiple_vars(self):
        assert extract_env_var_names("$FOO-$BAR") == {"FOO", "BAR"}

    def test_adjacent_vars(self):
        assert extract_env_var_names("$A$B") == {"A", "B"}

    def test_no_vars(self):
        assert extract_env_var_names("plain-string") == set()

    def test_mixed_syntax(self):
        assert extract_env_var_names("$A-${B}") == {"A", "B"}


class TestWandaSpecEnvVars:
    """Validate that all wanda specs only use environment variables set by build_env()."""

    def test_all_targets_use_defined_env_vars(self, repo_root):
        """Every env var in wanda spec 'name' fields must be set by build_env()."""
        env = build_env()
        for target_name, target_cls in TARGETS.items():
            spec_path = repo_root / target_cls.SPEC
            name_template = parse_wanda_name(spec_path)
            used_vars = extract_env_var_names(name_template)
            missing_vars = used_vars - set(env.keys())
            assert not missing_vars, (
                f"{target_name} uses env vars not set by build_env(): {missing_vars}. "
                f"Either add them to build_env() or remove from spec."
            )

    def test_detects_undefined_env_var(self, tmp_path):
        """Verify we can detect when a spec uses an undefined env var."""
        spec_file = tmp_path / "bad.wanda.yaml"
        spec_file.write_text('name: "my-image-$UNDEFINED_VAR"\n')
        name_template = parse_wanda_name(spec_file)
        used_vars = extract_env_var_names(name_template)
        env = build_env()
        missing_vars = used_vars - set(env.keys())
        assert missing_vars == {"UNDEFINED_VAR"}


class TestParseWandaName:
    def test_parses_double_quoted_name(self, tmp_path):
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text('name: "my-image-py$PYTHON_VERSION$ARCH_SUFFIX"\n')
        assert parse_wanda_name(spec_file) == "my-image-py$PYTHON_VERSION$ARCH_SUFFIX"

    def test_parses_single_quoted_name(self, tmp_path):
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text("name: 'my-image'\n")
        assert parse_wanda_name(spec_file) == "my-image"

    def test_parses_unquoted_name(self, tmp_path):
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text("name: simple-name\n")
        assert parse_wanda_name(spec_file) == "simple-name"

    def test_strips_inline_comment(self, tmp_path):
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text("name: test-image  # this is a comment\n")
        assert parse_wanda_name(spec_file) == "test-image"

    def test_name_containing_hash(self, tmp_path):
        """Name containing ' #' should not be split incorrectly."""
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text('name: "my-image #1" # a comment\n')
        assert parse_wanda_name(spec_file) == "my-image #1"

    def test_skips_leading_comments(self, tmp_path):
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text(
            """# This is a header comment
# Another comment
name: my-image
"""
        )
        assert parse_wanda_name(spec_file) == "my-image"

    def test_skips_empty_lines(self, tmp_path):
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text(
            """
name: my-image
"""
        )
        assert parse_wanda_name(spec_file) == "my-image"

    def test_raises_if_no_name(self, tmp_path):
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text("froms:\n  - base\n")
        with pytest.raises(ValueError, match="No 'name:' field found"):
            parse_wanda_name(spec_file)

    def test_finds_name_after_other_fields(self, tmp_path):
        """Name field doesn't have to be first."""
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text(
            """# comment
disable_caching: true
name: my-image
froms:
  - base
"""
        )
        # Note: our simple parser finds the first 'name:' line
        assert parse_wanda_name(spec_file) == "my-image"

    def test_handles_complex_env_vars(self, tmp_path):
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text('name: "ray-wheel-py$PYTHON_VERSION$ARCH_SUFFIX"\n')
        assert parse_wanda_name(spec_file) == "ray-wheel-py$PYTHON_VERSION$ARCH_SUFFIX"

    def test_handles_hash_in_quoted_name(self, tmp_path):
        spec_file = tmp_path / "test.wanda.yaml"
        spec_file.write_text('name: "my-image-#1" # a comment\n')
        assert parse_wanda_name(spec_file) == "my-image-#1"


class TestGetWandaImageName:
    def test_expands_python_version(self, repo_root):
        spec_path = repo_root / "ci/docker/ray-core.wanda.yaml"
        env = {"PYTHON_VERSION": "3.12", "ARCH_SUFFIX": ""}
        name = get_wanda_image_name(spec_path, env)
        assert name == "ray-core-py3.12"

    def test_expands_arch_suffix(self, repo_root):
        spec_path = repo_root / "ci/docker/ray-core.wanda.yaml"
        env = {"PYTHON_VERSION": "3.11", "ARCH_SUFFIX": "-aarch64"}
        name = get_wanda_image_name(spec_path, env)
        assert name == "ray-core-py3.11-aarch64"

    def test_dashboard_no_vars(self, repo_root):
        spec_path = repo_root / "ci/docker/ray-dashboard.wanda.yaml"
        name = get_wanda_image_name(spec_path, {})
        assert name == "ray-dashboard"


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


class TestDetectTargetPlatform:
    def test_linux_returns_none(self):
        with mock.patch("platform.system", return_value="Linux"):
            assert detect_target_platform() is None

    def test_darwin_arm64(self):
        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch("platform.machine", return_value="arm64"):
                with mock.patch.dict(os.environ, {}, clear=True):
                    assert detect_target_platform() == "linux/arm64"

    def test_darwin_x86(self):
        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch("platform.machine", return_value="x86_64"):
                with mock.patch.dict(os.environ, {}, clear=True):
                    assert detect_target_platform() == "linux/amd64"

    def test_env_override(self):
        with mock.patch("platform.system", return_value="Darwin"):
            with mock.patch("platform.machine", return_value="arm64"):
                with mock.patch.dict(os.environ, {"WANDA_PLATFORM": "linux/amd64"}):
                    assert detect_target_platform() == "linux/amd64"


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
        )

        assert ctx.python_version == "3.12"  # via property
        assert ctx.env["PYTHON_VERSION"] == "3.12"
        assert ctx.repo_root == repo
        assert ctx.wanda_bin == wanda

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
