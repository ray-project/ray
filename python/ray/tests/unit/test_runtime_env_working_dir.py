"""Unit tests for working_dir runtime environment functionality."""

import sys

import pytest

from ray._private.ray_constants import get_runtime_env_default_excludes

ENV_VAR = "RAY_OVERRIDE_RUNTIME_ENV_DEFAULT_EXCLUDES"


class TestGetRuntimeEnvDefaultExcludes:
    """Tests for get_runtime_env_default_excludes()."""

    def test_returns_defaults_when_env_var_not_set(self, monkeypatch):
        monkeypatch.delenv(ENV_VAR, raising=False)
        result = get_runtime_env_default_excludes()
        assert ".git" in result and ".venv" in result

    def test_empty_env_var_disables_defaults(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "")
        assert get_runtime_env_default_excludes() == []

    def test_custom_env_var_overrides_defaults(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "foo, bar ,baz")
        assert get_runtime_env_default_excludes() == ["foo", "bar", "baz"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
