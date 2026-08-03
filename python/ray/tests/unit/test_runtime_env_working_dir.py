"""Unit tests for working_dir runtime environment functionality."""

import sys
from pathlib import Path

import pytest

from ray._common import runtime_env_uri
from ray._common.runtime_env_uri import parse_uri
from ray._private.ray_constants import get_runtime_env_default_excludes
from ray._private.runtime_env.packaging import (
    _get_local_path,
    get_path_from_local_dir_uri,
    is_local_dir_uri,
    is_local_dir_uri_or_raise,
)
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed

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


class TestLocalDirURI:
    """`local://` names a directory already present on every node."""

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("local:///app", True),
            ("local:///app/subdir", True),
            ("gcs://_ray_pkg_abc.zip", False),
            ("s3://bucket/pkg.zip", False),
            ("file:///tmp/pkg.zip", False),
            ("/app", False),
            ("", False),
        ],
    )
    def test_is_local_dir_uri(self, uri, expected):
        assert is_local_dir_uri(uri) is expected

    @pytest.mark.parametrize(
        "uri,expected", [("local:///app", True), ("s3://bucket/pkg.zip", False)]
    )
    def test_is_local_dir_uri_or_raise(self, uri, expected):
        assert is_local_dir_uri_or_raise(uri) is expected

    def test_is_local_dir_uri_or_raise_rejects_malformed_local_uri(self):
        with pytest.raises(ValueError, match="the path must be absolute"):
            is_local_dir_uri_or_raise("local://relative/path")

    def test_get_path_from_local_dir_uri(self):
        assert get_path_from_local_dir_uri("local:///app") == Path("/app")
        assert get_path_from_local_dir_uri("local:///a/b/c") == Path("/a/b/c")

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("local:///C:/app", "C:/app"),
            ("local:///c:/a/b", "c:/a/b"),
            ("local:////server/share/app", "//server/share/app"),
        ],
    )
    def test_windows_paths(self, monkeypatch, uri, expected):
        """A drive path arrives as '/C:/app' and must lose the leading slash."""
        monkeypatch.setattr(runtime_env_uri, "_WIN32", True)
        assert parse_uri(uri)[1] == expected

    @pytest.mark.parametrize("uri", ["local://C:/app", "local:///app", "local://app"])
    def test_windows_rejects_paths_without_a_root(self, monkeypatch, uri):
        monkeypatch.setattr(runtime_env_uri, "_WIN32", True)
        with pytest.raises(ValueError, match="the path must be absolute"):
            parse_uri(uri)

    def test_get_path_from_local_dir_uri_rejects_other_protocols(self):
        with pytest.raises(ValueError, match="Expected a 'local://' URI"):
            get_path_from_local_dir_uri("gcs://_ray_pkg_abc.zip")

    def test_local_uri_has_no_ray_managed_package_path(self):
        """Nothing may compute a managed path for an in image dir."""
        with pytest.raises(ValueError, match="never downloaded or unpacked"):
            _get_local_path("/tmp/ray/working_dir_files", "local:///app")

    def test_upload_is_a_no_op(self):
        """The client must not try to package or upload an in image directory."""
        runtime_env = {"working_dir": "local:///app"}
        assert upload_working_dir_if_needed(runtime_env, include_gitignore=False) == {
            "working_dir": "local:///app"
        }

    def test_upload_rejects_malformed_local_uri(self):
        with pytest.raises(ValueError, match="the path must be absolute"):
            upload_working_dir_if_needed(
                {"working_dir": "local://relative/path"}, include_gitignore=False
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
