"""Unit tests for working_dir runtime environment functionality."""

import sys
from pathlib import Path

import pytest

from ray._common.runtime_env_uri import parse_uri
from ray._private.ray_constants import get_runtime_env_default_excludes
from ray._private.runtime_env.packaging import (
    _get_local_path,
    get_local_dir_uri_path,
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
            ("local:///app", Path("/app")),
            ("local:///a/b/c", Path("/a/b/c")),
            ("LOCAL:///app", Path("/app")),
            ("gcs://_ray_pkg_abc.zip", None),
            ("s3://bucket/pkg.zip", None),
            ("file:///tmp/pkg.zip", None),
            ("/app", None),
            ("", None),
        ],
    )
    def test_get_local_dir_uri_path(self, uri, expected):
        assert get_local_dir_uri_path(uri) == expected

    def test_get_local_dir_uri_path_rejects_malformed_local_uri(self):
        with pytest.raises(ValueError, match="the path must be absolute"):
            get_local_dir_uri_path("local://relative/path")

    @pytest.mark.parametrize(
        "uri",
        [
            "local:///app/code.zip",
            "local:///app/lib.whl",
            "local:///app/code.tar.gz",
            "local:///app/code.tgz",
            "local:///app/code.tar.xz",
            "local://C:/app/code.zip",
        ],
    )
    def test_rejects_archives(self, uri):
        """A local:// URI names a directory used in place, never an archive."""
        with pytest.raises(ValueError, match="must be a directory"):
            parse_uri(uri)

    @pytest.mark.parametrize(
        "uri,expected",
        [
            ("local:///app", "/app"),
            ("local://C:/app", "C:/app"),
            ("local://c:/a/b", "c:/a/b"),
            ("local://C:\\app", "C:\\app"),
            ("local:///C:/app", "C:/app"),
            ("local:///C:\\app", "C:\\app"),
            ("local:////server/share/app", "//server/share/app"),
        ],
    )
    def test_parses_identically_on_every_platform(self, uri, expected):
        """A URI is not a path: the host OS must not change what it means."""
        assert parse_uri(uri)[1] == expected

    @pytest.mark.parametrize(
        "uri", ["local://app", "local://", "local://server/share", "local://C:app"]
    )
    def test_rejects_paths_without_a_root(self, uri):
        with pytest.raises(ValueError, match="the path must be absolute"):
            parse_uri(uri)

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
