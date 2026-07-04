"""Unit tests for get_haproxy_binary() resolution logic.

get_haproxy_binary() resolves an HAProxy binary path with this priority:
  1. Explicit RAY_SERVE_HAPROXY_BINARY_PATH override, validated and returned.
  2. Bundled binary from the ``ray-haproxy`` PyPI package.
  3. System ``haproxy`` on PATH.
  4. FileNotFoundError with an actionable message.
"""

import stat
from unittest.mock import MagicMock, patch

import pytest

from ray.serve._private.haproxy import get_haproxy_binary

# Patch targets: module-level constants imported at the top of haproxy.py.
HAPROXY_MODULE = "ray.serve._private.haproxy"
BINARY_PATH_PATCH = f"{HAPROXY_MODULE}.RAY_SERVE_HAPROXY_BINARY_PATH"
WHICH_PATCH = f"{HAPROXY_MODULE}.shutil.which"


def _make_executable_binary(tmp_path):
    """Create an empty executable file under tmp_path and return its path."""
    binary = tmp_path / "haproxy"
    binary.write_bytes(b"")
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    return binary


def test_explicit_path_takes_precedence_over_bundled(tmp_path):
    """An explicit RAY_SERVE_HAPROXY_BINARY_PATH wins over the bundled
    ray-haproxy package, so an operator's custom binary is not silently
    overridden."""
    binary = _make_executable_binary(tmp_path)

    mock_module = MagicMock()
    mock_module.get_haproxy_binary.return_value = "/bundled/haproxy"
    with patch.dict("sys.modules", {"ray_haproxy": mock_module}), patch(
        BINARY_PATH_PATCH, str(binary)
    ):
        assert get_haproxy_binary() == str(binary)
        mock_module.get_haproxy_binary.assert_not_called()


def test_explicit_path_validates_executable(tmp_path):
    """An explicit RAY_SERVE_HAPROXY_BINARY_PATH is validated before use: it must
    exist and be executable, otherwise resolution raises."""
    binary = _make_executable_binary(tmp_path)

    with patch(BINARY_PATH_PATCH, str(binary)):
        assert get_haproxy_binary() == str(binary)

    # Same file without the execute bit is rejected.
    binary.chmod(0o644)
    with patch(BINARY_PATH_PATCH, str(binary)):
        with pytest.raises(FileNotFoundError, match="HAPROXY_BINARY_PATH"):
            get_haproxy_binary()

    # A nonexistent path is rejected.
    with patch(BINARY_PATH_PATCH, str(tmp_path / "no_such_file")):
        with pytest.raises(FileNotFoundError, match="HAPROXY_BINARY_PATH"):
            get_haproxy_binary()


@patch(BINARY_PATH_PATCH, "")
def test_bundled_package_used_when_no_explicit_path():
    """With no explicit path set, the bundled ray-haproxy binary is used."""
    mock_module = MagicMock()
    mock_module.get_haproxy_binary.return_value = "/bundled/haproxy"
    with patch.dict("sys.modules", {"ray_haproxy": mock_module}):
        assert get_haproxy_binary() == "/bundled/haproxy"


@patch(WHICH_PATCH, return_value="/usr/sbin/haproxy")
@patch(BINARY_PATH_PATCH, "")
def test_pip_package_oserror_falls_through(_mock_which):
    """If ray-haproxy is installed but its binary is broken (e.g. missing file,
    bad permissions), the function should fall through to the system haproxy
    rather than crashing the proxy actor. We catch OSError (not just
    FileNotFoundError) so PermissionError is also handled."""
    mock_module = MagicMock()
    mock_module.get_haproxy_binary.side_effect = OSError("bad permissions")

    with patch.dict("sys.modules", {"ray_haproxy": mock_module}):
        assert get_haproxy_binary() == "/usr/sbin/haproxy"


@patch(WHICH_PATCH, return_value=None)
@patch(BINARY_PATH_PATCH, "")
def test_nothing_available_raises_with_instructions(_mock_which):
    """When no binary is available from any source, the error message must
    tell the user how to fix it (install ray[serve], set the env var, or
    put haproxy on PATH)."""
    with patch.dict("sys.modules", {"ray_haproxy": None}):
        with pytest.raises(FileNotFoundError, match=r"ray\[serve\]"):
            get_haproxy_binary()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
