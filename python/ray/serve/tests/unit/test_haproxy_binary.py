"""Unit tests for get_haproxy_binary() resolution logic.

get_haproxy_binary() resolves an HAProxy binary path with this priority:
  0. Flag off  → return RAY_SERVE_HAPROXY_BINARY_PATH verbatim (no resolution).
  1. Explicit RAY_SERVE_HAPROXY_BINARY_PATH override → validate and return.
  2. Bundled binary from the ``ray-haproxy`` PyPI package.
  3. System ``haproxy`` on PATH.
  4. FileNotFoundError with an actionable message.

Tests that exercise the system-PATH fallback (steps 3-4) expect ``haproxy``
to be installed in the CI Docker image.
"""

import stat
from unittest.mock import MagicMock, patch

import pytest

from ray.serve._private.haproxy import get_haproxy_binary

# Patch targets — module-level constants imported at the top of haproxy.py.
HAPROXY_MODULE = "ray.serve._private.haproxy"
FLAG_PATCH = f"{HAPROXY_MODULE}.RAY_SERVE_EXPERIMENTAL_PIP_HAPROXY"
BINARY_PATH_PATCH = f"{HAPROXY_MODULE}.RAY_SERVE_HAPROXY_BINARY_PATH"


# -- Flag off (default behaviour) --


@patch(BINARY_PATH_PATCH, "haproxy")
@patch(FLAG_PATCH, False)
def test_flag_off_returns_constant():
    """When the feature flag is off, return the constant with no resolution."""
    assert get_haproxy_binary() == "haproxy"


# -- Explicit binary path override (step 1) --


@patch(FLAG_PATCH, True)
def test_explicit_path_valid_executable(tmp_path):
    """A user-supplied path to a valid executable is returned directly."""
    binary = tmp_path / "haproxy"
    binary.write_bytes(b"")
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)

    with patch(BINARY_PATH_PATCH, str(binary)):
        assert get_haproxy_binary() == str(binary)


@patch(FLAG_PATCH, True)
def test_explicit_path_nonexistent_raises(tmp_path):
    """A user-supplied path that doesn't exist raises with a clear message."""
    with patch(BINARY_PATH_PATCH, str(tmp_path / "no_such_file")):
        with pytest.raises(FileNotFoundError, match="HAPROXY_BINARY_PATH"):
            get_haproxy_binary()


@patch(FLAG_PATCH, True)
def test_explicit_path_not_executable_raises(tmp_path):
    """A user-supplied path that exists but is not executable raises."""
    binary = tmp_path / "haproxy"
    binary.write_bytes(b"")
    binary.chmod(0o644)

    with patch(BINARY_PATH_PATCH, str(binary)):
        with pytest.raises(FileNotFoundError, match="HAPROXY_BINARY_PATH"):
            get_haproxy_binary()


# -- ray-haproxy pip package (step 2) --


@patch(BINARY_PATH_PATCH, "haproxy")
@patch(FLAG_PATCH, True)
def test_pip_package_bundled_binary():
    """When the ray-haproxy package is installed, use its bundled binary."""
    mock_module = MagicMock()
    mock_module.get_haproxy_binary.return_value = "/opt/ray_haproxy/bin/haproxy"

    with patch.dict("sys.modules", {"ray_haproxy": mock_module}):
        assert get_haproxy_binary() == "/opt/ray_haproxy/bin/haproxy"
    mock_module.get_haproxy_binary.assert_called_once()


@patch(BINARY_PATH_PATCH, "haproxy")
@patch(FLAG_PATCH, True)
def test_pip_package_oserror_falls_through():
    """If the package is installed but its binary is broken (e.g. bad
    permissions), gracefully fall through to the system haproxy."""
    mock_module = MagicMock()
    mock_module.get_haproxy_binary.side_effect = OSError("bad permissions")

    with patch.dict("sys.modules", {"ray_haproxy": mock_module}):
        result = get_haproxy_binary()
    assert result.endswith("haproxy")


# -- System PATH fallback (step 3) --


@patch(BINARY_PATH_PATCH, "haproxy")
@patch(FLAG_PATCH, True)
def test_system_path_fallback():
    """When ray-haproxy is not installed, find haproxy on the system PATH."""
    with patch.dict("sys.modules", {"ray_haproxy": None}):
        result = get_haproxy_binary()
    assert result.endswith("haproxy")


@patch(f"{HAPROXY_MODULE}.shutil.which", return_value=None)
@patch(BINARY_PATH_PATCH, "haproxy")
@patch(FLAG_PATCH, True)
def test_nothing_available_raises(_mock_which):
    """When no binary source is available, raise with install instructions."""
    with patch.dict("sys.modules", {"ray_haproxy": None}):
        with pytest.raises(FileNotFoundError, match=r"ray\[haproxy\]"):
            get_haproxy_binary()
