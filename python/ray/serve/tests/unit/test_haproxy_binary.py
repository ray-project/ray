"""Unit tests for get_haproxy_binary() in ray.serve._private.haproxy."""
import stat
from unittest.mock import MagicMock, patch

import pytest

from ray.serve._private.haproxy import get_haproxy_binary

_FLAG_PATH = "ray.serve._private.haproxy.RAY_SERVE_EXPERIMENTAL_PIP_HAPROXY"
_BINARY_PATH = "ray.serve._private.haproxy.RAY_SERVE_HAPROXY_BINARY_PATH"


class TestGetHaproxyBinary:
    """Tests for get_haproxy_binary() resolution logic."""

    # 0. Flag off → returns RAY_SERVE_HAPROXY_BINARY_PATH (default "haproxy")
    def test_flag_off_returns_default(self):
        with patch(_FLAG_PATH, False), patch(_BINARY_PATH, "haproxy"):
            assert get_haproxy_binary() == "haproxy"

    # 0b. Flag off with explicit path → returns that path
    def test_flag_off_returns_explicit_path(self):
        with patch(_FLAG_PATH, False), patch(_BINARY_PATH, "/usr/local/bin/haproxy"):
            assert get_haproxy_binary() == "/usr/local/bin/haproxy"

    # 1. Explicit binary path — valid executable
    def test_explicit_binary_path_valid(self, tmp_path):
        binary = tmp_path / "haproxy"
        binary.write_bytes(b"")
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR)

        with patch(_FLAG_PATH, True), patch(_BINARY_PATH, str(binary)):
            assert get_haproxy_binary() == str(binary)

    # 2. Explicit binary path — does not exist → FileNotFoundError
    def test_explicit_binary_path_nonexistent_raises(self, tmp_path):
        with patch(_FLAG_PATH, True), patch(
            _BINARY_PATH, str(tmp_path / "no_such_file")
        ):
            with pytest.raises(FileNotFoundError, match="HAPROXY_BINARY_PATH"):
                get_haproxy_binary()

    # 3. Explicit binary path — not executable → FileNotFoundError
    def test_explicit_binary_path_not_executable_raises(self, tmp_path):
        binary = tmp_path / "haproxy"
        binary.write_bytes(b"")
        binary.chmod(0o644)

        with patch(_FLAG_PATH, True), patch(_BINARY_PATH, str(binary)):
            with pytest.raises(FileNotFoundError, match="HAPROXY_BINARY_PATH"):
                get_haproxy_binary()

    # 4. ray_haproxy package installed → uses bundled binary
    def test_ray_haproxy_package_used(self):
        fake_path = "/opt/ray_haproxy/bin/haproxy"
        mock_module = MagicMock()
        mock_module.get_haproxy_binary.return_value = fake_path

        with patch(_FLAG_PATH, True), patch(_BINARY_PATH, "haproxy"):
            with patch.dict("sys.modules", {"ray_haproxy": mock_module}):
                result = get_haproxy_binary()

        assert result == fake_path
        mock_module.get_haproxy_binary.assert_called_once()

    # 5. ray_haproxy not installed, system haproxy on PATH
    def test_falls_back_to_system_path(self):
        with patch(_FLAG_PATH, True), patch(_BINARY_PATH, "haproxy"):
            with patch.dict("sys.modules", {"ray_haproxy": None}):
                with patch(
                    "ray.serve._private.haproxy.shutil.which",
                    return_value="/usr/bin/haproxy",
                ):
                    result = get_haproxy_binary()

        assert result == "/usr/bin/haproxy"

    # 6. ray_haproxy not installed, not on PATH → FileNotFoundError
    def test_no_binary_raises(self):
        with patch(_FLAG_PATH, True), patch(_BINARY_PATH, "haproxy"):
            with patch.dict("sys.modules", {"ray_haproxy": None}):
                with patch(
                    "ray.serve._private.haproxy.shutil.which",
                    return_value=None,
                ):
                    with pytest.raises(FileNotFoundError, match=r"ray\[haproxy\]"):
                        get_haproxy_binary()

    # 7. ray_haproxy installed but binary missing → falls through to PATH
    def test_ray_haproxy_file_not_found_falls_through_to_path(self):
        mock_module = MagicMock()
        mock_module.get_haproxy_binary.side_effect = FileNotFoundError(
            "bundled binary missing"
        )

        with patch(_FLAG_PATH, True), patch(_BINARY_PATH, "haproxy"):
            with patch.dict("sys.modules", {"ray_haproxy": mock_module}):
                with patch(
                    "ray.serve._private.haproxy.shutil.which",
                    return_value="/usr/local/bin/haproxy",
                ):
                    result = get_haproxy_binary()

        assert result == "/usr/local/bin/haproxy"
