"""Unit tests for get_haproxy_binary() in ray.serve._private.haproxy."""
import stat
from unittest.mock import MagicMock, patch

import pytest

from ray.serve._private.haproxy import get_haproxy_binary

_ENV_VAR = "RAY_SERVE_HAPROXY_BINARY"
_FLAG_PATH = "ray.serve._private.haproxy.RAY_SERVE_EXPERIMENTAL_PIP_HAPROXY"


def _clear_env(monkeypatch):
    monkeypatch.delenv(_ENV_VAR, raising=False)


class TestGetHaproxyBinary:
    """Tests for get_haproxy_binary() resolution logic."""

    # 0. Flag off → returns "haproxy" (system PATH, no change from default)
    def test_flag_off_returns_system_haproxy(self):
        with patch(_FLAG_PATH, False):
            assert get_haproxy_binary() == "haproxy"

    # 1. Env var — valid executable path
    def test_env_var_valid(self, tmp_path, monkeypatch):
        binary = tmp_path / "haproxy"
        binary.write_bytes(b"")
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR)

        monkeypatch.setenv(_ENV_VAR, str(binary))
        with patch(_FLAG_PATH, True):
            assert get_haproxy_binary() == str(binary)

    # 2. Env var — path does not exist → FileNotFoundError
    def test_env_var_nonexistent_raises(self, tmp_path, monkeypatch):
        monkeypatch.setenv(_ENV_VAR, str(tmp_path / "no_such_file"))
        with patch(_FLAG_PATH, True):
            with pytest.raises(FileNotFoundError, match=_ENV_VAR):
                get_haproxy_binary()

    # 3. Env var — file exists but is not executable → FileNotFoundError
    def test_env_var_not_executable_raises(self, tmp_path, monkeypatch):
        binary = tmp_path / "haproxy"
        binary.write_bytes(b"")
        binary.chmod(0o644)

        monkeypatch.setenv(_ENV_VAR, str(binary))
        with patch(_FLAG_PATH, True):
            with pytest.raises(FileNotFoundError, match=_ENV_VAR):
                get_haproxy_binary()

    # 4. ray_haproxy package installed → uses bundled binary
    def test_ray_haproxy_package_used(self, monkeypatch):
        _clear_env(monkeypatch)

        fake_path = "/opt/ray_haproxy/bin/haproxy"
        mock_module = MagicMock()
        mock_module.get_haproxy_binary.return_value = fake_path

        with patch(_FLAG_PATH, True):
            with patch.dict("sys.modules", {"ray_haproxy": mock_module}):
                result = get_haproxy_binary()

        assert result == fake_path
        mock_module.get_haproxy_binary.assert_called_once()

    # 5. ray_haproxy not installed, system haproxy on PATH
    def test_falls_back_to_system_path(self, monkeypatch):
        _clear_env(monkeypatch)

        with patch(_FLAG_PATH, True):
            with patch.dict("sys.modules", {"ray_haproxy": None}):
                with patch(
                    "ray.serve._private.haproxy.shutil.which",
                    return_value="/usr/bin/haproxy",
                ):
                    result = get_haproxy_binary()

        assert result == "/usr/bin/haproxy"

    # 6. ray_haproxy not installed, not on PATH → FileNotFoundError
    def test_no_binary_raises(self, monkeypatch):
        _clear_env(monkeypatch)

        with patch(_FLAG_PATH, True):
            with patch.dict("sys.modules", {"ray_haproxy": None}):
                with patch(
                    "ray.serve._private.haproxy.shutil.which",
                    return_value=None,
                ):
                    with pytest.raises(FileNotFoundError, match=r"ray\[haproxy\]"):
                        get_haproxy_binary()

    # 7. ray_haproxy installed but its binary is missing → falls through to PATH
    def test_ray_haproxy_file_not_found_falls_through_to_path(self, monkeypatch):
        _clear_env(monkeypatch)

        mock_module = MagicMock()
        mock_module.get_haproxy_binary.side_effect = FileNotFoundError(
            "bundled binary missing"
        )

        with patch(_FLAG_PATH, True):
            with patch.dict("sys.modules", {"ray_haproxy": mock_module}):
                with patch(
                    "ray.serve._private.haproxy.shutil.which",
                    return_value="/usr/local/bin/haproxy",
                ):
                    result = get_haproxy_binary()

        assert result == "/usr/local/bin/haproxy"
