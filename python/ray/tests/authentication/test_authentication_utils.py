import sys

import pytest

from ray._private.authentication import authentication_utils


def test_token_auth_enabled_fail_closed_without_raylet(monkeypatch):
    """If RAY_AUTH_MODE=token is set but ray._raylet is unavailable (broken or
    partial install), auth must fail closed (report enabled so middlewares
    enforce) rather than silently disabling itself."""
    monkeypatch.setattr(authentication_utils, "_RAYLET_AVAILABLE", False)
    monkeypatch.setenv("RAY_AUTH_MODE", "token")
    assert authentication_utils.is_token_auth_enabled() is True


def test_token_auth_disabled_without_raylet_when_unset(monkeypatch):
    """Doc builds / minimal installs without ray._raylet keep the no-op
    behaviour when the operator has not requested token auth."""
    monkeypatch.setattr(authentication_utils, "_RAYLET_AVAILABLE", False)
    monkeypatch.delenv("RAY_AUTH_MODE", raising=False)
    assert authentication_utils.is_token_auth_enabled() is False


def test_token_auth_disabled_without_raylet_when_explicitly_disabled(monkeypatch):
    monkeypatch.setattr(authentication_utils, "_RAYLET_AVAILABLE", False)
    monkeypatch.setenv("RAY_AUTH_MODE", "disabled")
    assert authentication_utils.is_token_auth_enabled() is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
