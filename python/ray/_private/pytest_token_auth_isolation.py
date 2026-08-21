"""Pytest plugin for token-authentication test isolation.

Loaded for every Ray test via ``addopts = -p ...`` in ``pytest.ini`` so the
behavior applies uniformly, with no per-directory conftest wiring (a conftest
only reaches tests in its own directory subtree, and many test directories don't
have or depend on one).

A local cluster started with a default-on ``ray.init()`` enables token auth: it
sets ``RAY_AUTH_MODE=token`` in ``os.environ`` and writes ``~/.ray/auth_token``.
Both persist in the driver process and, under ``bazel test`` where HOME is
unset, across test targets that share one home. Left unchecked they make later
tests spawn auth-enabled clusters they don't expect. This plugin resets that
state between independent tests while leaving a live cluster's state alone, and
offers an opt-in fixture to make ``requests`` calls carry the cluster's token.

Heavy imports are done lazily inside the fixtures so loading the plugin at pytest
startup stays cheap and can't break collection in minimal installs.
"""

import os

import pytest

_TOKEN_AUTH_ENV_VARS = ("RAY_AUTH_MODE", "RAY_AUTH_TOKEN", "RAY_AUTH_TOKEN_PATH")


def _default_token_path():
    return os.path.join(os.path.expanduser("~"), ".ray", "auth_token")


@pytest.fixture(scope="session", autouse=True)
def _isolate_token_auth_state():
    """Clear a leftover token at session start and restore it at session end.

    Session-scoped so it never disturbs a module- or session-scoped cluster
    mid-run. Restoring the original at the end keeps a developer's existing
    ``~/.ray/auth_token`` unmodified by a test run.
    """
    from ray._private.authentication_test_utils import reset_auth_token_state

    default_token = _default_token_path()
    original_token = None
    if os.path.exists(default_token):
        with open(default_token) as f:
            original_token = f.read()
        os.remove(default_token)
    reset_auth_token_state()
    try:
        yield
    finally:
        if original_token is None:
            if os.path.exists(default_token):
                os.remove(default_token)
        else:
            os.makedirs(os.path.dirname(default_token), exist_ok=True)
            with open(default_token, "w") as f:
                f.write(original_token)
        reset_auth_token_state()


@pytest.fixture(scope="session")
def _token_auth_env_baseline():
    return {k: os.environ.get(k) for k in _TOKEN_AUTH_ENV_VARS}


@pytest.fixture(autouse=True)
def _restore_token_auth_env(_token_auth_env_baseline):
    """Reset the auth env vars and a generated token between independent tests.

    Only when no cluster is live: a module- or session-scoped cluster keeps Ray
    initialized across its tests, and its processes and subprocess drivers still
    need both the env var (to connect) and the token file (its token value), so
    leave everything in place while it runs.
    """
    import ray
    from ray._private.authentication_test_utils import reset_auth_token_state

    yield
    if ray.is_initialized():
        return
    for key, value in _token_auth_env_baseline.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    default_token = _default_token_path()
    if os.path.exists(default_token):
        os.remove(default_token)
    reset_auth_token_state()


@pytest.fixture
def auth_token_requests(monkeypatch):
    """Make ``requests`` calls in a test carry the cluster's auth token.

    A local cluster started with a default-on ``ray.init()`` enables token auth,
    so raw ``requests`` to the dashboard without a token get 401. A test that
    hits the dashboard over HTTP can request this fixture to inject the
    ``Authorization`` header automatically. The token is read per request (so
    fixture ordering doesn't matter) and nothing is added when no token is
    available, so it's a no-op with auth disabled. An explicit ``Authorization``
    header from the caller is preserved (negative auth tests still work).
    """
    import requests

    from ray._raylet import AuthenticationTokenLoader

    original_request = requests.sessions.Session.request

    def request_with_token(self, method, url, **kwargs):
        header = AuthenticationTokenLoader.instance().get_token_for_http_header(
            ignore_auth_mode=True
        )
        if header:
            kwargs["headers"] = {**header, **(kwargs.get("headers") or {})}
        return original_request(self, method, url, **kwargs)

    monkeypatch.setattr(requests.sessions.Session, "request", request_with_token)
    yield
