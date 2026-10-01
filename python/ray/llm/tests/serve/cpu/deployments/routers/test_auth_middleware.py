"""Tests for AuthMiddleware (bearer-token enforcement on the ingress)."""

import sys

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from ray.llm._internal.serve.core.ingress.middleware import (
    AUTHENTICATED_USER_ID,
    AuthMiddleware,
    add_auth_middleware,
    get_user_id,
)

API_KEY = "secret-key"
HEALTH_PATH = "/health"


def _build_app(api_key=None, api_key_env_var=None, exempt_paths=()):
    """Build a minimal app guarded by AuthMiddleware for testing.

    ``/v1/chat/completions`` echoes back the authenticated user id so tests can
    assert the ``request.state.user_id`` slot is populated; ``/health`` exists
    to exercise exempt paths.
    """
    app = FastAPI()
    add_auth_middleware(
        app,
        api_key=api_key,
        api_key_env_var=api_key_env_var,
        exempt_paths=exempt_paths,
    )

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        return {"user_id": get_user_id(request)}

    @app.get(HEALTH_PATH)
    async def health():
        return {"status": "ok"}

    return app


def _post(app, headers=None):
    with TestClient(app) as client:
        return client.post("/v1/chat/completions", headers=headers or {})


class TestAuthMiddleware:
    def test_no_key_configured_is_noop(self):
        """With no key configured the endpoint stays open (backwards compat)."""
        app = _build_app(api_key=None)
        # No header, and a bogus header, both succeed and no user is tagged.
        assert _post(app).status_code == 200
        resp = _post(app, {"Authorization": "Bearer anything"})
        assert resp.status_code == 200
        assert resp.json()["user_id"] is None

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="missing-header"),
            pytest.param({"Authorization": "Bearer wrong-key"}, id="wrong-token"),
            pytest.param({"Authorization": "Bearer "}, id="empty-token"),
            pytest.param({"Authorization": f"Basic {API_KEY}"}, id="wrong-scheme"),
            pytest.param({"Authorization": API_KEY}, id="no-scheme"),
        ],
    )
    def test_rejects_invalid_credentials(self, headers):
        app = _build_app(api_key=API_KEY)
        resp = _post(app, headers)
        assert resp.status_code == 401
        assert resp.headers["WWW-Authenticate"] == "Bearer"
        assert resp.json()["error"]["code"] == "invalid_api_key"

    def test_accepts_valid_key_and_tags_user(self):
        app = _build_app(api_key=API_KEY)
        resp = _post(app, {"Authorization": f"Bearer {API_KEY}"})
        assert resp.status_code == 200
        assert resp.json()["user_id"] == AUTHENTICATED_USER_ID

    def test_bearer_scheme_is_case_insensitive(self):
        app = _build_app(api_key=API_KEY)
        resp = _post(app, {"Authorization": f"bearer {API_KEY}"})
        assert resp.status_code == 200

    def test_exempt_path_bypasses_auth(self):
        app = _build_app(api_key=API_KEY, exempt_paths=(HEALTH_PATH,))
        with TestClient(app) as client:
            resp = client.get(HEALTH_PATH)
        assert resp.status_code == 200

    def test_options_preflight_bypasses_auth(self):
        """CORS preflight has no Authorization header and must not be 401'd."""
        app = _build_app(api_key=API_KEY)
        with TestClient(app) as client:
            resp = client.options("/v1/chat/completions")
        # Middleware lets it through; FastAPI itself may 405 the OPTIONS verb,
        # but it must never be rejected as unauthorized.
        assert resp.status_code != 401

    def test_add_auth_middleware_noop_without_key(self):
        """add_auth_middleware installs nothing when no key is given."""
        app = FastAPI()
        before = len(app.user_middleware)
        add_auth_middleware(app, api_key=None)
        assert len(app.user_middleware) == before

    def test_add_auth_middleware_installs_with_key(self):
        app = FastAPI()
        add_auth_middleware(app, api_key=API_KEY)
        assert any(mw.cls is AuthMiddleware for mw in app.user_middleware)

    def test_add_auth_middleware_installs_with_env_var_only(self):
        """Installed even without an explicit key, since the env may supply it
        at replica startup."""
        app = FastAPI()
        add_auth_middleware(app, api_key=None, api_key_env_var="VLLM_API_KEY")
        assert any(mw.cls is AuthMiddleware for mw in app.user_middleware)


ENV_VAR = "VLLM_API_KEY"


class TestApiKeyResolution:
    """The ingress resolves its key from an explicit value or the env var."""

    def test_env_var_enforced_when_no_explicit_key(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, API_KEY)
        app = _build_app(api_key=None, api_key_env_var=ENV_VAR)
        assert _post(app).status_code == 401
        assert _post(app, {"Authorization": f"Bearer {API_KEY}"}).status_code == 200

    def test_explicit_key_takes_precedence_over_env(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "env-key")
        app = _build_app(api_key="explicit-key", api_key_env_var=ENV_VAR)
        # The env value must not be accepted once an explicit key is set.
        assert _post(app, {"Authorization": "Bearer env-key"}).status_code == 401
        assert _post(app, {"Authorization": "Bearer explicit-key"}).status_code == 200

    def test_unset_env_is_noop(self, monkeypatch):
        monkeypatch.delenv(ENV_VAR, raising=False)
        app = _build_app(api_key=None, api_key_env_var=ENV_VAR)
        assert _post(app).status_code == 200


class TestInitWiring:
    """`init()` installs and configures the auth middleware on the real app."""

    def _init(self, **kwargs):
        from ray.llm._internal.serve.core.ingress.ingress import init

        return init(**kwargs)

    def _auth_mw(self, app):
        return next(
            (mw for mw in app.user_middleware if mw.cls is AuthMiddleware), None
        )

    def test_init_always_installs_auth_middleware(self):
        # Installed even without a key so the env var can enable it at runtime.
        assert self._auth_mw(self._init()) is not None

    def test_init_forwards_explicit_key_and_env_var(self):
        mw = self._auth_mw(self._init(api_key=API_KEY))
        assert mw.kwargs["api_key"] == API_KEY
        assert mw.kwargs["api_key_env_var"] == ENV_VAR

    def test_init_auth_runs_inside_cors_metrics_and_request_id(self, monkeypatch):
        # The HTTP metrics middleware is env-gated (ENABLE_VERBOSE_TELEMETRY);
        # force it on so we can assert auth sits inside it too.
        import ray.llm._internal.serve.observability.metrics.fast_api_metrics as m

        monkeypatch.setattr(m, "ENABLE_VERBOSE_TELEMETRY", True, raising=False)

        # Starlette inserts each add_middleware at index 0, so user_middleware is
        # outermost-first (higher index == more inner == runs later inbound).
        # Auth must sit INSIDE CORS, metrics and request-id so that a 401
        # short-circuit still passes back out through them: CORS adds its headers
        # to the rejection, metrics records it, and the request keeps its id.
        classes = [mw.cls.__name__ for mw in self._init().user_middleware]
        auth = classes.index("AuthMiddleware")
        assert auth > classes.index("CORSMiddleware")
        assert auth > classes.index("SetRequestIdMiddleware")
        # Present only when telemetry is enabled (guarded in case the forced flag
        # is not honored in a given environment).
        if "MeasureHTTPRequestMetricsMiddleware" in classes:
            assert auth > classes.index("MeasureHTTPRequestMetricsMiddleware")


class TestApplyIngressApiKeyToDirectStreaming:
    """`_apply_ingress_api_key` propagates the explicit key to the replica env so
    vLLM's native auth enforces it on direct-streaming paths."""

    def _config(self, runtime_env=None):
        from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

        return LLMConfig(
            model_loading_config=dict(model_id="m"), runtime_env=runtime_env
        )

    def _apply(self, config, api_key):
        from ray.llm._internal.serve.core.ingress.builder import (
            _apply_ingress_api_key,
        )

        return _apply_ingress_api_key(config, api_key)

    def test_noop_without_key(self):
        config = self._config()
        assert self._apply(config, None) is config

    def test_injects_vllm_api_key_env_var(self):
        out = self._apply(self._config(), API_KEY)
        assert out.runtime_env["env_vars"][ENV_VAR] == API_KEY

    def test_preserves_existing_env_vars(self):
        config = self._config(runtime_env={"env_vars": {"OTHER": "1"}})
        env = self._apply(config, API_KEY).runtime_env["env_vars"]
        assert env == {"OTHER": "1", ENV_VAR: API_KEY}

    def test_explicit_key_overrides_existing_env(self):
        config = self._config(runtime_env={"env_vars": {ENV_VAR: "old"}})
        assert self._apply(config, API_KEY).runtime_env["env_vars"][ENV_VAR] == API_KEY

    def test_original_config_unchanged(self):
        config = self._config()
        self._apply(config, API_KEY)
        assert config.runtime_env is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
