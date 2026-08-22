"""Pytest plugin: carry the cluster auth token on HTTP calls in tests.

Loaded for every Ray test via ``addopts = -p ...`` in ``pytest.ini``. A local
cluster started with a default-on ``ray.init()`` enables token auth, so raw
``requests``/``httpx``/``aiohttp`` calls to the dashboard without a token get
401. This plugin autouse-patches the HTTP clients so every test's calls carry
the token when one is available.

It only reads the token (via the loader's non-raising ``get_token_for_http_header``)
and adds an ``Authorization`` header; it never touches the token file, env vars,
or auth mode, so it can't affect auth state or crash. An explicit ``Authorization``
header from the caller is preserved. A test that must talk to the dashboard
*without* a token (e.g. asserting 401) opts out with ``@pytest.mark.no_auth_token``.

Only ``pytest`` is imported at module load; heavier imports happen inside the
fixture so loading the plugin can't affect collection in minimal installs.
"""

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "no_auth_token: don't auto-attach the cluster auth token to this test's "
        "HTTP requests (for tests that assert unauthenticated behavior).",
    )


@pytest.fixture(autouse=True)
def _auth_token_requests(request, monkeypatch):
    if request.node.get_closest_marker("no_auth_token"):
        yield
        return

    import ipaddress
    from urllib.parse import urlsplit

    from ray._raylet import AuthenticationTokenLoader

    def is_local_cluster_url(url):
        # Only attach the token to the local Ray dashboard, never to external
        # services (e.g. dataset downloads from a public host).
        host = urlsplit(str(url)).hostname or ""
        if host == "localhost":
            return True
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            return False
        return ip.is_loopback or ip.is_private

    def auth_header():
        return AuthenticationTokenLoader.instance().get_token_for_http_header(
            ignore_auth_mode=True
        )

    def sync_with_token(original):
        def wrapped(self, method, url, **kwargs):
            if is_local_cluster_url(url):
                header = auth_header()
                if header:
                    kwargs["headers"] = {**header, **(kwargs.get("headers") or {})}
            return original(self, method, url, **kwargs)

        return wrapped

    import requests

    monkeypatch.setattr(
        requests.sessions.Session,
        "request",
        sync_with_token(requests.sessions.Session.request),
    )

    try:
        import httpx
    except ImportError:
        httpx = None
    if httpx is not None:
        monkeypatch.setattr(
            httpx.Client, "request", sync_with_token(httpx.Client.request)
        )

        original_async = httpx.AsyncClient.request

        async def async_with_token(self, method, url, **kwargs):
            if is_local_cluster_url(url):
                header = auth_header()
                if header:
                    kwargs["headers"] = {**header, **(kwargs.get("headers") or {})}
            return await original_async(self, method, url, **kwargs)

        monkeypatch.setattr(httpx.AsyncClient, "request", async_with_token)

    try:
        import aiohttp
    except ImportError:
        aiohttp = None
    if aiohttp is not None:
        original_aiohttp = aiohttp.ClientSession._request

        async def aiohttp_with_token(self, method, str_or_url, **kwargs):
            if is_local_cluster_url(str_or_url):
                header = auth_header()
                if header:
                    existing = kwargs.get("headers") or {}
                    kwargs["headers"] = {**header, **dict(existing)}
            return await original_aiohttp(self, method, str_or_url, **kwargs)

        monkeypatch.setattr(aiohttp.ClientSession, "_request", aiohttp_with_token)

    yield
