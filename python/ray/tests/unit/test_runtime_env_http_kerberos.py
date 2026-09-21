"""Exercise the HTTP transport without requiring a KDC or a Ray cluster."""

import io
import ssl
import sys
import types
import zipfile
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import pytest
import requests
from pytest_httpserver import HTTPServer

from ray._private.runtime_env.protocol import (
    RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR as BEARER_TOKEN,
    RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR as KERBEROS_HOSTS,
    ProtocolsProvider,
)
from ray._private.runtime_env.py_modules import PyModulesPlugin
from ray._private.runtime_env.working_dir import WorkingDirPlugin

PACKAGE_URI = "https://files.example.org/webhdfs/v1/code.zip?op=OPEN"


@pytest.fixture(autouse=True)
def clean_auth_environment(monkeypatch):
    monkeypatch.delenv(KERBEROS_HOSTS, raising=False)
    monkeypatch.delenv(BEARER_TOKEN, raising=False)
    # Do not let a developer's netrc or proxy settings affect mock downloads.
    for variable in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        monkeypatch.delenv(variable, raising=False)
        monkeypatch.delenv(variable.upper(), raising=False)
    monkeypatch.setattr(requests.utils, "get_netrc_auth", lambda *args: None)
    monkeypatch.setattr(requests.sessions, "get_netrc_auth", lambda *args: None)


@pytest.fixture
def kerberos(monkeypatch):
    instances = []

    class FakeKerberosAuth(requests.auth.AuthBase):
        def __init__(self):
            instances.append(self)

        def __call__(self, request):
            # Model a host-specific token and an authentication response hook.
            host = urlparse(request.url).hostname
            request.headers["Authorization"] = f"Negotiate {host}:{len(instances)}"
            request.register_hook("response", self.handle_response)
            return request

        def handle_response(self, response, **kwargs):
            return response

    monkeypatch.setitem(
        sys.modules,
        "requests_kerberos",
        types.SimpleNamespace(HTTPKerberosAuth=FakeKerberosAuth),
    )
    monkeypatch.setenv(
        KERBEROS_HOSTS,
        " FILES.example.org, gateway.example.org ",
    )
    return instances


@pytest.fixture
def http_transport(monkeypatch):
    """Use real smart_open and Requests, replacing only network transmission."""
    routes = {}
    sent = []

    def send(adapter, request, **kwargs):
        sent.append(request)
        status, headers, payload = routes[request.url]
        response = requests.Response()
        response.status_code = status
        response.headers.update(headers)
        response.raw = io.BytesIO(payload)
        response.url = request.url
        response.request = request
        response.connection = adapter
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", send)
    return routes, sent


def download(tmp_path, uri=PACKAGE_URI):
    dest = tmp_path / "package.zip"
    ProtocolsProvider.download_remote_uri(urlparse(uri).scheme, uri, str(dest))
    return dest.read_bytes()


@pytest.mark.parametrize(
    "hosts,uri,token",
    [
        ("", PACKAGE_URI, None),
        ("elsewhere.example.org", PACKAGE_URI, None),
        ("example.org", PACKAGE_URI, None),
        ("files.example.org.evil.org", PACKAGE_URI, None),
        ("files.example.org", PACKAGE_URI.replace("https:", "http:"), None),
        ("", PACKAGE_URI, "test-token"),
        ("elsewhere.example.org", PACKAGE_URI, "test-token"),
        ("https://elsewhere.example.org", PACKAGE_URI, None),
        ("elsewhere.example.org,*.example.org", PACKAGE_URI, "test-token"),
        (
            "files.example.org,*.example.org",
            PACKAGE_URI.replace("https:", "http:"),
            None,
        ),
    ],
)
def test_download_without_kerberos(
    tmp_path, monkeypatch, http_transport, hosts, uri, token
):
    monkeypatch.setenv(KERBEROS_HOSTS, hosts)
    monkeypatch.setitem(sys.modules, "requests_kerberos", None)
    if token:
        monkeypatch.setenv(BEARER_TOKEN, token)
    routes, sent = http_transport
    routes[uri] = (200, {}, b"package")

    assert download(tmp_path, uri) == b"package"
    assert sent[0].headers.get("Authorization") == (
        f"Bearer {token}" if token else None
    )


@pytest.mark.parametrize(
    "host", ["files.example.org", "gateway.example.org", "vip.example.org"]
)
def test_configured_httpfs_host(tmp_path, monkeypatch, kerberos, http_transport, host):
    hosts = " FILES.example.org, gateway.example.org "
    monkeypatch.setenv(
        KERBEROS_HOSTS, "vip.example.org" if host.startswith("vip.") else hosts
    )
    uri = PACKAGE_URI.replace("files.example.org", host)
    routes, sent = http_transport
    routes[uri] = (200, {}, b"package")

    assert download(tmp_path, uri) == b"package"
    assert len(sent) == len(kerberos) == 1
    assert sent[0].headers["Authorization"] == f"Negotiate {host}:1"


def test_embedded_credentials_rejected(tmp_path, kerberos, http_transport):
    with pytest.raises(ValueError, match="without embedded credentials"):
        download(tmp_path, PACKAGE_URI.replace("https://", "https://user:secret@"))
    assert http_transport[1] == []


def test_bearer_conflict(tmp_path, monkeypatch, kerberos, http_transport):
    monkeypatch.setenv(BEARER_TOKEN, "test-secret")
    with pytest.raises(ValueError, match="cannot be used together") as exc:
        download(tmp_path)
    assert "test-secret" not in str(exc.value)
    assert http_transport[1] == []


def test_missing_kerberos_dependency(tmp_path, monkeypatch, kerberos):
    monkeypatch.setitem(sys.modules, "requests_kerberos", None)
    with pytest.raises(ImportError, match="pip install requests-kerberos") as exc:
        download(tmp_path)
    assert "preinstalled" in str(exc.value)


@pytest.mark.parametrize(
    "hosts",
    [
        "https://files.example.org",
        "*.example.org",
        "files.example.org:443",
        "127.0.0.1",
    ],
)
def test_invalid_host_configuration(tmp_path, monkeypatch, http_transport, hosts):
    monkeypatch.setenv(KERBEROS_HOSTS, f"files.example.org,{hosts}")
    with pytest.raises(ValueError, match=KERBEROS_HOSTS):
        download(tmp_path)
    assert http_transport[1] == []


@pytest.mark.parametrize("status", [401, 403, 500])
def test_http_failure_does_not_fall_back(tmp_path, kerberos, http_transport, status):
    routes, sent = http_transport
    routes[PACKAGE_URI] = (status, {}, b"error")
    with pytest.raises(requests.HTTPError):
        download(tmp_path)
    assert len(sent) == 1
    assert not (tmp_path / "package.zip").exists()


@pytest.mark.parametrize("stage", ["credentials", "mutual_authentication"])
def test_kerberos_failure_does_not_fall_back(
    tmp_path, monkeypatch, kerberos, http_transport, stage
):
    def fail(*args, **kwargs):
        raise RuntimeError("Kerberos authentication failed")

    routes, sent = http_transport
    routes[PACKAGE_URI] = (200, {}, b"package")
    if stage == "credentials":
        monkeypatch.setattr(sys.modules["requests_kerberos"], "HTTPKerberosAuth", fail)
    else:
        monkeypatch.setattr(
            sys.modules["requests_kerberos"].HTTPKerberosAuth, "handle_response", fail
        )
    with pytest.raises(RuntimeError, match="Kerberos authentication failed"):
        download(tmp_path)
    assert len(sent) == (0 if stage == "credentials" else 1)
    assert not (tmp_path / "package.zip").exists()


@pytest.mark.parametrize(
    "certificate",
    [
        "trusted",
        "unknown_ca",
        "wrong_hostname",
        "expired",
    ],
)
def test_local_https_download(tmp_path, monkeypatch, kerberos, certificate):
    """Check real TLS verification with simulated Kerberos tokens."""
    import trustme

    ca = trustme.CA()
    if certificate == "wrong_hostname":
        cert = ca.issue_cert("other.example.org", common_name="localhost")
    elif certificate == "expired":
        cert = ca.issue_cert(
            "localhost", not_after=datetime.now(timezone.utc) - timedelta(days=1)
        )
    else:
        cert = ca.issue_cert("localhost", common_name="wrong.example.org")
    ca_path = tmp_path / "ca.pem"
    (trustme.CA() if certificate == "unknown_ca" else ca).cert_pem.write_to_path(
        ca_path
    )
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(ca_path))
    monkeypatch.setenv(KERBEROS_HOSTS, "localhost")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert.configure_cert(context)
    with HTTPServer(host="localhost", ssl_context=context) as server:
        server.expect_request("/code.zip").respond_with_data(b"package")
        if certificate == "trusted":
            assert download(tmp_path, server.url_for("/code.zip")) == b"package"
            assert server.log[0][0].headers["Authorization"] == "Negotiate localhost:1"
        else:
            with pytest.raises(requests.exceptions.SSLError):
                download(tmp_path, server.url_for("/code.zip"))
            assert not server.log


@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_class", [WorkingDirPlugin, PyModulesPlugin])
async def test_plugins_download_kerberos_zip(
    tmp_path, kerberos, http_transport, plugin_class
):
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as archive:
        archive.writestr("code/hello.py", "value = 42\n")
    routes, sent = http_transport
    routes[PACKAGE_URI] = (200, {}, content.getvalue())
    plugin = plugin_class(str(tmp_path), None)

    assert await plugin.create(PACKAGE_URI, {}, None) > 0
    assert next(tmp_path.rglob("hello.py")).read_text() == "value = 42\n"
    assert len(sent) == len(kerberos) == 1
    assert sent[0].headers["Authorization"].startswith("Negotiate files.example.org:")
    assert sent[0].url.endswith("code.zip?op=OPEN")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
