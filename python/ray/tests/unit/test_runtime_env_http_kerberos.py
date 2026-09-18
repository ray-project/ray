"""Exercise the HTTP transport without requiring a KDC or a Ray cluster."""

import io
import socket
import ssl
import sys
import types
import zipfile
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

import pytest
import requests
import smart_open
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
        " FILES.example.org, datanode.example.org ",
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
    "target,allowed",
    [
        ("/code.zip", True),
        ("https://files.example.org:50475/code.zip?op=OPEN", True),
        ("http://files.example.org/code.zip", False),
        ("https://unlisted.example.org/code.zip", False),
        ("https://files.example.org.evil.org/code.zip", False),
        ("https://user:secret@datanode.example.org/code.zip", False),
    ],
)
def test_redirects(tmp_path, kerberos, http_transport, target, allowed):
    routes, sent = http_transport
    intermediate = "https://datanode.example.org/redirect.zip"
    routes[PACKAGE_URI] = (307, {"Location": intermediate}, b"")
    routes[intermediate] = (307, {"Location": target}, b"")
    routes[urljoin(intermediate, target)] = (200, {}, b"package")

    if allowed:
        assert download(tmp_path) == b"package"
        assert len(sent) == 3
        assert len({request.headers["Authorization"] for request in sent}) == 3
        assert all(len(request.hooks["response"]) == 1 for request in sent)
    else:
        with pytest.raises(ValueError, match="Kerberos downloads and redirects") as exc:
            download(tmp_path)
        assert "secret" not in str(exc.value)
        assert len(sent) == 2  # Reject the target before sending to it.


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


@pytest.mark.parametrize("dependency", ["requests_kerberos", "smart_open", "requests"])
def test_missing_or_old_dependency(tmp_path, monkeypatch, kerberos, dependency):
    if dependency == "requests_kerberos":
        monkeypatch.setitem(sys.modules, dependency, None)
    elif dependency == "smart_open":
        monkeypatch.setattr(smart_open.http, "open", lambda uri, mode: None)
    else:
        monkeypatch.delattr(
            requests.adapters.HTTPAdapter, "build_connection_pool_key_attributes"
        )
    with pytest.raises(ImportError, match="pip install") as exc:
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
    monkeypatch.setenv(KERBEROS_HOSTS, hosts)
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
        "default_ca",
        "unknown_ca",
        "wrong_hostname",
        "cn_only",
        "expired",
        "redirect_san_mismatch",
    ],
)
def test_local_https_download(tmp_path, monkeypatch, kerberos, certificate):
    """Check real TLS verification and redirects with simulated Kerberos tokens."""
    import trustme

    ca = trustme.CA()
    if certificate == "cn_only":
        # No SAN identities: only the CN can match localhost.
        cert = ca.issue_cert(common_name="localhost")
    elif certificate == "wrong_hostname":
        cert = ca.issue_cert("other.example.org", common_name="localhost")
    elif certificate == "redirect_san_mismatch":
        cert = ca.issue_cert("localhost", common_name="datanode.example.org")
    elif certificate == "expired":
        cert = ca.issue_cert(
            "localhost", not_after=datetime.now(timezone.utc) - timedelta(days=1)
        )
    else:
        cert = ca.issue_cert(
            "localhost", "datanode.example.org", common_name="wrong.example.org"
        )
    ca_path = tmp_path / "ca.pem"
    (trustme.CA() if certificate == "unknown_ca" else ca).cert_pem.write_to_path(
        ca_path
    )
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(ca_path))
    if certificate == "default_ca":
        # Exercise verify=True as well as an explicit CA bundle. Requests 2.32
        # uses a preloaded context for its default trust store.
        monkeypatch.delenv("REQUESTS_CA_BUNDLE")
        monkeypatch.delenv("CURL_CA_BUNDLE", raising=False)
        monkeypatch.setattr(requests.utils, "DEFAULT_CA_BUNDLE_PATH", str(ca_path))
        monkeypatch.setattr(requests.adapters, "DEFAULT_CA_BUNDLE_PATH", str(ca_path))
    monkeypatch.setenv("NO_PROXY", "localhost,datanode.example.org")
    monkeypatch.setenv(KERBEROS_HOSTS, "localhost,datanode.example.org")
    getaddrinfo = socket.getaddrinfo

    def resolve(host, *args, **kwargs):
        if host == "datanode.example.org":
            host = "127.0.0.1"
        return getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert.configure_cert(context)
    with HTTPServer(host="localhost", ssl_context=context) as server:
        target = server.url_for("/code.zip").replace(
            "localhost", "datanode.example.org"
        )
        server.expect_request("/redirect.zip").respond_with_data(
            status=307, headers={"Location": target}
        )
        server.expect_request("/code.zip").respond_with_data(b"package")
        if certificate in ("trusted", "default_ca"):
            assert download(tmp_path, server.url_for("/redirect.zip")) == b"package"
            assert [request.headers["Authorization"] for request, _ in server.log] == [
                "Negotiate localhost:1",
                "Negotiate datanode.example.org:2",
            ]
        else:
            with pytest.raises(requests.exceptions.SSLError):
                download(tmp_path, server.url_for("/redirect.zip"))
            assert len(server.log) == (
                1 if certificate == "redirect_san_mismatch" else 0
            )


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
