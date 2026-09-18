"""Exercise the HTTP transport without requiring a KDC or a Ray cluster."""

import io
import socket
import ssl
import sys
import threading
import types
import zipfile
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pytest
import requests
import smart_open

from ray._private.runtime_env.protocol import (
    RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR,
    RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR,
    ProtocolsProvider,
)

PACKAGE_URI = "https://files.example.org/webhdfs/v1/code.zip?op=OPEN"


@pytest.fixture(autouse=True)
def clean_auth_environment(monkeypatch):
    monkeypatch.delenv(RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR, raising=False)
    monkeypatch.delenv(RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR, raising=False)
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
            request.headers[
                "Authorization"
            ] = f"Negotiate {urlparse(request.url).hostname}:{len(instances)}"
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
        RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR,
        "files.example.org,datanode.example.org",
    )
    return instances


@pytest.fixture
def http_transport(monkeypatch):
    """Use real smart_open and Requests, replacing only network transmission."""
    routes = {}
    sent = []

    def send(adapter, request, **kwargs):
        sent.append((request, kwargs))
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
    monkeypatch.setenv(RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR, hosts)
    monkeypatch.setitem(sys.modules, "requests_kerberos", None)
    if token:
        monkeypatch.setenv(RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR, token)
    routes, sent = http_transport
    routes[uri] = (200, {}, b"package")

    assert download(tmp_path, uri) == b"package"
    assert sent[0][0].headers.get("Authorization") == (
        f"Bearer {token}" if token else None
    )


def test_kerberos_download(tmp_path, monkeypatch, kerberos, http_transport):
    monkeypatch.setenv(
        RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR, " FILES.example.org, "
    )
    routes, sent = http_transport
    routes[PACKAGE_URI] = (200, {}, b"package")

    assert download(tmp_path) == b"package"
    assert len(kerberos) == 1
    request, kwargs = sent[0]
    assert request.headers["Authorization"].startswith("Negotiate files.example.org:")
    assert request.headers["Accept"] == "*/*"
    assert kwargs["verify"] is not False
    assert kwargs["timeout"] == 60


@pytest.mark.parametrize(
    "target",
    [
        "/download/code.zip",
        "https://datanode.example.org:50475/code.zip?op=OPEN",
    ],
)
def test_allowed_redirect(tmp_path, kerberos, http_transport, target):
    from urllib.parse import urljoin

    routes, sent = http_transport
    routes[PACKAGE_URI] = (307, {"Location": target}, b"")
    routes[urljoin(PACKAGE_URI, target)] = (200, {}, b"package")

    assert download(tmp_path) == b"package"
    assert len(sent) == len(kerberos) == 2
    first, second = (entry[0] for entry in sent)
    assert first.headers["Authorization"] != second.headers["Authorization"]
    assert second.hooks["response"] == [kerberos[1].handle_response]


@pytest.mark.parametrize(
    "target",
    [
        "http://files.example.org/code.zip",
        "https://unlisted.example.org/code.zip",
        "https://files.example.org.evil.org/code.zip",
        "https://user:secret@datanode.example.org/code.zip",
    ],
)
def test_reject_redirect_before_sending(tmp_path, kerberos, http_transport, target):
    routes, sent = http_transport
    routes[PACKAGE_URI] = (307, {"Location": target}, b"")

    with pytest.raises(ValueError, match="Kerberos downloads and redirects") as exc:
        download(tmp_path)
    assert len(sent) == 1
    assert "secret" not in str(exc.value)
    assert not (tmp_path / "package.zip").exists()


def test_validate_every_redirect(tmp_path, kerberos, http_transport):
    routes, sent = http_transport
    target = "https://datanode.example.org/code.zip"
    routes[PACKAGE_URI] = (302, {"Location": target}, b"")
    routes[target] = (302, {"Location": "http://datanode.example.org/code.zip"}, b"")

    with pytest.raises(ValueError, match="Kerberos downloads and redirects"):
        download(tmp_path)
    assert len(sent) == 2


def test_embedded_credentials_rejected(tmp_path, kerberos, http_transport):
    with pytest.raises(ValueError, match="without embedded credentials"):
        download(tmp_path, PACKAGE_URI.replace("https://", "https://user:secret@"))
    assert http_transport[1] == []


def test_bearer_conflict(tmp_path, monkeypatch, kerberos, http_transport):
    monkeypatch.setenv(RAY_RUNTIME_ENV_BEARER_TOKEN_ENV_VAR, "test-secret")
    with pytest.raises(ValueError, match="cannot be used together") as exc:
        download(tmp_path)
    assert "test-secret" not in str(exc.value)
    assert http_transport[1] == []


@pytest.mark.parametrize("dependency", ["requests_kerberos", "smart_open"])
def test_missing_dependency(tmp_path, monkeypatch, kerberos, dependency):
    monkeypatch.setitem(sys.modules, dependency, None)
    with pytest.raises(ImportError, match="pip install") as exc:
        download(tmp_path)
    assert "preinstalled" in str(exc.value)


def test_old_smart_open(tmp_path, monkeypatch, kerberos, http_transport):
    # smart_open 6.2 and 7.0 don't accept sessions; never silently ignore the guard.
    monkeypatch.setattr(smart_open.http, "open", lambda uri, mode: None)
    with pytest.raises(ImportError, match="smart_open.*>=7.1.0"):
        download(tmp_path)
    assert http_transport[1] == []


def test_old_requests(tmp_path, monkeypatch, kerberos, http_transport):
    monkeypatch.delattr(
        requests.adapters.HTTPAdapter, "build_connection_pool_key_attributes"
    )
    with pytest.raises(ImportError, match="requests>=2.32.3"):
        download(tmp_path)
    assert http_transport[1] == []


@pytest.mark.parametrize(
    "hosts", ["https://files.example.org", "*.example.org", "files.example.org:443"]
)
def test_invalid_host_configuration(tmp_path, monkeypatch, http_transport, hosts):
    monkeypatch.setenv(RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR, hosts)
    with pytest.raises(ValueError, match="comma-separated hostnames"):
        download(tmp_path)
    assert http_transport[1] == []


def test_kerberos_hosts_require_dns_names(tmp_path, monkeypatch, http_transport):
    monkeypatch.setenv(RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR, "127.0.0.1")
    with pytest.raises(ValueError, match="requires DNS hostnames"):
        download(tmp_path, "https://127.0.0.1/package.zip")
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


def cn_only_certificate(ca):
    """Issue a CN-only certificate without even an empty SAN extension."""
    import trustme
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization

    leaf = ca.issue_cert(common_name="localhost")
    original = x509.load_pem_x509_certificate(leaf.cert_chain_pems[0].bytes())
    builder = (
        x509.CertificateBuilder()
        .subject_name(original.subject)
        .issuer_name(original.issuer)
        .public_key(original.public_key())
        .serial_number(original.serial_number)
        .not_valid_before(original.not_valid_before_utc)
        .not_valid_after(original.not_valid_after_utc)
    )
    for extension in original.extensions:
        if extension.oid != x509.ExtensionOID.SUBJECT_ALTERNATIVE_NAME:
            builder = builder.add_extension(extension.value, extension.critical)
    key = serialization.load_pem_private_key(ca.private_key_pem.bytes(), password=None)
    certificate = builder.sign(key, hashes.SHA256())
    return trustme.LeafCert(
        leaf.private_key_pem.bytes(),
        certificate.public_bytes(serialization.Encoding.PEM),
        [],
    )


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
        cert = cn_only_certificate(ca)
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
    monkeypatch.setenv(
        RAY_RUNTIME_ENV_HTTP_KERBEROS_HOSTS_ENV_VAR, "localhost,datanode.example.org"
    )
    getaddrinfo = socket.getaddrinfo

    def resolve(host, *args, **kwargs):
        if host == "datanode.example.org":
            host = "127.0.0.1"
        return getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append(self.headers.get("Authorization"))
            if self.path == "/redirect.zip":
                self.send_response(307)
                self.send_header(
                    "Location",
                    f"https://datanode.example.org:{self.server.server_port}/code.zip?op=OPEN",
                )
                self.end_headers()
            else:
                self.send_response(200)
                self.send_header("Content-Length", "7")
                self.end_headers()
                self.wfile.write(b"package")

        def log_message(self, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        cert.configure_cert(context)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            uri = f"https://localhost:{server.server_port}/redirect.zip"
            if certificate in ("trusted", "default_ca"):
                assert download(tmp_path, uri) == b"package"
                assert seen == [
                    "Negotiate localhost:1",
                    "Negotiate datanode.example.org:2",
                ]
            else:
                with pytest.raises(requests.exceptions.SSLError):
                    download(tmp_path, uri)
                assert len(seen) == (1 if certificate == "redirect_san_mismatch" else 0)
        finally:
            server.shutdown()
            thread.join()


@pytest.mark.asyncio
@pytest.mark.parametrize("plugin_name", ["working_dir", "py_modules"])
async def test_plugins_download_kerberos_zip(
    tmp_path, kerberos, http_transport, plugin_name
):
    from ray._private.runtime_env.py_modules import PyModulesPlugin
    from ray._private.runtime_env.working_dir import WorkingDirPlugin

    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as archive:
        archive.writestr("code/hello.py", "value = 42\n")
    routes, sent = http_transport
    routes[PACKAGE_URI] = (200, {}, content.getvalue())
    plugin_class = {
        "working_dir": WorkingDirPlugin,
        "py_modules": PyModulesPlugin,
    }[plugin_name]
    plugin = plugin_class(str(tmp_path), None)

    assert await plugin.create(PACKAGE_URI, {}, None) > 0
    assert next(tmp_path.rglob("hello.py")).read_text() == "value = 42\n"
    assert len(sent) == 1
    assert sent[0][0].url.endswith("code.zip?op=OPEN")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
