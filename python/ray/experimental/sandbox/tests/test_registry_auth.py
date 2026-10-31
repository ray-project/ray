"""Pulls from registries that want credentials, served by a local registry.

The registry authenticates with HTTP basic, as Amazon ECR does, or with a
bearer token service, as Docker Hub and GHCR do. Credentials come from a
Docker config in a temporary ``DOCKER_CONFIG`` and from fake credential
helpers on PATH. Docker Hub references reach the registry through
``RAY_SANDBOX_REGISTRY_MIRROR``, which accepts a plain-HTTP registry.
"""

import base64
import hashlib
import io
import json
import os
import shlex
import sys
import tarfile
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from ray.experimental.sandbox._internal.docker_config import (
    RegistryCredentials,
    registry_credentials,
)
from ray.experimental.sandbox._internal.image_utils import (
    ROOTFS_IMAGE,
    _describe_pull_error,
    get_registry_auth_headers,
    pull_and_extract_container_image,
)
from ray.experimental.sandbox.exceptions import SandboxCreationError

_USER = "AWS"
_PASSWORD = "registry-s3cret"
_TOKEN = "pull-token"
_REFRESH_TOKEN = "refresh-s3cret"
_REPO = "team/app"
_FILE = b"from a private image\n"
_FORM_TYPE = "application/x-www-form-urlencoded"


def _b64(text):
    return base64.b64encode(text.encode()).decode()


def _basic(user, password):
    return f"Basic {_b64(f'{user}:{password}')}"


def _digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _unauthorized_body(authorization):
    """The error body a registry sends with its 401."""
    if authorization:
        error = {"code": "DENIED", "message": "Your authorization token has expired."}
    else:
        error = {"code": "UNAUTHORIZED", "message": "authentication required"}
    return json.dumps({"errors": [error]}).encode()


class _Registry(ThreadingHTTPServer):
    """A registry serving one single-layer image, ``_REPO`` at tag ``1``.

    ``auth`` is "basic" (Amazon ECR's challenge), "bearer" (a token service
    at ``/token``), or None. Layer blobs redirect to ``/storage/``, which,
    like a presigned S3 URL, refuses a request that carries credentials.
    Each request lands in ``requests`` as (method, path, Authorization,
    body).
    """

    def __init__(self, auth):
        super().__init__(("127.0.0.1", 0), _RegistryHandler)
        self.auth = auth
        self.requests = []
        layer = io.BytesIO()
        with tarfile.open(fileobj=layer, mode="w") as tar:
            info = tarfile.TarInfo("etc/app.txt")
            info.size = len(_FILE)
            tar.addfile(info, io.BytesIO(_FILE))
        layer = layer.getvalue()
        config = json.dumps({"architecture": "amd64", "os": "linux"}).encode()
        self.layer_digest = _digest(layer)
        self.blobs = {_digest(config): config, self.layer_digest: layer}
        self.manifest = json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {"digest": _digest(config), "size": len(config)},
                "layers": [{"digest": self.layer_digest, "size": len(layer)}],
            }
        ).encode()

    @property
    def host(self):
        return f"127.0.0.1:{self.server_port}"

    def authorizations(self, prefix):
        """The Authorization of every request whose path starts with prefix."""
        return [a for _, path, a, _ in self.requests if path.startswith(prefix)]


class _RegistryHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def _send(self, status, body, headers=()):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle(self):
        registry = self.server
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode()
        authorization = self.headers.get("Authorization")
        registry.requests.append((self.command, path, authorization, body))

        if path == "/token":
            # Like an OAuth2 token endpoint, it reads a body only as a form.
            is_form = self.headers.get("Content-Type") == _FORM_TYPE
            form = parse_qs(body) if is_form else {}
            if authorization == _basic(_USER, _PASSWORD):
                self._send(200, json.dumps({"token": _TOKEN}).encode())
            elif form.get("refresh_token") == [_REFRESH_TOKEN]:
                self._send(200, json.dumps({"access_token": _TOKEN}).encode())
            else:
                self._send(401, b"{}")
        elif path.startswith("/storage/"):
            if authorization:
                self._send(400, b"Only one auth mechanism allowed")
            else:
                self._send(200, registry.blobs[path.rsplit("/", 1)[1]])
        elif registry.auth == "basic" and authorization != _basic(_USER, _PASSWORD):
            challenge = (
                f'Basic realm="http://{registry.host}/",service="ecr.amazonaws.com"'
            )
            body = _unauthorized_body(authorization)
            self._send(401, body, [("WWW-Authenticate", challenge)])
        elif registry.auth == "bearer" and authorization != f"Bearer {_TOKEN}":
            challenge = (
                f'Bearer realm="http://{registry.host}/token",service="fake",'
                f'scope="repository:{_REPO}:pull"'
            )
            body = _unauthorized_body(authorization)
            self._send(401, body, [("WWW-Authenticate", challenge)])
        elif path == f"/v2/{_REPO}/manifests/1":
            self._send(200, registry.manifest)
        elif path == f"/v2/{_REPO}/blobs/{registry.layer_digest}":
            location = f"http://{registry.host}/storage/{registry.layer_digest}"
            self._send(307, b"", [("Location", location)])
        elif path.startswith(f"/v2/{_REPO}/blobs/"):
            self._send(200, registry.blobs[path.rsplit("/", 1)[1]])
        else:
            self._send(404, b"{}")


@pytest.fixture
def start_registry(monkeypatch):
    """Start a ``_Registry`` that Docker Hub references reach over HTTP."""
    started = []

    def start(auth):
        registry = _Registry(auth)
        threading.Thread(
            target=registry.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        ).start()
        started.append(registry)
        monkeypatch.setenv("RAY_SANDBOX_REGISTRY_MIRROR", f"http://{registry.host}")
        return registry

    yield start
    for registry in started:
        registry.shutdown()
        registry.server_close()


@pytest.fixture
def docker_config(tmp_path, monkeypatch):
    """Write the config a fresh ``DOCKER_CONFIG`` holds; returns its path."""
    config_dir = tmp_path / "docker-config"
    config_dir.mkdir()
    monkeypatch.setenv("DOCKER_CONFIG", str(config_dir))
    path = config_dir / "config.json"

    def write(config):
        path.write_text(json.dumps(config))
        return path

    return write


@pytest.fixture
def install_helper(tmp_path, monkeypatch):
    """Put a fake ``docker-credential-<name>`` on PATH.

    It answers the servers in ``answers`` ({server: (username, secret)}) and
    reports any other server as not found, the way real helpers do. Every
    server it is asked about lands in the returned file, one per line.
    """
    bin_dir = tmp_path / "helper-bin"
    bin_dir.mkdir()
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    def install(name, answers):
        calls = bin_dir / f"{name}.calls"
        cases = "".join(
            f"  {shlex.quote(server)}) printf '%s' "
            + shlex.quote(
                json.dumps({"ServerURL": server, "Username": user, "Secret": secret})
            )
            + " ;;\n"
            for server, (user, secret) in answers.items()
        )
        script = bin_dir / f"docker-credential-{name}"
        script.write_text(
            "#!/bin/sh\n"
            '[ "$1" = get ] || exit 1\n'
            "server=$(cat)\n"
            f'printf "%s\\n" "$server" >> {shlex.quote(str(calls))}\n'
            'case "$server" in\n'
            f"{cases}"
            "  *) echo 'credentials not found in native keychain'; exit 1 ;;\n"
            "esac\n"
        )
        script.chmod(0o755)
        return calls

    return install


def _pull(tmp_path):
    return pull_and_extract_container_image(
        f"{_REPO}:1", images_dir=str(tmp_path / "images"), timeout_seconds=10
    )


def test_basic_auth_pull_sends_helper_credentials_only_to_the_registry(
    start_registry, docker_config, install_helper, fake_mkfs_erofs, tmp_path
):
    """Amazon ECR's shape: a Basic challenge, answered with the credentials
    of the helper credHelpers names, sent on the registry's requests but not
    on the redirect to the layer's storage."""
    registry = start_registry("basic")
    calls = install_helper("ecr-login", {registry.host: (_USER, _PASSWORD)})
    docker_config({"credHelpers": {registry.host: "ecr-login"}})

    image_dir = _pull(tmp_path)

    with tarfile.open(os.path.join(image_dir, ROOTFS_IMAGE)) as tar:
        assert tar.extractfile("./etc/app.txt").read() == _FILE
    # Asked once, after the anonymous request drew the challenge.
    assert calls.read_text().splitlines() == [registry.host]
    probe, *authorized = registry.authorizations("/v2/")
    assert probe is None
    assert authorized and set(authorized) == {_basic(_USER, _PASSWORD)}
    assert registry.authorizations("/storage/") == [None]
    for root, _, files in os.walk(tmp_path / "images"):
        for name in files:
            with open(os.path.join(root, name), "rb") as f:
                content = f.read()
            assert _PASSWORD.encode() not in content
            assert _basic(_USER, _PASSWORD).encode() not in content


def test_bearer_pull_trades_auths_credentials_for_a_token(
    start_registry, docker_config, fake_mkfs_erofs, tmp_path
):
    registry = start_registry("bearer")
    auth = _b64(f"{_USER}:{_PASSWORD}")
    # Keyed by URL, the way older ``docker login`` versions wrote entries.
    docker_config({"auths": {f"https://{registry.host}/v1/": {"auth": auth}}})

    _pull(tmp_path)

    assert registry.authorizations("/token") == [_basic(_USER, _PASSWORD)]
    probe, *authorized = registry.authorizations("/v2/")
    assert probe is None
    assert authorized and set(authorized) == {f"Bearer {_TOKEN}"}


def test_identity_token_takes_the_oauth2_refresh_grant(
    start_registry, docker_config, install_helper, fake_mkfs_erofs, tmp_path
):
    registry = start_registry("bearer")
    install_helper("store", {registry.host: ("<token>", _REFRESH_TOKEN)})
    docker_config({"credsStore": "store"})

    _pull(tmp_path)

    ((method, _, authorization, body),) = [
        r for r in registry.requests if r[1] == "/token"
    ]
    assert (method, authorization) == ("POST", None)
    form = parse_qs(body)
    assert form["grant_type"] == ["refresh_token"]
    assert form["refresh_token"] == [_REFRESH_TOKEN]
    assert form["scope"] == [f"repository:{_REPO}:pull"]
    assert set(registry.authorizations("/v2/")[1:]) == {f"Bearer {_TOKEN}"}


def test_registry_token_goes_to_the_registry_as_is(start_registry, docker_config):
    registry = start_registry("bearer")
    docker_config({"auths": {registry.host: {"registrytoken": _TOKEN}}})

    headers = get_registry_auth_headers(
        f"http://{registry.host}", _REPO, reference="1", timeout=10
    )

    assert headers == {"Authorization": f"Bearer {_TOKEN}"}
    assert registry.authorizations("/token") == []


def test_unchallenged_pull_runs_no_credential_helper(
    start_registry, docker_config, install_helper, fake_mkfs_erofs, tmp_path
):
    registry = start_registry(None)
    calls = install_helper("desktop", {registry.host: (_USER, _PASSWORD)})
    docker_config({"credsStore": "desktop"})

    _pull(tmp_path)

    assert not calls.exists()
    assert set(registry.authorizations("/")) == {None}


@pytest.mark.parametrize("helper_configured", [False, True])
def test_basic_challenge_without_credentials_names_the_docker_config(
    start_registry,
    docker_config,
    install_helper,
    fake_mkfs_erofs,
    tmp_path,
    helper_configured,
):
    registry = start_registry("basic")
    if helper_configured:
        # A helper with nothing for the registry, as ecr-login answers when
        # the node's AWS identity can't get a token.
        install_helper("ecr-login", {})
        config = docker_config({"credHelpers": {registry.host: "ecr-login"}})
    else:
        config = docker_config({})

    with pytest.raises(SandboxCreationError, match="requires credentials") as info:
        _pull(tmp_path)

    assert registry.host in str(info.value)
    assert str(config) in str(info.value)


@pytest.mark.parametrize(
    "helper, script, error",
    [
        (
            "broken",
            "echo 'error getting credentials - err: NoCredentialProviders'\nexit 1",
            "docker-credential-broken failed to get credentials for "
            "127.0.0.1:[0-9]+: error getting credentials - err: "
            "NoCredentialProviders",
        ),
        ("missing", None, "Cannot run docker-credential-missing"),
        ("garbled", "echo 'not json'", "docker-credential-garbled answered"),
    ],
)
# A bearer registry, like Docker Hub, challenges pulls of public images too.
@pytest.mark.parametrize("auth", ["basic", "bearer"])
def test_failing_credential_helper_fails_the_pull(
    start_registry,
    docker_config,
    fake_mkfs_erofs,
    tmp_path,
    monkeypatch,
    auth,
    helper,
    script,
    error,
):
    registry = start_registry(auth)
    if script:
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        program = bin_dir / f"docker-credential-{helper}"
        program.write_text(f"#!/bin/sh\n{script}\n")
        program.chmod(0o755)
        monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    docker_config({"credHelpers": {registry.host: helper}})

    with pytest.raises(SandboxCreationError, match=error):
        _pull(tmp_path)


@pytest.mark.parametrize(
    "auth, error",
    [
        # The registry turns the credentials down, and says why.
        ("basic", "HTTP Error 401: Unauthorized: DENIED: Your authorization token"),
        # The token service turns them down.
        (
            "bearer",
            r"Failed to get a pull token .* from 'http://127\.0\.0\.1:\d+/token'",
        ),
    ],
)
def test_refused_credentials_fail_the_pull_with_the_reason(
    start_registry, docker_config, fake_mkfs_erofs, tmp_path, auth, error
):
    registry = start_registry(auth)
    docker_config(
        {"auths": {registry.host: {"username": _USER, "password": "stale-token"}}}
    )

    with pytest.raises(SandboxCreationError, match=error) as info:
        _pull(tmp_path)

    assert "stale-token" not in str(info.value)


@pytest.mark.parametrize(
    "body, reasons",
    [
        ('{"errors": [{"code": "DENIED", "message": "no"}]}', ": DENIED: no"),
        ("<html>502 Bad Gateway</html>", ""),
        ('"denied"', ""),
        ('{"message": "denied"}', ""),
        ('{"errors": "denied"}', ""),
        ('{"errors": ["denied"]}', ""),
        ('{"errors": [{"code": "DENIED"}]}', ""),
        ("", ""),
    ],
)
def test_pull_error_adds_only_the_registry_errors(body, reasons):
    """A registry's errors join the HTTP error; a body of another shape, such
    as a proxy's HTML page, leaves the HTTP error as it is."""
    err = urllib.error.HTTPError(
        "https://r.example/v2/", 403, "Forbidden", {}, io.BytesIO(body.encode())
    )
    assert _describe_pull_error(err) == "HTTP Error 403: Forbidden" + reasons


def test_lookup_follows_the_docker_cli_precedence(docker_config, install_helper):
    """credHelpers, then credsStore, then auths; Docker Hub's entries live
    under its index URL; an empty credHelpers entry opts a registry out of
    credsStore."""
    calls = install_helper(
        "store",
        {
            "a.example": ("store-user", "store-secret"),
            "https://index.docker.io/v1/": ("hub-user", "hub-secret"),
        },
    )
    install_helper("special", {"b.example:5000": ("b-user", "b-secret")})
    docker_config(
        {
            "credsStore": "store",
            "credHelpers": {"b.example:5000": "special", "c.example": ""},
            "auths": {
                "a.example": {"auth": _b64("auths-user:auths-secret")},
                "c.example": {"username": "c-user", "password": "c-secret"},
            },
        }
    )

    def lookup(registry):
        return registry_credentials(registry, timeout=10)

    assert lookup("a.example") == RegistryCredentials("store-user", "store-secret")
    assert lookup("registry-1.docker.io") == RegistryCredentials(
        "hub-user", "hub-secret"
    )
    assert lookup("http://b.example:5000") == RegistryCredentials("b-user", "b-secret")
    assert lookup("c.example") == RegistryCredentials("c-user", "c-secret")
    assert lookup("d.example") is None
    assert calls.read_text().splitlines() == [
        "a.example",
        "https://index.docker.io/v1/",
        "d.example",
    ]


def test_lookup_reads_auths_entries(docker_config):
    docker_config(
        {
            "auths": {
                "https://index.docker.io/v1/": {"auth": _b64("hub-user:hub:secret")},
                "fields.example": {"username": "user", "password": "secret"},
                "oauth.example": {"identitytoken": "refresh"},
                "token.example": {"registrytoken": "bearer"},
                "placeholder.example": {},
            }
        }
    )

    def lookup(registry):
        return registry_credentials(registry, timeout=10)

    assert lookup("registry-1.docker.io") == RegistryCredentials(
        "hub-user", "hub:secret"
    )
    assert lookup("fields.example") == RegistryCredentials("user", "secret")
    assert lookup("oauth.example") == RegistryCredentials(identity_token="refresh")
    assert lookup("token.example") == RegistryCredentials(registry_token="bearer")
    assert lookup("placeholder.example") is None
    assert lookup("other.example") is None


@pytest.mark.parametrize("key", ["docker.io", "https://registry-1.docker.io"])
def test_lookup_takes_docker_hub_auths_under_its_other_hostnames(docker_config, key):
    """A Kubernetes Secret may key Docker Hub's entry by docker.io, which
    kubelet accepts. The Docker CLI's own key still comes first."""
    k8s_entry = {"auth": _b64("k8s-user:k8s-secret")}
    docker_config({"auths": {key: k8s_entry}})
    assert registry_credentials(
        "registry-1.docker.io", timeout=10
    ) == RegistryCredentials("k8s-user", "k8s-secret")
    assert registry_credentials("a.example", timeout=10) is None

    docker_config(
        {
            "auths": {
                key: k8s_entry,
                "https://index.docker.io/v1/": {"auth": _b64("hub-user:hub-secret")},
            }
        }
    )
    assert registry_credentials(
        "registry-1.docker.io", timeout=10
    ) == RegistryCredentials("hub-user", "hub-secret")


def test_lookup_without_a_config_file_and_with_a_broken_one(docker_config):
    assert registry_credentials("a.example", timeout=10) is None
    path = docker_config({})
    path.write_text("{not json")
    with pytest.raises(SandboxCreationError, match="Cannot read the Docker config"):
        registry_credentials("a.example", timeout=10)


@pytest.mark.parametrize(
    "config",
    [
        ["not", "an", "object"],
        {"credHelpers": ["b.example"]},
        {"auths": ["a.example"]},
        {"auths": {"a.example": "user:secret"}},
        {"auths": {"a.example": {"auth": 12345}}},
        {"auths": {"a.example": {"auth": "not-base64"}}},
        {"auths": {"a.example": {"auth": _b64("secret-without-a-user")}}},
        # http.client's error for this header value would quote the token.
        {"auths": {"a.example": {"registrytoken": "secret\n"}}},
    ],
)
def test_lookup_refuses_a_malformed_config(docker_config, config):
    """A malformed field fails the lookup with the file's path, and the
    message carries no secret."""
    path = docker_config(config)
    with pytest.raises(SandboxCreationError, match="malformed") as err:
        registry_credentials("a.example", timeout=10)
    assert str(path) in str(err.value)
    assert "secret" not in str(err.value)


def test_credentials_repr_hides_the_secrets():
    creds = RegistryCredentials("user", "pass-s3cret", "idtok-s3cret", "regtok-s3cret")
    assert "user" in repr(creds)
    assert "s3cret" not in repr(creds)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
