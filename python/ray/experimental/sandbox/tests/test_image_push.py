import gzip
import hashlib
import io
import json
import sys
import tarfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from ray.experimental.sandbox._internal import image_push
from ray.experimental.sandbox._internal.image_utils import (
    extract_tar_layer,
    image_cache_name,
)
from ray.experimental.sandbox.exceptions import SandboxError


@pytest.fixture(scope="session", autouse=True)
def ensure_runsc() -> None:
    """Registry push and layer tests never start gVisor."""


def _digest(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


@pytest.mark.parametrize("split", [False, True])
def test_build_layers_roundtrip_whiteouts_and_metadata(tmp_path, monkeypatch, split):
    root = tmp_path / "root"
    root.mkdir()
    for name in ["deleted", "deleted-link"]:
        (root / name).write_text("old")
    (root / "opaque").mkdir()
    (root / "opaque" / "old").write_text("old")
    archive = tmp_path / "changes.tar"
    with tarfile.open(archive, "w") as tar:
        deleted = tarfile.TarInfo("deleted")
        deleted.type = tarfile.CHRTYPE
        tar.addfile(deleted)
        link = tarfile.TarInfo("deleted-link")
        link.type = tarfile.LNKTYPE
        link.linkname = "deleted"
        tar.addfile(link)
        opaque = tarfile.TarInfo("opaque")
        opaque.type = tarfile.DIRTYPE
        opaque.mode = 0o755
        opaque.pax_headers = {image_push._OPAQUE_XATTR: "y"}
        tar.addfile(opaque)
        child = tarfile.TarInfo("opaque/new")
        child.size = 3
        child.uid, child.gid, child.mode = 123, 456, 0o750
        tar.addfile(child, io.BytesIO(b"new"))
    if split:
        monkeypatch.setattr(image_push, "_PART_BYTES", 1)
    layers = image_push.build_layers(str(archive), str(tmp_path))
    assert len(layers) > 1 if split else len(layers) == 1
    ownership = {}
    for layer in layers:
        with open(layer.path, "rb") as f:
            data = f.read()
        assert layer.digest == _digest(data)
        assert layer.size == len(data)
        assert layer.diff_id == _digest(gzip.decompress(data))
        with tarfile.open(fileobj=io.BytesIO(data)) as tar:
            assert all(image_push._OPAQUE_XATTR not in m.pax_headers for m in tar)
        extract_tar_layer(data, str(root), ownership=ownership)
    assert not (root / "deleted").exists()
    assert not (root / "deleted-link").exists()
    assert not (root / "opaque" / "old").exists()
    assert (root / "opaque" / "new").read_bytes() == b"new"
    assert ownership["opaque/new"] == (123, 456)
    assert (root / "opaque" / "new").stat().st_mode & 0o777 == 0o750


@pytest.mark.parametrize("mount", [False, True])
@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize(
    "media_type", [image_push._DOCKER_MANIFEST, image_push._OCI_MANIFEST]
)
def test_push_image_mounts_or_uploads_base_and_preserves_config(
    tmp_path, monkeypatch, mount, cached, media_type
):
    from ray.experimental.sandbox._internal import layer_cache

    base_data = b"base layer"
    base_digest = _digest(base_data)
    if cached:
        path = Path(layer_cache.blob_path(str(tmp_path), base_digest))
        path.parent.mkdir(parents=True)
        path.write_bytes(base_data)
    blobs = {}
    manifests = {}
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, status, body=b"", location=None):
            self.send_response(status)
            if location:
                self.send_header("Location", location)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_HEAD(self):
            assert self.headers.get("Authorization") == "Bearer test-token"
            digest = self.path.rsplit("/", 1)[1]
            self.reply(200 if digest in blobs else 404)

        def do_GET(self):
            assert self.headers.get("Authorization") == "Bearer test-token"
            calls.append(("GET", self.path))
            self.reply(200, base_data)

        def do_POST(self):
            assert self.headers.get("Authorization") == "Bearer test-token"
            query = parse_qs(urlparse(self.path).query)
            if mount and "mount" in query:
                assert query["from"] == ["source"]
                blobs[query["mount"][0]] = base_data
                self.reply(201)
            else:
                self.reply(202, location="/v2/target/blobs/uploads/id?state=1")

        def do_PUT(self):
            assert self.headers.get("Authorization") == "Bearer test-token"
            data = self.rfile.read(int(self.headers["Content-Length"]))
            parsed = urlparse(self.path)
            if "/manifests/" in parsed.path:
                manifests[parsed.path.rsplit("/", 1)[1]] = data
            else:
                digest = parse_qs(parsed.query)["digest"][0]
                assert _digest(data) == digest
                blobs[digest] = data
            self.reply(201)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    host = f"127.0.0.1:{server.server_port}"
    base = f"{host}/source:old"
    config = {
        "architecture": "amd64",
        "os": "linux",
        "config": {"Env": ["A=old", "B=keep"], "Cmd": ["python"]},
        "rootfs": {"type": "layers", "diff_ids": ["sha256:base-diff"] * 2},
        "history": [{"created_by": "base"}],
    }
    manifest = {
        "schemaVersion": 2,
        "mediaType": media_type,
        "config": {"mediaType": "application/vnd.docker.container.image.v1+json"},
        "layers": [{"digest": base_digest, "size": len(base_data)}] * 2,
    }
    image_dir = tmp_path / image_cache_name(base)
    image_dir.mkdir()
    (image_dir / ".manifest.json").write_text(json.dumps(manifest))
    (image_dir / ".image_config.json").write_text(json.dumps(config))
    changed = tmp_path / "changed.gz"
    changed.write_bytes(gzip.compress(b"changes", mtime=0))
    layer = image_push.Layer(
        str(changed),
        _digest(changed.read_bytes()),
        _digest(b"changes"),
        changed.stat().st_size,
    )
    scopes = []

    def auth(*args, **kwargs):
        scopes.append(kwargs.get("scopes"))
        return {"Authorization": "Bearer test-token"}

    monkeypatch.setattr(image_push, "get_registry_auth_headers", auth)
    monkeypatch.setattr(
        image_push, "registry_base_url", lambda registry: f"http://{host}"
    )
    try:
        result = image_push.push_image(
            base,
            f"{host}/target:new",
            [layer],
            {"A": "new", "C": "add"},
            str(tmp_path),
            5,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    assert result == f"{host}/target@{_digest(manifests['new'])}"
    pushed = json.loads(manifests["new"])
    assert pushed["layers"][:2] == manifest["layers"]
    assert pushed["layers"][2]["digest"] == layer.digest
    assert pushed["mediaType"] == media_type
    new_config = json.loads(blobs[pushed["config"]["digest"]])
    assert new_config["config"]["Env"] == ["A=new", "B=keep", "C=add"]
    assert new_config["config"]["Cmd"] == ["python"]
    assert new_config["rootfs"]["diff_ids"] == ["sha256:base-diff"] * 2 + [
        layer.diff_id
    ]
    assert new_config["history"][0] == config["history"][0]
    assert blobs[base_digest] == base_data
    assert scopes[0] == ["repository:target:pull,push", "repository:source:pull"]
    assert len(calls) == (0 if mount or cached else 1)
    assert json.loads((image_dir / ".image_config.json").read_text()) == config


@pytest.mark.parametrize("manifest", [None, {}, {"config": None}, {"config": []}])
def test_push_rejects_invalid_config_descriptor_before_registry_requests(
    tmp_path, monkeypatch, manifest
):
    base = "registry.example/source:old"
    image_dir = tmp_path / image_cache_name(base)
    image_dir.mkdir()
    (image_dir / ".manifest.json").write_text(json.dumps(manifest))
    (image_dir / ".image_config.json").write_text("{}")
    monkeypatch.setattr(
        image_push,
        "get_registry_auth_headers",
        lambda *args, **kwargs: pytest.fail("Registry contacted for invalid manifest"),
    )
    with pytest.raises(SandboxError, match="requires a config descriptor object"):
        image_push.push_image(
            base, "registry.example/target:new", [], None, str(tmp_path), 5
        )


@pytest.mark.parametrize("config_present", [False, True])
def test_commit_env_with_missing_or_null_image_config(config_present):
    config = {"config": None} if config_present else {}
    committed = json.loads(image_push._commit_config(config, [], {"A": "new"}))
    assert committed["config"] == {"Env": ["A=new"]}


@pytest.mark.parametrize("image_config", [[], "invalid"])
def test_commit_env_rejects_non_object_image_config(image_config):
    with pytest.raises(SandboxError, match="config field must be an object or null"):
        image_push._commit_config({"config": image_config}, [], {"A": "new"})


@pytest.mark.parametrize("status", [400, 401, 500])
def test_mount_authentication_and_unexpected_errors_remain_terminal(
    monkeypatch, status
):
    import requests

    repository = image_push._Repository("registry.example", "target", "Bearer test", 5)

    def send(request, **kwargs):
        response = requests.Response()
        response.request = request
        response.status_code = status
        response._content = b"denied"
        response._content_consumed = True
        return response

    monkeypatch.setattr(repository._session, "send", send)
    try:
        with pytest.raises(SandboxError, match=f"failed: {status}"):
            repository.mount("sha256:test", "source")
    finally:
        repository.close()


@pytest.mark.parametrize(
    "location, registry_origin, status",
    [
        ("https://registry.example:443/upload", True, 201),
        ("https://storage.example/upload?signature=test", False, 201),
        ("https://registry.example:444/upload", False, 201),
        ("http://registry.example/upload", False, 201),
        ("https://storage.example/upload?signature=test", False, 401),
    ],
)
@pytest.mark.parametrize("auth", [None, "Bearer test"])
def test_upload_location_keeps_auth_only_on_registry_origin(
    monkeypatch, location, registry_origin, status, auth
):
    import requests

    repository = image_push._Repository("registry.example", "target", auth, 5)
    calls = []

    def send(request, **kwargs):
        calls.append(request)
        response = requests.Response()
        response.request = request
        response._content = b"denied" if status == 401 else b""
        response._content_consumed = True
        response.status_code = (
            404
            if request.method == "HEAD"
            else 202
            if request.method == "POST"
            else status
        )
        response.headers["Location"] = location
        response.headers["WWW-Authenticate"] = 'Basic realm="storage"'
        return response

    monkeypatch.setattr(repository._session, "send", send)
    monkeypatch.setattr(
        requests.sessions,
        "get_netrc_auth",
        lambda url: ("netrc-user", "netrc-password")
        if not registry_origin and url.startswith(location)
        else None,
    )
    monkeypatch.setattr(
        repository, "_authenticate", lambda challenge: pytest.fail("Storage auth retry")
    )
    try:
        if status == 401:
            with pytest.raises(SandboxError, match="Registry PUT .* failed: 401"):
                repository.upload("sha256:test", b"layer")
        else:
            repository.upload("sha256:test", b"layer")
    finally:
        repository.close()
    assert [request.method for request in calls] == ["HEAD", "POST", "PUT"]
    assert all(request.headers.get("Authorization") == auth for request in calls[:2])
    assert calls[2].headers.get("Authorization") == (auth if registry_origin else None)
    assert calls[2].headers["Content-Type"] == "application/octet-stream"
    assert calls[2].body == b"layer"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
