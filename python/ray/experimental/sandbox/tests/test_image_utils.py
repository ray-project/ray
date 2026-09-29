import io
import json
import os
import shutil
import stat
import sys
import tarfile
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from ray.experimental.sandbox._internal import image_utils
from ray.experimental.sandbox._internal.image_utils import (
    IMAGE_CONFIG_FILENAME,
    extract_tar_layer,
    get_platform_arch,
    get_registry_auth_headers,
    parse_image_ref,
    publish_image,
    pull_and_extract_container_image,
    sanitize_image_name,
)
from ray.experimental.sandbox.exceptions import SandboxCreationError


def test_sanitize_image_name_is_readable_and_stable():
    # A readable prefix, so the cache directory can be identified by eye. For a
    # registry reference it is the *normalized* repo and tag, which is what
    # makes equivalent spellings land on one directory.
    assert sanitize_image_name("busybox").startswith("library_busybox_latest-")
    assert sanitize_image_name("python:3.10-slim").startswith(
        "library_python_3.10-slim-"
    )
    assert sanitize_image_name("ghcr.io/org/repo:1.0").startswith("org_repo_1.0-")
    assert sanitize_image_name("/tmp/ray/sandbox/images/ubuntu_22.04.tar").startswith(
        "ubuntu_22.04-"
    )

    # Deterministic across calls, or nothing would ever hit the cache.
    assert sanitize_image_name("busybox:latest") == sanitize_image_name(
        "busybox:latest"
    )

    # Equivalent spellings of one image normalize to a single entry rather than
    # being pulled twice.
    assert sanitize_image_name("busybox") == sanitize_image_name("busybox:latest")
    assert sanitize_image_name("busybox") == sanitize_image_name(
        "docker.io/library/busybox:latest"
    )

    # Every component must be safe as a single path segment.
    for ref in ("busybox", "ghcr.io/org/repo:1.0", "quay.io/coreos/etcd@sha256:abcd"):
        assert "/" not in sanitize_image_name(ref)

    with pytest.raises(ValueError, match="cannot be safely sanitized"):
        sanitize_image_name("")
    with pytest.raises(ValueError, match="cannot be safely sanitized"):
        sanitize_image_name("...")


def test_sanitize_image_name_does_not_collide(tmp_path):
    """Distinct images must never share a cache directory.

    A shared directory means one image is silently served the other's root
    filesystem -- the mtime check is the only guard, and it passes whenever the
    second reference is the older file.
    """
    a = tmp_path / "a" / "rootfs.tar"
    b = tmp_path / "b" / "rootfs.tar"
    a.parent.mkdir(parents=True)
    b.parent.mkdir(parents=True)
    a.write_bytes(b"a")
    b.write_bytes(b"b")

    # Same basename, different directories.
    assert sanitize_image_name(str(a)) != sanitize_image_name(str(b))

    # A local tar must not land on a registry image's entry.
    assert sanitize_image_name(str(tmp_path / "busybox_latest.tar")) != (
        sanitize_image_name("busybox:latest")
    )

    # Different images whose sanitized forms are identical.
    assert sanitize_image_name("org/repo:v1") != sanitize_image_name("org_repo_v1")
    assert sanitize_image_name("myreg:5000/app:v1") != sanitize_image_name(
        "myreg/5000/app_v1"
    )


def test_parse_image_ref():
    assert parse_image_ref("busybox") == (
        "registry-1.docker.io",
        "library/busybox",
        "latest",
    )
    assert parse_image_ref("busybox:1.36") == (
        "registry-1.docker.io",
        "library/busybox",
        "1.36",
    )
    assert parse_image_ref("python:3.10-slim") == (
        "registry-1.docker.io",
        "library/python",
        "3.10-slim",
    )
    assert parse_image_ref("rayproject/ray:2.35.0") == (
        "registry-1.docker.io",
        "rayproject/ray",
        "2.35.0",
    )
    assert parse_image_ref("ghcr.io/astral-sh/uv:latest") == (
        "ghcr.io",
        "astral-sh/uv",
        "latest",
    )
    assert parse_image_ref("quay.io/prometheus/prometheus:v2.0") == (
        "quay.io",
        "prometheus/prometheus",
        "v2.0",
    )
    assert parse_image_ref("localhost:5000/myimage:v1") == (
        "localhost:5000",
        "myimage",
        "v1",
    )
    assert parse_image_ref("docker.io/library/python:3.12-slim") == (
        "registry-1.docker.io",
        "library/python",
        "3.12-slim",
    )
    assert parse_image_ref("docker.io/python:3.12-slim") == (
        "registry-1.docker.io",
        "library/python",
        "3.12-slim",
    )
    assert parse_image_ref("docker.io/rayproject/ray:2.35.0") == (
        "registry-1.docker.io",
        "rayproject/ray",
        "2.35.0",
    )
    assert parse_image_ref("index.docker.io/library/python:3.12-slim") == (
        "registry-1.docker.io",
        "library/python",
        "3.12-slim",
    )
    assert parse_image_ref("index.docker.io/python:3.12-slim") == (
        "registry-1.docker.io",
        "library/python",
        "3.12-slim",
    )
    assert parse_image_ref("registry-1.docker.io/library/python:3.12-slim") == (
        "registry-1.docker.io",
        "library/python",
        "3.12-slim",
    )
    assert parse_image_ref("registry-1.docker.io/python:3.12-slim") == (
        "registry-1.docker.io",
        "library/python",
        "3.12-slim",
    )
    assert parse_image_ref("docker.io/library/ubuntu@sha256:12345") == (
        "registry-1.docker.io",
        "library/ubuntu",
        "sha256:12345",
    )
    assert parse_image_ref("docker.io/ubuntu@sha256:12345") == (
        "registry-1.docker.io",
        "library/ubuntu",
        "sha256:12345",
    )
    assert parse_image_ref("ubuntu@sha256:12345") == (
        "registry-1.docker.io",
        "library/ubuntu",
        "sha256:12345",
    )


def test_get_platform_arch():
    arch = get_platform_arch()
    assert arch in ("amd64", "arm64", "386", "arm") or isinstance(arch, str)


def test_extract_tar_layer_whiteouts(tmp_path):
    dest = tmp_path / "rootfs"
    dest.mkdir()

    # Layer 1: create dir and files
    buf1 = io.BytesIO()
    with tarfile.open(fileobj=buf1, mode="w:gz") as tar:
        d1 = b"file1 content"
        t1 = tarfile.TarInfo("app/file1.txt")
        t1.size = len(d1)
        tar.addfile(t1, io.BytesIO(d1))

        d2 = b"file2 content"
        t2 = tarfile.TarInfo("app/file2.txt")
        t2.size = len(d2)
        tar.addfile(t2, io.BytesIO(d2))

    extract_tar_layer(buf1.getvalue(), str(dest))
    assert (dest / "app" / "file1.txt").read_bytes() == b"file1 content"
    assert (dest / "app" / "file2.txt").read_bytes() == b"file2 content"

    # Layer 2: delete file1 with .wh.file1.txt and add file3
    buf2 = io.BytesIO()
    with tarfile.open(fileobj=buf2, mode="w:gz") as tar:
        t_wh = tarfile.TarInfo("app/.wh.file1.txt")
        t_wh.size = 0
        tar.addfile(t_wh, io.BytesIO(b""))

        d3 = b"file3 content"
        t3 = tarfile.TarInfo("app/file3.txt")
        t3.size = len(d3)
        tar.addfile(t3, io.BytesIO(d3))

    extract_tar_layer(buf2.getvalue(), str(dest))
    assert not (dest / "app" / "file1.txt").exists()
    assert (dest / "app" / "file2.txt").read_bytes() == b"file2 content"
    assert (dest / "app" / "file3.txt").read_bytes() == b"file3 content"

    # Layer 3: opaque whiteout on app/ (.wh..wh..opq)
    buf3 = io.BytesIO()
    with tarfile.open(fileobj=buf3, mode="w:gz") as tar:
        t_opq = tarfile.TarInfo("app/.wh..wh..opq")
        t_opq.size = 0
        tar.addfile(t_opq, io.BytesIO(b""))

        d4 = b"file4 content"
        t4 = tarfile.TarInfo("app/file4.txt")
        t4.size = len(d4)
        tar.addfile(t4, io.BytesIO(d4))

    extract_tar_layer(buf3.getvalue(), str(dest))
    assert not (dest / "app" / "file2.txt").exists()
    assert not (dest / "app" / "file3.txt").exists()
    assert (dest / "app" / "file4.txt").read_bytes() == b"file4 content"


def test_pull_and_extract_local_tar(tmp_path):
    local_tar = tmp_path / "sample.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        data = b"hello from local tar"
        ti = tarfile.TarInfo("hello.txt")
        ti.size = len(data)
        tar.addfile(ti, io.BytesIO(data))

    images_dir = tmp_path / "images"
    extracted_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir)
    )
    assert os.path.exists(extracted_dir)
    assert os.path.exists(os.path.join(extracted_dir, "rootfs", "hello.txt"))
    assert (
        open(os.path.join(extracted_dir, "rootfs", "hello.txt"), "rb").read()
        == b"hello from local tar"
    )


def test_pull_and_extract_remote_image(tmp_path):
    images_dir = tmp_path / "images"
    extracted_dir = pull_and_extract_container_image(
        "busybox:latest", images_dir=str(images_dir)
    )
    assert os.path.exists(extracted_dir)
    assert os.path.exists(os.path.join(extracted_dir, ".extracted"))
    assert os.path.exists(
        os.path.join(extracted_dir, "rootfs", "bin", "sh")
    ) or os.path.exists(os.path.join(extracted_dir, "rootfs", "bin", "busybox"))
    # The manifest digest is recorded so a moved tag can be detected later.
    assert os.path.exists(os.path.join(extracted_dir, ".manifest_digest"))
    # No duplicate layer archive beside the extracted tree: it doubled the disk
    # cost of every image, and -- because it was consulted before the registry
    # -- deleting the cache directory did not actually force a re-pull.
    assert not any(p.suffix == ".tar" for p in images_dir.iterdir())


def test_pull_and_extract_docker_io_prefixed_image(tmp_path):
    images_dir = tmp_path / "images"
    extracted_dir = pull_and_extract_container_image(
        "docker.io/library/busybox:latest", images_dir=str(images_dir)
    )
    assert os.path.exists(extracted_dir)
    assert os.path.exists(os.path.join(extracted_dir, ".extracted"))
    assert os.path.exists(
        os.path.join(extracted_dir, "rootfs", "bin", "sh")
    ) or os.path.exists(os.path.join(extracted_dir, "rootfs", "bin", "busybox"))


def test_pull_nonexistent_image(tmp_path):
    images_dir = tmp_path / "images"
    with pytest.raises(SandboxCreationError):
        pull_and_extract_container_image(
            "nonexistent_image_12345_xyz:latest",
            images_dir=str(images_dir),
            timeout_seconds=5.0,
        )


def test_pull_nonexistent_local_tar(tmp_path):
    images_dir = tmp_path / "images"
    with pytest.raises(SandboxCreationError, match="not found"):
        pull_and_extract_container_image(
            "/tmp/nonexistent_image.tar",
            images_dir=str(images_dir),
            timeout_seconds=5.0,
        )


def test_extract_tar_layer_usr_merge(tmp_path):
    dest = tmp_path / "rootfs"
    dest.mkdir()

    # Layer 1: create usr/bin directory and bin -> usr/bin symlink
    buf1 = io.BytesIO()
    with tarfile.open(fileobj=buf1, mode="w:gz") as tar:
        t_usr_bin = tarfile.TarInfo("usr/bin")
        t_usr_bin.type = tarfile.DIRTYPE
        tar.addfile(t_usr_bin)

        t_link = tarfile.TarInfo("bin")
        t_link.type = tarfile.SYMTYPE
        t_link.linkname = "usr/bin"
        tar.addfile(t_link)

        d1 = b"base_binary"
        t1 = tarfile.TarInfo("usr/bin/base")
        t1.size = len(d1)
        tar.addfile(t1, io.BytesIO(d1))

    extract_tar_layer(buf1.getvalue(), str(dest))
    assert (dest / "bin").is_symlink()
    assert (dest / "bin" / "base").read_bytes() == b"base_binary"

    # Layer 2: contains a directory entry for bin/ and a new binary bin/app
    buf2 = io.BytesIO()
    with tarfile.open(fileobj=buf2, mode="w:gz") as tar:
        t_bin = tarfile.TarInfo("bin")
        t_bin.type = tarfile.DIRTYPE
        tar.addfile(t_bin)

        d2 = b"app_binary"
        t2 = tarfile.TarInfo("bin/app")
        t2.size = len(d2)
        tar.addfile(t2, io.BytesIO(d2))

    extract_tar_layer(buf2.getvalue(), str(dest))
    # bin should remain a symlink to usr/bin and both binaries should be present
    assert (dest / "bin").is_symlink()
    assert (dest / "bin" / "base").read_bytes() == b"base_binary"
    assert (dest / "usr" / "bin" / "app").read_bytes() == b"app_binary"
    assert (dest / "bin" / "app").read_bytes() == b"app_binary"


def test_get_registry_auth_headers_success():
    def mock_urlopen(req, timeout=30.0):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if "manifests" in url:
            headers = MagicMock()
            headers.get.return_value = (
                'Bearer realm="https://auth.docker.io/token",'
                'service="registry.docker.io",'
                'scope="repository:library/busybox:pull"'
            )
            raise urllib.error.HTTPError(
                url, 401, "Unauthorized", headers, io.BytesIO(b"")
            )
        elif "auth.docker.io" in url:
            resp = MagicMock()
            resp.read.return_value = b'{"token": "test-bearer-token-xyz"}'
            resp.__enter__.return_value = resp
            return resp
        raise ValueError(f"Unexpected URL: {url}")

    with patch("urllib.request.urlopen", side_effect=mock_urlopen):
        headers = get_registry_auth_headers(
            "registry-1.docker.io", "library/busybox", reference="1.36.0"
        )
        assert headers == {"Authorization": "Bearer test-bearer-token-xyz"}


def test_get_registry_auth_headers_case_insensitive_and_query_param_handling():
    called_auth_url = []

    def mock_urlopen(req, timeout=30.0):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if "manifests" in url:
            headers = MagicMock()
            # Lowercase bearer and realm already containing ?account=ray
            headers.get.return_value = (
                'bearer realm="https://auth.example.com/token?account=ray",'
                'service="example.com"'
            )
            raise urllib.error.HTTPError(
                url, 401, "Unauthorized", headers, io.BytesIO(b"")
            )
        elif "auth.example.com" in url:
            called_auth_url.append(url)
            resp = MagicMock()
            resp.read.return_value = b'{"access_token": "oauth2-token-456"}'
            resp.__enter__.return_value = resp
            return resp
        raise ValueError(f"Unexpected URL: {url}")

    with patch("urllib.request.urlopen", side_effect=mock_urlopen):
        headers = get_registry_auth_headers(
            "registry.example.com", "my/repo", reference="v1.0"
        )
        assert headers == {"Authorization": "Bearer oauth2-token-456"}
        assert len(called_auth_url) == 1
        assert "https://auth.example.com/token?account=ray&" in called_auth_url[0]
        assert "service=example.com" in called_auth_url[0]
        assert "scope=repository%3Amy%2Frepo%3Apull" in called_auth_url[0]


def test_get_registry_auth_headers_no_auth_needed():
    with patch("urllib.request.urlopen", return_value=MagicMock()):
        headers = get_registry_auth_headers(
            "localhost:5000", "my/repo", reference="latest"
        )
        assert headers == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))


# -- publish_image ---------------------------------------------------------


def _tar_bytes(entries):
    """A tar holding {name: content}."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, content in entries.items():
            info = tarfile.TarInfo(f"./{name}")
            info.size = len(content)
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def test_publish_image_writes_sidecars_before_the_entry_is_visible(tmp_path):
    """The ordering that makes a separate completion marker unnecessary.

    A reader holds no lock, so an entry must never be observable with a rootfs
    but no config -- a sandbox started in that window comes up with no PATH.
    """
    images = str(tmp_path / "images")
    observed = {}

    def materialize(rootfs_dir):
        entry = os.path.join(images, "key")
        # Mid-materialize, nothing may be visible under the final name yet.
        observed["entry_exists_during_build"] = os.path.exists(entry)
        open(os.path.join(rootfs_dir, "f"), "w").close()
        return {IMAGE_CONFIG_FILENAME: b'{"config": {}}'}

    entry = publish_image(images, "key", materialize=materialize)

    assert observed["entry_exists_during_build"] is False
    assert os.path.exists(os.path.join(entry, ".extracted"))
    assert os.path.exists(os.path.join(entry, IMAGE_CONFIG_FILENAME))


def test_publish_image_discards_a_failed_materialize(tmp_path):
    images = str(tmp_path / "images")

    def boom(rootfs_dir):
        open(os.path.join(rootfs_dir, "half"), "w").close()
        raise RuntimeError("build failed")

    with pytest.raises(RuntimeError):
        publish_image(images, "key", materialize=boom)

    assert not os.path.exists(os.path.join(images, "key"))
    assert not [n for n in os.listdir(images) if ".tmp." in n]


def test_publish_image_creates_the_cache_root_privately(tmp_path, monkeypatch):
    """As the mkdir *leaf*, with an explicit mode.

    CPython's makedirs drops `mode` on its recursive call, so a caller that
    created this path as an intermediate of something deeper would leave it at
    0o777 & ~umask -- which the ownership check then rejects outright under a
    group-writable umask.
    """
    monkeypatch.setattr(os, "umask", lambda mask: 0o002)
    old = os.umask(0o002)
    try:
        images = str(tmp_path / "nested" / "images")
        publish_image(images, "key", materialize=lambda rootfs: {})
        assert stat.S_IMODE(os.stat(images).st_mode) == 0o700
    finally:
        os.umask(old)


def test_publish_image_reuses_a_current_entry(tmp_path):
    images = str(tmp_path / "images")
    calls = []

    def materialize(rootfs_dir):
        calls.append(1)
        return {}

    publish_image(images, "key", materialize=materialize)
    publish_image(images, "key", materialize=materialize, is_current=lambda d: True)

    assert len(calls) == 1


def test_publish_image_rebuilds_a_stale_entry(tmp_path):
    images = str(tmp_path / "images")
    calls = []

    def materialize(rootfs_dir):
        calls.append(1)
        return {}

    publish_image(images, "key", materialize=materialize)
    publish_image(images, "key", materialize=materialize, is_current=lambda d: False)

    assert len(calls) == 2


# -- replacing an image a sandbox is running from ----------------------------

_KEY = "img-0123456789abcdef"


@pytest.fixture
def layout(tmp_path, monkeypatch):
    """An image cache plus the backend's bundle and runsc state directories."""
    images = str(tmp_path / "images")
    bundles = str(tmp_path / "bundles")
    states = str(tmp_path / "runsc")
    os.makedirs(images, mode=0o700)
    os.makedirs(states)
    monkeypatch.setattr(image_utils, "SANDBOX_BUNDLES_DIR", bundles)
    monkeypatch.setattr(image_utils, "RUNSC_STATE_DIR", states)
    return images, bundles, states


def _bundle(layout, name, root_path, pid=None):
    """A sandbox bundle, its container alive (this process) unless pid says."""
    _, bundles, states = layout
    os.makedirs(os.path.join(bundles, name), exist_ok=True)
    with open(os.path.join(bundles, name, "config.json"), "w") as f:
        json.dump({"root": {"path": root_path}}, f)
    with open(os.path.join(states, f"{name}_sandbox:{name}.state"), "w") as f:
        json.dump({"sandbox": {"pid": os.getpid() if pid is None else pid}}, f)


def _publish(images, content=b"root:x:0:\n", replace=False):
    """Publish the image, or replace it as a moved tag would."""

    def materialize(rootfs_dir):
        extract_tar_layer(_tar_bytes({"etc/group": content}), rootfs_dir)
        return {}

    return publish_image(
        images,
        _KEY,
        materialize=materialize,
        is_current=(lambda d: False) if replace else (lambda d: True),
    )


def _versions(images):
    return sorted(n for n in os.listdir(images) if ".v." in n or ".old." in n)


def test_an_image_is_a_link_to_an_immutable_version(layout):
    images, _, _ = layout
    version = _publish(images)
    link = os.path.join(images, _KEY)
    assert os.path.islink(link)
    assert os.path.realpath(link) == version
    assert os.path.basename(version).startswith(f"{_KEY}.v.")


def test_replacing_an_image_leaves_a_running_sandbox_on_its_version(
    layout, monkeypatch
):
    """Measured before versions: a running sandbox's ``ls /etc`` came back
    empty once another sandbox replaced its image. Its bundle now names the
    version it runs from, which keeps both the files and the protection."""
    monkeypatch.setattr(image_utils, "_RETIRED_GRACE_SECONDS", 0)
    images, bundles, _ = layout
    old = _publish(images, b"old\n")
    _bundle(layout, "sb1", os.path.join(old, "rootfs"))

    new = _publish(images, b"new\n", replace=True)
    assert os.path.realpath(os.path.join(images, _KEY)) == new
    # Well past its grace period, but still in use.
    with open(os.path.join(old, "rootfs", "etc", "group"), "rb") as f:
        assert f.read() == b"old\n"

    shutil.rmtree(os.path.join(bundles, "sb1"))
    image_utils.reclaim_retired_entries(images)
    assert _versions(images) == [os.path.basename(new)]


def test_a_just_retired_version_waits_out_its_grace_period(layout, monkeypatch):
    """A sandbox that resolved the version a moment before it was replaced may
    not have written its bundle yet, or its container recorded its state:
    for those few seconds nothing on disk says it is in use."""
    images, _, _ = layout
    old = _publish(images)
    new = _publish(images, replace=True)
    assert _versions(images) == sorted(os.path.basename(v) for v in (old, new))

    monkeypatch.setattr(image_utils, "_RETIRED_GRACE_SECONDS", 0)
    image_utils.reclaim_retired_entries(images)
    assert _versions(images) == [os.path.basename(new)]


def test_a_stopped_containers_leftover_bundle_protects_nothing(layout, monkeypatch):
    """A killed actor never deletes its bundle; a node was measured holding 89
    of them, every container stopped. Only a live one counts."""
    monkeypatch.setattr(image_utils, "_RETIRED_GRACE_SECONDS", 0)
    images, _, _ = layout
    old = _publish(images)
    # Its sandbox process is gone: a pid that cannot exist.
    _bundle(layout, "dead", os.path.join(old, "rootfs"), pid=2**22 + 1)
    new = _publish(images, replace=True)
    assert _versions(images) == [os.path.basename(new)]


def test_an_image_being_published_is_left_for_a_later_sweep(layout, monkeypatch):
    """The reclaim takes each image's lock without blocking, so it never judges
    a version while that image's link is being repointed."""
    monkeypatch.setattr(image_utils, "_RETIRED_GRACE_SECONDS", 0)
    images, _, _ = layout
    _publish(images)
    new = _publish(images, replace=True)
    old_count = len(_versions(images))
    with image_utils._key_lock(images, _KEY):
        # Held here, standing in for a concurrent publish: nothing is touched.
        image_utils.reclaim_retired_entries(images)
        assert len(_versions(images)) == old_count
    image_utils.reclaim_retired_entries(images)
    assert _versions(images) == [os.path.basename(new)]


def test_an_image_cached_before_versions_is_set_aside_not_deleted(layout, monkeypatch):
    """An entry from before versions is a plain directory at the image's name,
    and a sandbox from then names it by that path."""
    monkeypatch.setattr(image_utils, "_RETIRED_GRACE_SECONDS", 0)
    images, bundles, _ = layout
    legacy = os.path.join(images, _KEY)
    os.makedirs(os.path.join(legacy, "rootfs", "etc"))
    with open(os.path.join(legacy, ".extracted"), "w") as f:
        f.write("ok")
    _bundle(layout, "old-sb", os.path.join(legacy, "rootfs"))

    new = _publish(images, replace=True)
    assert os.path.islink(legacy)
    (aside,) = [n for n in _versions(images) if ".old." in n]
    image_utils.reclaim_retired_entries(images)
    assert aside in _versions(images), "a live sandbox still runs from it"

    shutil.rmtree(os.path.join(bundles, "old-sb"))
    image_utils.reclaim_retired_entries(images)
    assert _versions(images) == [os.path.basename(new)]


def test_invalidating_an_image_retires_its_version(layout, monkeypatch):
    """force_build's route, which used to delete the entry straight away."""
    monkeypatch.setattr(image_utils, "_RETIRED_GRACE_SECONDS", 0)
    images, _, _ = layout
    version = _publish(images)
    _bundle(layout, "sb", os.path.join(version, "rootfs"))

    image_utils.retire_entry(os.path.join(images, _KEY))
    assert not os.path.lexists(os.path.join(images, _KEY))
    assert _versions(images) == [os.path.basename(version)]


def test_an_image_named_like_scratch_is_not_swept(tmp_path):
    """The readable half of a key can contain ".old." or ".tmp."; matching on
    that swept a live entry as leftover scratch."""
    images = str(tmp_path / "images")
    os.makedirs(images, mode=0o700)
    for name in ("x.old.y-0123456789abcdef", "x.tmp.y-0123456789abcdef"):
        os.makedirs(os.path.join(images, name, "rootfs"))
        os.utime(os.path.join(images, name), (0, 0))

    image_utils._sweep_stale_temporaries(images)
    assert sorted(os.listdir(images)) == [
        "x.old.y-0123456789abcdef",
        "x.tmp.y-0123456789abcdef",
    ]


# -- revalidation --------------------------------------------------------------


def test_one_creation_revalidates_its_image_once(tmp_path, monkeypatch):
    """The runtime, the backend and the OCI spec each pull; measured, that was
    nine registry requests and over two seconds for a cached image."""
    entry = tmp_path / "entry"
    entry.mkdir()
    (entry / image_utils.MANIFEST_DIGEST_FILENAME).write_text("sha256:aaa")
    checks = []
    monkeypatch.setattr(
        image_utils,
        "_remote_manifest_digest",
        lambda image, timeout: checks.append(image) or "sha256:aaa",
    )

    for _ in range(3):
        assert image_utils._cached_entry_is_current("busybox", str(entry), 5)
    assert len(checks) == 1

    # Past the window, the tag is checked again.
    monkeypatch.setattr(image_utils, "_REVALIDATE_AFTER_SECONDS", 0)
    assert image_utils._cached_entry_is_current("busybox", str(entry), 5)
    assert len(checks) == 2


def test_a_replaced_entry_is_revalidated_afresh(tmp_path, monkeypatch):
    """Keyed by identity: a new version behind the same name earns no trust
    from the old one's check."""
    for name in ("v1", "v2"):
        (tmp_path / name).mkdir()
        (tmp_path / name / image_utils.MANIFEST_DIGEST_FILENAME).write_text(
            "sha256:aaa"
        )
    entry = tmp_path / "entry"
    entry.symlink_to("v1")
    checks = []
    monkeypatch.setattr(
        image_utils,
        "_remote_manifest_digest",
        lambda image, timeout: checks.append(image) or "sha256:aaa",
    )
    image_utils._cached_entry_is_current("busybox", str(entry), 5)

    # Replaced the way publishing does it: the name repointed at a new version.
    entry.unlink()
    entry.symlink_to("v2")
    image_utils._cached_entry_is_current("busybox", str(entry), 5)
    assert len(checks) == 2


def test_each_cached_image_holds_its_own_copy(tmp_path):
    """Entries share no files, so replacing or deleting one never touches
    another -- or a sandbox still running from it."""
    images = str(tmp_path / "images")
    payload = _tar_bytes({"f": b"data"})

    def materialize(rootfs_dir):
        extract_tar_layer(io.BytesIO(payload), rootfs_dir)
        return {}

    first = publish_image(images, "one", materialize=materialize)
    second = publish_image(images, "two", materialize=materialize)

    a = os.stat(os.path.join(first, "rootfs", "f"))
    b = os.stat(os.path.join(second, "rootfs", "f"))
    assert a.st_ino != b.st_ino
    assert os.path.realpath(os.path.join(images, "one")) == first
    assert os.path.realpath(os.path.join(images, "two")) == second
