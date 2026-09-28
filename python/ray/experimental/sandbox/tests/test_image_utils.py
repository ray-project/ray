import io
import json
import mmap
import os
import shutil
import sys
import tarfile
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from ray.experimental.sandbox._internal import image_utils
from ray.experimental.sandbox._internal.image_utils import (
    ROOTFS_IMAGE,
    _owner_filter,
    expected_extract_marker,
    extract_tar_layer,
    get_platform_arch,
    get_registry_auth_headers,
    parse_image_ref,
    pull_and_extract_container_image,
    sanitize_image_name,
)
from ray.experimental.sandbox.exceptions import SandboxCreationError


@pytest.fixture(autouse=True)
def _build_images_with_fake_mkfs(fake_mkfs_erofs):
    """Every pull here builds its image through the fake mkfs.erofs."""
    yield


def _image_tree(image_dir):
    """{path: TarInfo} of a cached image built by the fake mkfs.erofs."""
    with tarfile.open(os.path.join(image_dir, ROOTFS_IMAGE)) as tar:
        return {os.path.normpath(m.name): m for m in tar.getmembers()}


def _image_file(image_dir, path):
    """Contents of ``path`` inside a cached image built by the fake mkfs.erofs."""
    with tarfile.open(os.path.join(image_dir, ROOTFS_IMAGE)) as tar:
        return tar.extractfile(f"./{path}").read()


def test_sanitize_image_name():
    assert sanitize_image_name("busybox") == "busybox"
    assert sanitize_image_name("busybox:latest") == "busybox_latest"
    assert sanitize_image_name("python:3.10-slim") == "python_3.10-slim"
    assert sanitize_image_name("ghcr.io/org/repo:1.0") == "ghcr.io_org_repo_1.0"
    assert (
        sanitize_image_name("quay.io/coreos/etcd@sha256:abcd")
        == "quay.io_coreos_etcd_sha256_abcd"
    )
    assert (
        sanitize_image_name("/tmp/ray/sandbox/images/ubuntu_22.04.tar")
        == "ubuntu_22.04"
    )

    with pytest.raises(ValueError, match="cannot be safely sanitized"):
        sanitize_image_name("")
    with pytest.raises(ValueError, match="cannot be safely sanitized"):
        sanitize_image_name("...")


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
    image_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir)
    )
    assert os.path.exists(image_dir)
    assert _image_file(image_dir, "hello.txt") == b"hello from local tar"
    # The cache holds the image; the tree it was built from is gone.
    assert not os.path.exists(os.path.join(image_dir, "rootfs"))


def _write_sample_tar(path):
    with tarfile.open(str(path), "w") as tar:
        data = b"hello"
        ti = tarfile.TarInfo("hello.txt")
        ti.size = len(data)
        tar.addfile(ti, io.BytesIO(data))


def test_pull_pins_image_until_release(tmp_path, monkeypatch):
    """A pull with an instance id survives eviction until the id is released."""
    from ray.experimental.sandbox._internal.image_utils import (
        _release_image_use,
        evict_least_recently_used_images,
    )

    monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", "0")
    local_tar = tmp_path / "sample.tar"
    _write_sample_tar(local_tar)
    images_dir = tmp_path / "images"

    extracted_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir), instance_id="sb-1"
    )
    assert os.path.exists(os.path.join(extracted_dir, ".users", "sb-1"))
    # A cache hit re-pins for another instance.
    pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir), instance_id="sb-2"
    )
    assert os.path.exists(os.path.join(extracted_dir, ".users", "sb-2"))

    evict_least_recently_used_images(str(images_dir), max_bytes=0)
    assert os.path.exists(extracted_dir)

    _release_image_use(extracted_dir, "sb-1")
    _release_image_use(extracted_dir, "sb-2")
    evict_least_recently_used_images(str(images_dir), max_bytes=0)
    assert not os.path.exists(extracted_dir)


def _renamed_for_deletion(image_dir):
    """Names of the image's directories renamed for deletion."""
    images_dir, name = os.path.split(image_dir)
    return [n for n in os.listdir(images_dir) if n.startswith(f".{name}.deleting.")]


def test_eviction_crash_leaves_no_partial_image(tmp_path, monkeypatch):
    """Eviction moves an image aside before deleting it, so a crash midway
    leaves nothing under the image's name, and the next pull sweeps up
    what's left, with eviction disabled too."""
    local_tar = tmp_path / "sample.tar"
    _write_sample_tar(local_tar)
    images_dir = tmp_path / "images"
    image_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir)
    )

    def crash(path, ignore_errors=False):
        raise RuntimeError("worker killed")

    with monkeypatch.context() as m:
        m.setattr(shutil, "rmtree", crash)
        with pytest.raises(RuntimeError):
            image_utils.evict_least_recently_used_images(str(images_dir), 0)
    assert not os.path.exists(image_dir)
    stale = _renamed_for_deletion(image_dir)
    assert len(stale) == 1

    # Eviction doesn't take what's left for a cached image.
    image_utils.evict_least_recently_used_images(str(images_dir), 0)
    assert _renamed_for_deletion(image_dir) == stale
    assert not os.path.exists(images_dir / f"{stale[0]}.lock")

    monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", "0")
    pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
    assert os.path.isdir(image_dir)
    assert _renamed_for_deletion(image_dir) == []


def test_eviction_counts_an_image_evicted_concurrently(tmp_path, monkeypatch):
    """An image another pull evicted since the scan counts as evicted, so
    eviction doesn't evict another image in its place."""
    images_dir = tmp_path / "images"
    image_dirs = []
    for name in ("older", "newer"):
        local_tar = tmp_path / f"{name}.tar"
        _write_sample_tar(local_tar)
        image_dirs.append(
            pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
        )
    older, newer = image_dirs
    marker = os.path.join(older, ".extracted")
    os.utime(marker, (1, 1))

    has_users = image_utils._has_users

    def evicted_by_another_pull(image_dir):
        if image_dir == older and os.path.isdir(older):
            shutil.rmtree(older, ignore_errors=True)
        return has_users(image_dir)

    monkeypatch.setattr(image_utils, "_has_users", evicted_by_another_pull)
    image_utils.evict_least_recently_used_images(
        str(images_dir), image_utils._dir_size_bytes(newer)
    )
    assert not os.path.exists(older)
    assert os.path.isdir(newer)


def test_repull_crash_leaves_no_partial_image(tmp_path, monkeypatch):
    """A re-pull moves the old image aside before deleting it, so a crash
    midway leaves nothing under the image's name, and the next pull sweeps
    up what's left, with eviction disabled too."""
    local_tar = tmp_path / "sample.tar"
    _write_sample_tar(local_tar)
    images_dir = tmp_path / "images"
    image_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir)
    )
    marker = os.path.join(image_dir, ".extracted")
    # Force a re-pull.
    stale = os.path.getmtime(local_tar) - 10
    os.utime(marker, (stale, stale))

    rmtree = shutil.rmtree

    def crash_deleting_the_old_image(path, ignore_errors=False):
        if ".deleting." in path:
            raise RuntimeError("worker killed")
        rmtree(path, ignore_errors=ignore_errors)

    with monkeypatch.context() as m:
        m.setattr(shutil, "rmtree", crash_deleting_the_old_image)
        with pytest.raises(RuntimeError):
            pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
    assert not os.path.exists(image_dir)
    assert len(_renamed_for_deletion(image_dir)) == 1

    monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", "0")
    pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
    assert os.path.isdir(image_dir)
    assert _renamed_for_deletion(image_dir) == []


def test_image_cache_max_bytes_default_and_env(tmp_path, monkeypatch):
    import shutil

    from ray.experimental.sandbox._internal.image_utils import image_cache_max_bytes

    monkeypatch.delenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", raising=False)
    half_disk = shutil.disk_usage(str(tmp_path)).total // 2
    assert image_cache_max_bytes(str(tmp_path)) == half_disk

    monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", "12345")
    assert image_cache_max_bytes(str(tmp_path)) == 12345

    monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", "0")
    assert image_cache_max_bytes(str(tmp_path)) == 0

    monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", "10G")
    with patch(
        "ray.experimental.sandbox._internal.image_utils.logger.warning"
    ) as warning:
        assert image_cache_max_bytes(str(tmp_path)) == half_disk
    warning.assert_called_once_with(
        "Ignoring %s=%r: expected an integer number of bytes.",
        "RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES",
        "10G",
    )


def test_pull_and_extract_remote_image(tmp_path):
    images_dir = tmp_path / "images"
    image_dir = pull_and_extract_container_image(
        "busybox:latest", images_dir=str(images_dir)
    )
    assert os.path.exists(image_dir)
    assert os.path.exists(os.path.join(image_dir, ".extracted"))
    tree = _image_tree(image_dir)
    assert "bin/sh" in tree or "bin/busybox" in tree
    # Only the image is cached: no extracted tree, and no archive doubling
    # its footprint.
    assert not os.path.exists(os.path.join(image_dir, "rootfs"))
    assert not os.path.exists(str(images_dir / "busybox_latest.tar"))


def test_pull_and_extract_docker_io_prefixed_image(tmp_path):
    images_dir = tmp_path / "images"
    image_dir = pull_and_extract_container_image(
        "docker.io/library/busybox:latest", images_dir=str(images_dir)
    )
    assert os.path.exists(image_dir)
    assert os.path.exists(os.path.join(image_dir, ".extracted"))
    tree = _image_tree(image_dir)
    assert "bin/sh" in tree or "bin/busybox" in tree


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
        t_usr_bin.mode = 0o755  # archived dir modes are honored; 0644 blocks non-root
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
        t_bin.mode = 0o755
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


def _add_member(
    tar, name, data=b"", uid=0, gid=0, mode=0o644, typ=tarfile.REGTYPE, linkname=""
):
    ti = tarfile.TarInfo(name)
    ti.uid = uid
    ti.gid = gid
    ti.mode = mode
    ti.type = typ
    ti.linkname = linkname
    if typ == tarfile.REGTYPE:
        ti.size = len(data)
        tar.addfile(ti, io.BytesIO(data))
    else:
        tar.addfile(ti)


def test_extract_tar_layer_ownership_recording(tmp_path):
    """The ownership map accumulates across layers with normalized paths and
    honors whiteouts and root-owned replacements (mailman-shaped image)."""
    dest = tmp_path / "rootfs"
    dest.mkdir()
    ownership = {}

    buf1 = io.BytesIO()
    with tarfile.open(fileobj=buf1, mode="w:gz") as tar:
        _add_member(
            tar, "./var/spool/postfix/defer", uid=101, mode=0o700, typ=tarfile.DIRTYPE
        )
        _add_member(
            tar,
            "var/spool/postfix/maildrop",
            uid=101,
            gid=104,
            mode=0o1730,
            typ=tarfile.DIRTYPE,
        )
        _add_member(
            tar,
            "var/lib/mailman3/data",
            uid=38,
            gid=38,
            mode=0o755,
            typ=tarfile.DIRTYPE,
        )
        _add_member(tar, "var/lib/mailman3/data/gone.txt", b"x", uid=38, gid=38)
        # A hardlink is recorded under its own name: the re-pack looks owners
        # up by path and may emit this name before its target's.
        _add_member(
            tar,
            "var/lib/mailman3/data/gone.link",
            uid=38,
            gid=38,
            typ=tarfile.LNKTYPE,
            linkname="var/lib/mailman3/data/gone.txt",
        )
        _add_member(tar, "etc/passwd", b"root:x:0:0::/root:/bin/sh\n")
    extract_tar_layer(buf1.getvalue(), str(dest), ownership=ownership)
    assert ownership == {
        "var/spool/postfix/defer": (101, 0),
        "var/spool/postfix/maildrop": (101, 104),
        "var/lib/mailman3/data": (38, 38),
        "var/lib/mailman3/data/gone.txt": (38, 38),
        "var/lib/mailman3/data/gone.link": (38, 38),
    }
    assert os.path.samefile(
        dest / "var/lib/mailman3/data/gone.link",
        dest / "var/lib/mailman3/data/gone.txt",
    )

    # Layer 2: a deletion whiteout, an opaque whiteout, and a root-owned
    # re-ship of a recorded path all drop entries.
    buf2 = io.BytesIO()
    with tarfile.open(fileobj=buf2, mode="w:gz") as tar:
        _add_member(tar, "var/spool/postfix/.wh.maildrop")
        _add_member(tar, "var/lib/mailman3/.wh..wh..opq")
        _add_member(tar, "var/lib/mailman3/fresh.txt", b"y")
        _add_member(tar, "var/spool/postfix/defer", mode=0o755, typ=tarfile.DIRTYPE)
    extract_tar_layer(buf2.getvalue(), str(dest), ownership=ownership)
    assert ownership == {}


def test_owner_filter_restores_recorded_owners():
    fn = _owner_filter({"opt/data": (38, 38)})
    ti = tarfile.TarInfo("./opt/data")
    ti.uid, ti.gid, ti.uname, ti.gname = 1000, 1000, "ray", "ray"
    out = fn(ti)
    assert (out.uid, out.gid, out.uname, out.gname) == (38, 38, "", "")
    ti = tarfile.TarInfo("./bin/sh")
    ti.uid = ti.gid = 1000
    assert (fn(ti).uid, fn(ti).gid) == (0, 0)


def test_pull_builds_erofs_image_with_recorded_owners(tmp_path, fake_mkfs_erofs):
    """The cache holds an image built from a tar carrying the image's real
    owners, no extracted tree, and the current format marker."""
    bin_dir = fake_mkfs_erofs

    local_tar = tmp_path / "sample.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        _add_member(tar, "opt/data", uid=38, gid=38, mode=0o750, typ=tarfile.DIRTYPE)
        _add_member(tar, "opt/data/f.txt", b"z", uid=38, gid=38)
        _add_member(
            tar,
            "etc/passwd",
            b"root:x:0:0::/root:/bin/sh\nmail:x:38:38::/var/mail:/bin/sh\n",
        )
    images_dir = tmp_path / "images"
    image_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir)
    )

    assert not os.path.exists(os.path.join(image_dir, "rootfs"))
    args = (bin_dir / "mkfs.args").read_text().split()
    assert args[:3] == ["--tar=f", f"-b{mmap.PAGESIZE}", "-E^inline_data"]
    marker = open(os.path.join(image_dir, ".extracted"), encoding="utf-8").read()
    assert marker == expected_extract_marker()
    # The fake image *is* the flattened tar: check the owners it carries.
    owners = {path: (m.uid, m.gid) for path, m in _image_tree(image_dir).items()}
    assert owners["opt/data"] == (38, 38)
    assert owners["opt/data/f.txt"] == (38, 38)
    assert owners["etc/passwd"] == (0, 0)
    assert owners["tmp/.ray-sandbox-keep"] == (0, 0)  # Docker-parity /tmp seed
    # runsc's mount points, which an immutable image cannot grow on demand.
    for mountpoint in ("proc", "sys", "dev", "dev/pts", "dev/shm", "run", "tmp"):
        assert owners[mountpoint] == (0, 0)


def test_resolve_in_root(tmp_path):
    """Paths resolve with the tree as ``/``: an absolute link target restarts
    at the root, ``..`` never climbs above it, missing components pass
    through, and a symlink loop fails instead of spinning."""
    root = tmp_path / "rootfs"
    (root / "usr" / "bin").mkdir(parents=True)
    (root / "bin").symlink_to("usr/bin")
    (root / "sbin").symlink_to("/usr/bin")
    (root / "etc").symlink_to("../../../outside")
    (root / "loop-a").symlink_to("loop-b")
    (root / "loop-b").symlink_to("loop-a")

    resolve = image_utils._resolve_in_root
    assert resolve(str(root), "") == (str(root), ".")
    assert resolve(str(root), "bin") == (str(root / "usr" / "bin"), "usr/bin")
    assert resolve(str(root), "sbin/tool") == (
        str(root / "usr" / "bin" / "tool"),
        "usr/bin/tool",
    )
    assert resolve(str(root), "etc/hosts") == (
        str(root / "outside" / "hosts"),
        "outside/hosts",
    )
    assert resolve(str(root), "new/dir") == (str(root / "new" / "dir"), "new/dir")
    with pytest.raises(OSError):
        resolve(str(root), "loop-a/x")


def test_extract_tar_layer_owners_through_symlinked_parents(tmp_path):
    """A member under a symlinked directory lands at, and is recorded under,
    the path the re-pack walk finds it at, absolute links included, and the
    whiteouts that later delete it look it up the same way."""
    dest = tmp_path / "rootfs"
    dest.mkdir()
    ownership = {}

    buf1 = io.BytesIO()
    with tarfile.open(fileobj=buf1, mode="w:gz") as tar:
        _add_member(tar, "usr/bin", mode=0o755, typ=tarfile.DIRTYPE)
        _add_member(tar, "usr/sbin", mode=0o755, typ=tarfile.DIRTYPE)
        _add_member(tar, "run", mode=0o755, typ=tarfile.DIRTYPE)
        _add_member(tar, "var", mode=0o755, typ=tarfile.DIRTYPE)
        _add_member(tar, "bin", typ=tarfile.SYMTYPE, linkname="usr/bin")
        _add_member(tar, "sbin", typ=tarfile.SYMTYPE, linkname="/usr/sbin")
        _add_member(tar, "var/run", typ=tarfile.SYMTYPE, linkname="/run")
    extract_tar_layer(buf1.getvalue(), str(dest), ownership=ownership)
    assert ownership == {}

    # Layer 2 ships daemon-owned paths through the links, as an image layer
    # built on a UsrMerge base does.
    buf2 = io.BytesIO()
    with tarfile.open(fileobj=buf2, mode="w:gz") as tar:
        _add_member(tar, "bin/tool", b"t", uid=101, gid=101, mode=0o755)
        _add_member(tar, "sbin/daemon", b"d", uid=102, mode=0o755)
        _add_member(
            tar,
            "var/run/postgresql",
            uid=999,
            gid=999,
            mode=0o2775,
            typ=tarfile.DIRTYPE,
        )
        _add_member(tar, "var/run/postgresql/.s.PGSQL.5432.lock", uid=999, gid=999)
    extract_tar_layer(buf2.getvalue(), str(dest), ownership=ownership)
    assert (dest / "bin").is_symlink() and (dest / "sbin").is_symlink()
    assert (dest / "usr" / "bin" / "tool").read_bytes() == b"t"
    assert (dest / "usr" / "sbin" / "daemon").read_bytes() == b"d"
    assert (dest / "run" / "postgresql").is_dir()
    assert ownership == {
        "usr/bin/tool": (101, 101),
        "usr/sbin/daemon": (102, 0),
        "run/postgresql": (999, 999),
        "run/postgresql/.s.PGSQL.5432.lock": (999, 999),
    }

    # Layer 3 deletes through the same links: a whiteout, an opaque whiteout
    # and a root-owned re-ship.
    buf3 = io.BytesIO()
    with tarfile.open(fileobj=buf3, mode="w:gz") as tar:
        _add_member(tar, "bin/.wh.tool")
        _add_member(tar, "var/run/.wh..wh..opq")
        _add_member(tar, "sbin/daemon", b"d2", mode=0o755)
    extract_tar_layer(buf3.getvalue(), str(dest), ownership=ownership)
    assert not (dest / "usr" / "bin" / "tool").exists()
    assert not (dest / "run" / "postgresql").exists()
    assert (dest / "usr" / "sbin" / "daemon").read_bytes() == b"d2"
    assert ownership == {}


def test_pull_keeps_owners_shipped_through_symlinked_dirs(tmp_path):
    """The built image stores the daemon uid/gid of a file the image ships as
    bin/tool with bin -> usr/bin: the re-pack finds it at usr/bin/tool."""
    local_tar = tmp_path / "sample.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        _add_member(tar, "usr/bin", mode=0o755, typ=tarfile.DIRTYPE)
        _add_member(tar, "bin", typ=tarfile.SYMTYPE, linkname="usr/bin")
        _add_member(tar, "bin/tool", b"t", uid=101, gid=101, mode=0o755)
    image_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(tmp_path / "images")
    )
    tree = _image_tree(image_dir)
    assert tree["bin"].issym() and tree["bin"].linkname == "usr/bin"
    assert (tree["usr/bin/tool"].uid, tree["usr/bin/tool"].gid) == (101, 101)
    assert "bin/tool" not in tree


def test_seeding_stays_inside_the_tree(tmp_path):
    """An image whose etc, dev or tmp links to a host directory gets its
    mount-point and /tmp seeds inside the tree, at the path the sandbox will
    see them at; the host directories are untouched."""
    root = tmp_path / "rootfs"
    root.mkdir()
    host_etc = tmp_path / "host-etc"
    host_tmp = tmp_path / "host-tmp"
    host_etc.mkdir()
    host_tmp.mkdir()
    host_tmp_mode = host_tmp.stat().st_mode
    (root / "etc").symlink_to(host_etc)  # absolute host path
    (root / "tmp").symlink_to(host_tmp)
    (root / "dev").symlink_to("../escape")  # climbs past the root

    image_utils._seed_tmp(str(root))
    image_utils._seed_mountpoints(str(root))

    assert os.listdir(host_etc) == []
    assert os.listdir(host_tmp) == []
    assert host_tmp.stat().st_mode == host_tmp_mode
    assert not (tmp_path / "escape").exists()
    # In the image, /etc -> /<abs> makes /etc/hosts /<abs>/hosts; same for /tmp.
    inside_etc = root / str(host_etc).lstrip("/")
    inside_tmp = root / str(host_tmp).lstrip("/")
    for name in ("resolv.conf", "hosts", "hostname"):
        assert (inside_etc / name).is_file()
    assert (inside_tmp / ".ray-sandbox-keep").is_file()
    assert (inside_tmp.stat().st_mode & 0o7777) == 0o1777
    assert (root / "escape" / "pts").is_dir()
    assert (root / "escape" / "shm").is_dir()


def test_pull_requires_mkfs_erofs_with_tar_support(tmp_path, monkeypatch):
    """Without a usable mkfs.erofs the pull fails before fetching anything,
    and the error says what to install."""
    local_tar = tmp_path / "sample.tar"
    _write_sample_tar(local_tar)
    images_dir = tmp_path / "images"

    # No mkfs.erofs at all.
    monkeypatch.setenv("PATH", str(tmp_path / "empty"))
    image_utils.mkfs_erofs_path.cache_clear()
    with pytest.raises(SandboxCreationError, match="mkfs.erofs is not in PATH"):
        pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))

    # One that predates --tar (erofs-utils before 1.7).
    old_bin = tmp_path / "old-bin"
    old_bin.mkdir()
    old = old_bin / "mkfs.erofs"
    old.write_text("#!/bin/sh\necho 'usage: mkfs.erofs [options] FILE DIRECTORY'\n")
    old.chmod(0o755)
    monkeypatch.setenv("PATH", f"{old_bin}:/usr/bin:/bin")
    image_utils.mkfs_erofs_path.cache_clear()
    with pytest.raises(SandboxCreationError, match="predates --tar"):
        pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
    # Nothing half-built is left behind.
    assert not (images_dir / "sample").exists()
    image_utils.mkfs_erofs_path.cache_clear()


def _forge_old_directory_cache(image_dir):
    """Turn a cache entry into what an earlier Ray left: an extracted
    ``rootfs/`` tree under a format-2 marker, and no EROFS image."""
    with open(os.path.join(image_dir, ".extracted"), "w", encoding="utf-8") as f:
        f.write(json.dumps({"format": 2, "rootfs": "dir"}, sort_keys=True))
    os.remove(os.path.join(image_dir, ROOTFS_IMAGE))
    os.makedirs(os.path.join(image_dir, "rootfs", "bin"))
    with open(os.path.join(image_dir, "rootfs", "bin", "sh"), "w") as f:
        f.write("#!/bin/sh\n")


def test_old_format_cache_repulls_once(tmp_path):
    """A cache entry of another format is rebuilt on the next pull and then
    reused; with no sandbox on the old entry, its tree goes at once."""
    local_tar = tmp_path / "sample.tar"
    _write_sample_tar(local_tar)
    images_dir = tmp_path / "images"
    image_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir)
    )
    _forge_old_directory_cache(image_dir)

    again = pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
    assert again == image_dir
    marker = os.path.join(image_dir, ".extracted")
    assert open(marker, encoding="utf-8").read() == expected_extract_marker()
    assert _image_file(image_dir, "hello.txt") == b"hello"
    assert not os.path.exists(os.path.join(image_dir, "rootfs"))

    # A current entry is a hit: nothing is rebuilt.
    built_at = os.path.getmtime(os.path.join(image_dir, ROOTFS_IMAGE))
    pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
    assert os.path.getmtime(os.path.join(image_dir, ROOTFS_IMAGE)) == built_at


def test_repull_keeps_tree_for_running_sandboxes(tmp_path):
    """Sandboxes running on an old extracted-directory entry keep their pins
    and their tree across the rebuild; the tree goes once none is left."""
    from ray.experimental.sandbox._internal.image_utils import _release_image_use

    local_tar = tmp_path / "sample.tar"
    _write_sample_tar(local_tar)
    images_dir = tmp_path / "images"
    image_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir), instance_id="sb-old"
    )
    _forge_old_directory_cache(image_dir)

    again = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir), instance_id="sb-new"
    )
    assert again == image_dir
    assert os.path.isfile(os.path.join(image_dir, ROOTFS_IMAGE))
    # The old tree moved over intact for sb-old's gofer.
    assert os.path.isfile(os.path.join(image_dir, "rootfs", "bin", "sh"))
    assert set(os.listdir(os.path.join(image_dir, ".users"))) == {"sb-old", "sb-new"}

    # Still in use: a further hit leaves the tree alone.
    pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
    assert os.path.isdir(os.path.join(image_dir, "rootfs"))

    _release_image_use(image_dir, "sb-old")
    _release_image_use(image_dir, "sb-new")
    pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
    assert not os.path.exists(os.path.join(image_dir, "rootfs"))
    assert os.path.isfile(os.path.join(image_dir, ROOTFS_IMAGE))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
