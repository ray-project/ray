import io
import json
import os
import sys
import tarfile
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from ray.experimental.sandbox._internal.idmap import IdMap
from ray.experimental.sandbox._internal.image_utils import (
    EXTRACT_MARKER,
    OWNERSHIP_SIDECAR,
    _cached_tar_is_ownership_true,
    _restore_owner_filter,
    ensure_idmapped_rootfs,
    extract_tar_layer,
    get_platform_arch,
    get_registry_auth_headers,
    parse_image_ref,
    pull_and_extract_container_image,
    sanitize_image_name,
)
from ray.experimental.sandbox.exceptions import SandboxCreationError


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
    assert os.path.exists(str(images_dir / "busybox_latest.tar"))


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


def _add_member(tar, name, data=b"", uid=0, gid=0, mode=0o644, typ=tarfile.REGTYPE):
    ti = tarfile.TarInfo(name)
    ti.uid = uid
    ti.gid = gid
    ti.mode = mode
    ti.type = typ
    if typ == tarfile.REGTYPE:
        ti.size = len(data)
        tar.addfile(ti, io.BytesIO(data))
    else:
        tar.addfile(ti)


def test_extract_tar_layer_ownership_recording(tmp_path):
    """The ownership map accumulates across layers and honors whiteouts —
    modeled on the mailman image (uid=101 spool dirs, opaque whiteout)."""
    dest = tmp_path / "rootfs"
    dest.mkdir()
    ownership = {}

    buf1 = io.BytesIO()
    with tarfile.open(fileobj=buf1, mode="w:gz") as tar:
        _add_member(
            tar, "var/spool/postfix/defer", uid=101, mode=0o700, typ=tarfile.DIRTYPE
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
        _add_member(tar, "etc/passwd", b"root:x:0:0::/root:/bin/sh\n")
    extract_tar_layer(buf1.getvalue(), str(dest), ownership=ownership)

    assert ownership == {
        "var/spool/postfix/defer": (101, 0),
        "var/spool/postfix/maildrop": (101, 104),
        "var/lib/mailman3/data": (38, 38),
        "var/lib/mailman3/data/gone.txt": (38, 38),
    }

    # Layer 2: deletion whiteout drops the file; opaque whiteout clears the
    # mailman3 subtree; a root-owned replacement records nothing.
    buf2 = io.BytesIO()
    with tarfile.open(fileobj=buf2, mode="w:gz") as tar:
        _add_member(tar, "var/spool/postfix/.wh.maildrop")
        _add_member(tar, "var/lib/mailman3/.wh..wh..opq")
        _add_member(tar, "var/lib/mailman3/fresh.txt", b"y")
    extract_tar_layer(buf2.getvalue(), str(dest), ownership=ownership)

    assert ownership == {"var/spool/postfix/defer": (101, 0)}


def test_extract_tar_layer_preserve_owner_order(tmp_path, monkeypatch):
    """preserve_owner chowns before chmod (setuid survival), lchowns
    symlinks, defers directories, and never chowns hardlinks."""
    calls = []
    monkeypatch.setattr(
        "ray.experimental.sandbox._internal.image_utils.os.lchown",
        lambda path, uid, gid: calls.append(("lchown", path, uid, gid)),
    )
    real_chmod = os.chmod
    monkeypatch.setattr(
        "ray.experimental.sandbox._internal.image_utils.os.chmod",
        lambda path, mode: (
            calls.append(("chmod", path, mode)),
            real_chmod(path, mode & 0o777),
        ),
    )

    dest = tmp_path / "rootfs"
    dest.mkdir()
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        _add_member(tar, "spool", uid=101, mode=0o700, typ=tarfile.DIRTYPE)
        _add_member(tar, "spool/suid-tool", b"#!", uid=101, gid=104, mode=0o4750)
        ti = tarfile.TarInfo("spool/link")
        ti.type = tarfile.SYMTYPE
        ti.linkname = "suid-tool"
        ti.uid = 101
        tar.addfile(ti)
        ti = tarfile.TarInfo("spool/hard")
        ti.type = tarfile.LNKTYPE
        ti.linkname = "spool/suid-tool"
        ti.uid = 101
        tar.addfile(ti)
    extract_tar_layer(buf.getvalue(), str(dest), preserve_owner=True)

    file_path = str(dest / "spool" / "suid-tool")
    ops_for_file = [c for c in calls if c[1] == file_path]
    assert ops_for_file[0] == ("lchown", file_path, 101, 104)
    assert ops_for_file[1] == ("chmod", file_path, 0o4750)

    link_path = str(dest / "spool" / "link")
    assert ("lchown", link_path, 101, 0) in calls
    hard_path = str(dest / "spool" / "hard")
    assert not any(c[0] == "lchown" and c[1] == hard_path for c in calls)

    # Directory ownership applied (deferred) with its mode after the loop.
    dir_path = str(dest / "spool")
    assert ("lchown", dir_path, 101, 0) in calls
    assert (
        calls[-1] == ("chmod", dir_path, 0o700) or ("chmod", dir_path, 0o700) in calls
    )


def test_local_tar_pull_writes_sidecar_and_versioned_marker(tmp_path):
    local_tar = tmp_path / "sample.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        _add_member(tar, "opt/data", uid=38, gid=38, mode=0o750, typ=tarfile.DIRTYPE)
        _add_member(tar, "opt/data/f.txt", b"z", uid=38, gid=38)
        _add_member(tar, "etc/hosts", b"127.0.0.1 localhost\n")

    images_dir = tmp_path / "images"
    extracted_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir)
    )
    marker = os.path.join(extracted_dir, ".extracted")
    assert open(marker, encoding="utf-8").read() == EXTRACT_MARKER
    sidecar = json.load(open(os.path.join(extracted_dir, OWNERSHIP_SIDECAR)))
    assert sidecar == {"opt/data": [38, 38], "opt/data/f.txt": [38, 38]}


def test_stale_marker_triggers_reextract(tmp_path):
    local_tar = tmp_path / "sample.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        _add_member(tar, "hello.txt", b"hi")

    images_dir = tmp_path / "images"
    extracted_dir = pull_and_extract_container_image(
        str(local_tar), images_dir=str(images_dir)
    )
    marker = os.path.join(extracted_dir, ".extracted")
    with open(marker, "w", encoding="utf-8") as f:
        f.write("ok")  # legacy format
    again = pull_and_extract_container_image(str(local_tar), images_dir=str(images_dir))
    assert again == extracted_dir
    assert open(marker, encoding="utf-8").read() == EXTRACT_MARKER


def test_cached_tar_ownership_detection(tmp_path):
    with_sidecar = tmp_path / "with.tar"
    with tarfile.open(str(with_sidecar), "w") as tar:
        _add_member(tar, f"./{OWNERSHIP_SIDECAR}", b"{}")
        _add_member(tar, "./rootfs/etc/hosts", b"x")
    without = tmp_path / "without.tar"
    with tarfile.open(str(without), "w") as tar:
        _add_member(tar, "./rootfs/etc/hosts", b"x")

    assert _cached_tar_is_ownership_true(str(with_sidecar)) is True
    assert _cached_tar_is_ownership_true(str(without)) is False
    assert _cached_tar_is_ownership_true(str(tmp_path / "absent.tar")) is False


def test_restore_owner_filter():
    fn = _restore_owner_filter({"etc/passwd": (0, 42), "opt/data": (38, 38)})

    ti = tarfile.TarInfo("./rootfs/opt/data")
    ti.uid, ti.gid, ti.uname, ti.gname = 1000, 1000, "ray", "ray"
    out = fn(ti)
    assert (out.uid, out.gid, out.uname, out.gname) == (38, 38, "", "")

    ti = tarfile.TarInfo("./rootfs/bin/sh")
    ti.uid = ti.gid = 1000
    out = fn(ti)
    assert (out.uid, out.gid) == (0, 0)

    ti = tarfile.TarInfo(f"./{OWNERSHIP_SIDECAR}")
    ti.uid = ti.gid = 1000
    out = fn(ti)
    assert (out.uid, out.gid) == (0, 0)


def test_ensure_idmapped_rootfs_requires_current_cache(tmp_path):
    images_dir = tmp_path / "images"
    target = images_dir / "img_latest"
    target.mkdir(parents=True)
    (target / ".extracted").write_text("ok")  # legacy
    idmap = IdMap(1000, 1000, 100000, 65536, 100000, 65536)
    with pytest.raises(SandboxCreationError, match="predates ownership-aware"):
        ensure_idmapped_rootfs("img:latest", idmap, images_dir=str(images_dir))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
