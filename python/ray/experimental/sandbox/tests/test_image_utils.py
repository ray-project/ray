import io
import os
import sys
import tarfile

import pytest

from ray.experimental.sandbox._internal.image_utils import (
    extract_tar_layer,
    get_platform_arch,
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
