import os
import platform
import sys
import tempfile
from unittest import mock

import pytest
import requests

from ci.ray_ci.automation.crane_lib import (
    CraneError,
    _crane_binary,
    _read_member_from_tar,
    call_crane_copy,
    call_crane_export,
    call_crane_index,
    call_crane_manifest,
    read_file_from_image,
)
from ci.ray_ci.automation.test_utils import local_registry  # noqa: F401, F811

TEST_IMAGE_AMD64 = "alpine:3.16@sha256:0db9d004361b106932f8c7632ae54d56e92c18281e2dd203127d77405020abf6"
TEST_IMAGE_ARM64 = "alpine:3.16@sha256:4bdb4ac63839546daabfe0a267a363b3effa17ce02ac5f42d222174484c5686c"


class TestCraneBinary:
    """Tests for _crane_binary function."""

    def test_crane_binary_returns_valid_path(self):
        """Test that crane binary path exists and is executable."""
        if platform.system() != "Linux" or platform.processor() != "x86_64":
            pytest.skip("Only supported on Linux x86_64")

        binary_path = _crane_binary()
        assert binary_path is not None
        assert binary_path.endswith("crane")

    @mock.patch("platform.system")
    @mock.patch("platform.processor")
    def test_crane_binary_unsupported_platform(self, mock_processor, mock_system):
        """Test crane binary raises error on unsupported platform."""
        mock_system.return_value = "Darwin"
        mock_processor.return_value = "arm64"

        with pytest.raises(ValueError, match="Unsupported platform"):
            _crane_binary()


class TestCraneCopyIntegration:
    """Integration tests for crane copy operations using a local registry."""

    def test_copy_public_image_to_local_registry(self, local_registry):  # noqa: F811
        """Test copying a public image to local registry."""
        port = local_registry
        # Use a small, well-known public image digest for reproducibility
        source = TEST_IMAGE_AMD64
        destination = f"localhost:{port}/test-alpine:copied"

        call_crane_copy(source=source, destination=destination)

        # Verify image exists in local registry
        response = requests.get(
            f"http://localhost:{port}/v2/test-alpine/manifests/copied"
        )
        assert response.status_code == 200

    def test_copy_nonexistent_image_fails(self, local_registry):  # noqa: F811
        """Test that copying a non-existent image raises CraneError."""
        port = local_registry
        source = "localhost:9999/nonexistent/image:tag"
        destination = f"localhost:{port}/should-not-exist:tag"

        with pytest.raises(CraneError):
            call_crane_copy(source=source, destination=destination)


class TestCraneManifestIntegration:
    """Integration tests for crane manifest operations."""

    def test_get_manifest_from_local_registry(self, local_registry):  # noqa: F811
        """Test getting manifest from local registry."""
        port = local_registry
        # First copy an image to the registry
        source = TEST_IMAGE_AMD64
        destination = f"localhost:{port}/manifest-test:v1"
        call_crane_copy(source=source, destination=destination)

        output = call_crane_manifest(tag=destination)

        assert "schemaVersion" in output or "config" in output

    def test_get_manifest_nonexistent_tag_fails(self, local_registry):  # noqa: F811
        """Test that getting manifest for non-existent tag raises CraneError."""
        port = local_registry
        tag = f"localhost:{port}/does-not-exist:missing"

        with pytest.raises(CraneError):
            call_crane_manifest(tag=tag)


class TestCraneIndexIntegration:
    """Integration tests for crane index operations."""

    def test_create_multiarch_index(self, local_registry):  # noqa: F811
        """Test creating a multi-architecture index."""
        port = local_registry

        # Copy two different architecture images
        amd64_dest = f"localhost:{port}/index-test:amd64"
        arm64_dest = f"localhost:{port}/index-test:arm64"

        call_crane_copy(source=TEST_IMAGE_AMD64, destination=amd64_dest)
        call_crane_copy(source=TEST_IMAGE_ARM64, destination=arm64_dest)

        # Create index
        index_name = f"localhost:{port}/index-test:multiarch"
        call_crane_index(index_name=index_name, tags=[amd64_dest, arm64_dest])

        # Verify index was created
        response = requests.get(
            f"http://localhost:{port}/v2/index-test/manifests/multiarch"
        )
        assert response.status_code == 200
        manifest = response.json()
        assert "manifests" in manifest
        assert len(manifest["manifests"]) == 2


class TestCraneExportIntegration:
    """Integration tests for crane export+extract operations."""

    def test_export_extracts_into_subdir(self, local_registry):  # noqa: F811
        """
        Test that call_crane_export exports a container filesystem and extracts
        it into the provided directory.
        """
        port = local_registry

        source = TEST_IMAGE_AMD64
        image = f"localhost:{port}/export-test:alpine"
        call_crane_copy(source=source, destination=image)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "nested", "wanda_fs")
            call_crane_export(tag=image, output_dir=out_dir)

            assert os.path.isdir(out_dir)
            assert any(os.scandir(out_dir)), "export dir is empty"

            # Alpine should have these paths in its root filesystem
            assert os.path.isdir(os.path.join(out_dir, "bin"))
            assert os.path.isdir(os.path.join(out_dir, "etc"))
            assert os.path.lexists(
                os.path.join(out_dir, "bin", "sh")
            ) or os.path.lexists(os.path.join(out_dir, "bin", "ash"))


class TestReadFileFromImage:
    """Tests for reading a single file out of an image tar."""

    def _write_tar_with_hardlink(self, tar_path, freeze_content=b"ray==2.56.0\n"):
        """
        Build a tar shaped like a `crane export` archive: the pip-freeze file
        plus a pair of identical files deduplicated into a hardlink. A full
        tarfile extraction of this trips the "linkname ... not found" failure
        we saw on CI; reading a single member must not.
        """
        import io
        import tarfile

        with tarfile.open(tar_path, mode="w") as tf:
            # Canonical regular file that the hardlink target points at, but
            # placed AFTER the hardlink so streaming link resolution fails.
            link_name = "home/ray/anaconda3/pkg_b/INSTALLER"
            link_target = "home/ray/anaconda3/pkg_a/INSTALLER"

            hardlink = tarfile.TarInfo(link_name)
            hardlink.type = tarfile.LNKTYPE
            hardlink.linkname = link_target
            tf.addfile(hardlink)

            freeze = tarfile.TarInfo("home/ray/pip-freeze.txt")
            freeze.size = len(freeze_content)
            tf.addfile(freeze, io.BytesIO(freeze_content))

            installer = b"pip\n"
            target = tarfile.TarInfo(link_target)
            target.size = len(installer)
            tf.addfile(target, io.BytesIO(installer))

    def test_read_member_survives_unextractable_hardlinks(self):
        from ci.ray_ci.automation.crane_lib import _extract_tar_to_dir

        freeze = b"ray==2.56.0\nnumpy==1.26.4\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, "image.tar")
            self._write_tar_with_hardlink(tar_path, freeze)

            # Sanity check: a full extraction of this tar reproduces the CI
            # failure (the whole reason for reading a single member instead).
            with pytest.raises(Exception, match="linkname"):
                _extract_tar_to_dir(tar_path, os.path.join(tmpdir, "full"))

            content = _read_member_from_tar(tar_path, "home/ray/pip-freeze.txt")
            assert content == freeze

    def test_read_member_absent_returns_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, "image.tar")
            self._write_tar_with_hardlink(tar_path)

            assert _read_member_from_tar(tar_path, "home/ray/missing.txt") is None

    def _write_tar_with_files(self, tar_path, files):
        """Write a tar with the given {member_name: bytes} contents."""
        import io
        import tarfile

        with tarfile.open(tar_path, mode="w") as tf:
            for name, content in files.items():
                info = tarfile.TarInfo(name)
                info.size = len(content)
                tf.addfile(info, io.BytesIO(content))

    def test_read_member_matches_dot_slash_prefixed_name(self):
        # crane/tar archives sometimes store members as "./path"; a request for
        # "path" must still find it via the normalization fallback.
        want = b"ray==2.56.0\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, "image.tar")
            self._write_tar_with_files(tar_path, {"./home/ray/pip-freeze.txt": want})

            assert _read_member_from_tar(tar_path, "home/ray/pip-freeze.txt") == want

    def test_read_member_does_not_conflate_leading_dot_names(self):
        # ".home/ray/x" and "home/ray/x" are DISTINCT members; the old
        # lstrip("./") normalization wrongly collapsed them. Requesting the
        # dotless name must not return the leading-dot member's bytes.
        with tempfile.TemporaryDirectory() as tmpdir:
            tar_path = os.path.join(tmpdir, "image.tar")
            self._write_tar_with_files(
                tar_path, {".home/ray/x": b"dot-file-contents\n"}
            )

            assert _read_member_from_tar(tar_path, "home/ray/x") is None

    @mock.patch("ci.ray_ci.automation.crane_lib.subprocess.check_call")
    @mock.patch("ci.ray_ci.automation.crane_lib._crane_binary")
    def test_read_file_from_image_exports_then_reads(self, mock_bin, mock_check_call):
        mock_bin.return_value = "/usr/bin/crane"
        freeze = b"ray==2.56.0\n"

        # Stand in for `crane export <tag> <tar_path>` by writing the tar the
        # command's 4th arg points at.
        def fake_export(cmd, env=None):
            self._write_tar_with_hardlink(cmd[3], freeze)

        mock_check_call.side_effect = fake_export

        content = read_file_from_image("some-repo:tag", "home/ray/pip-freeze.txt")
        assert content == freeze
        assert mock_check_call.call_count == 1
        assert mock_check_call.call_args[0][0][:3] == [
            "/usr/bin/crane",
            "export",
            "some-repo:tag",
        ]

    @mock.patch("ci.ray_ci.automation.crane_lib.subprocess.check_call")
    @mock.patch("ci.ray_ci.automation.crane_lib._crane_binary")
    def test_read_file_from_image_wraps_export_failure(self, mock_bin, mock_check_call):
        import subprocess

        mock_bin.return_value = "/usr/bin/crane"
        mock_check_call.side_effect = subprocess.CalledProcessError(1, "crane")

        with pytest.raises(CraneError, match="crane export failed"):
            read_file_from_image("some-repo:tag", "home/ray/pip-freeze.txt")


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
