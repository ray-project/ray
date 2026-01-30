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
    call_crane_copy,
    call_crane_export,
    call_crane_index,
    call_crane_manifest,
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
