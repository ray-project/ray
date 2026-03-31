import os
import unittest
from unittest.mock import patch

from ci.build.publish_core_image import (
    image_tag,
    publish,
    s3_key,
)


class TestS3Key(unittest.TestCase):
    def test_basic(self):
        got = s3_key("linux", "x86_64", "3.12", "sha256:abc123")
        self.assertEqual(got, "core-digest/linux/x86_64/3.12/sha256:abc123")

    def test_different_python(self):
        got = s3_key("linux", "x86_64", "3.10", "sha256:def456")
        self.assertEqual(got, "core-digest/linux/x86_64/3.10/sha256:def456")


class TestImageTag(unittest.TestCase):
    def test_basic(self):
        got = image_tag("ecr.example.com/repo", "abc123", "3.12")
        self.assertEqual(got, "ecr.example.com/repo:abc123-ray-core-py3.12")

    def test_with_arch_suffix(self):
        got = image_tag("ecr.example.com/repo", "abc123", "3.12", "-aarch64")
        self.assertEqual(got, "ecr.example.com/repo:abc123-ray-core-py3.12-aarch64")

    def test_different_version(self):
        got = image_tag("registry/citemp", "build42", "3.14")
        self.assertEqual(got, "registry/citemp:build42-ray-core-py3.14")


class TestPublish(unittest.TestCase):
    _ENV = {
        "PYTHON_VERSION": "3.12",
        "ARCH_SUFFIX": "",
        "HOSTTYPE": "x86_64",
        "RAYCI_WORK_REPO": "ecr.example.com/citemp",
        "RAYCI_BUILD_ID": "abc123",
    }

    @patch("ci.build.publish_core_image.compute_digest")
    @patch.dict(os.environ, _ENV, clear=False)
    def test_dry_run(self, mock_digest):
        mock_digest.return_value = "sha256:deadbeef"
        # Should not raise, and should not call export or upload
        publish(dry_run=True)
        mock_digest.assert_called_once()

    @patch("ci.build.publish_core_image.upload_to_s3")
    @patch("ci.build.publish_core_image.export_image")
    @patch("ci.build.publish_core_image.compute_digest")
    @patch.dict(os.environ, _ENV, clear=False)
    def test_publish_calls(self, mock_digest, mock_export, mock_upload):
        mock_digest.return_value = "sha256:deadbeef"
        publish(dry_run=False)

        mock_export.assert_called_once()
        tag_arg = mock_export.call_args[0][0]
        self.assertEqual(tag_arg, "ecr.example.com/citemp:abc123-ray-core-py3.12")

        mock_upload.assert_called_once()
        key_arg = mock_upload.call_args[0][1]
        self.assertEqual(key_arg, "core-digest/linux/x86_64/3.12/sha256:deadbeef")

    @patch("ci.build.publish_core_image.compute_digest")
    @patch.dict(os.environ, {}, clear=True)
    def test_missing_env_var(self, mock_digest):
        mock_digest.return_value = "sha256:deadbeef"
        with self.assertRaises(Exception) as ctx:
            publish(dry_run=True)
        self.assertIn("PYTHON_VERSION", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
