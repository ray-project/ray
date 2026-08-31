"""Tests for CloudFileSystem class."""

import os
import sys
import tempfile
from unittest.mock import patch

import pytest

from ray.llm._internal.common.utils.cloud_utils import CloudFileSystem


class TestCloudFileSystem:
    """Tests for the CloudFileSystem class."""

    @patch("ray.llm._internal.common.utils.cloud_utils.GCSFileSystem")
    def test_download_model(self, mock_gcs_filesystem):
        """Test downloading a model from cloud storage."""
        # Mock GCSFileSystem.get_file to return hash content
        mock_gcs_filesystem.get_file.return_value = "abcdef1234567890"

        # Create temp directory for testing
        with tempfile.TemporaryDirectory() as tempdir:
            # Test downloading model
            with patch.object(CloudFileSystem, "download_files") as mock_download:
                CloudFileSystem.download_model(tempdir, "gs://bucket/model", False)

                # Check that hash file was processed
                assert os.path.exists(os.path.join(tempdir, "refs", "main"))
                with open(os.path.join(tempdir, "refs", "main"), "r") as f:
                    assert f.read() == "abcdef1234567890"

                # Verify get_file was called for hash file
                mock_gcs_filesystem.get_file.assert_called_once_with(
                    "gs://bucket/model/hash", decode_as_utf_8=True
                )

                # Check that download_files was called correctly
                mock_download.assert_called_once()
                call_args = mock_download.call_args[1]
                assert call_args["path"] == os.path.join(
                    tempdir, "snapshots", "abcdef1234567890"
                )
                assert call_args["bucket_uri"] == "gs://bucket/model"
                assert call_args["substrings_to_include"] == []
                assert call_args["suffixes_to_exclude"] is None

    @patch("ray.llm._internal.common.utils.cloud_utils.GCSFileSystem")
    def test_download_model_tokenizer_only_includes_chat_template_jinja(
        self, mock_gcs_filesystem
    ):
        """Regression test for chat_template.jinja not downloaded in tokenizer-only mode.

        Models like Qwen3.5 use an external chat_template.jinja file instead of the
        legacy chat_template field inside tokenizer_config.json. Without "chat_template"
        in the substring filter, the .jinja file is skipped and ChatTemplateStage fails
        with: ValueError: Cannot use apply_chat_template because this processor does not
        have a chat template.
        """
        mock_gcs_filesystem.get_file.return_value = "abcdef1234567890"

        with tempfile.TemporaryDirectory() as tempdir:
            with patch.object(CloudFileSystem, "download_files") as mock_download:
                CloudFileSystem.download_model(tempdir, "gs://bucket/model", True)

                mock_download.assert_called_once()
                substrings = mock_download.call_args[1]["substrings_to_include"]
                # "chat_template" matches chat_template.jinja, which is the HuggingFace
                # convention used by newer models (e.g. Qwen3.5) for external chat templates.
                assert (
                    "chat_template" in substrings
                ), "chat_template.jinja files must be downloaded in tokenizer-only mode"

    @patch("ray.llm._internal.common.utils.cloud_utils.GCSFileSystem")
    def test_upload_model(self, mock_gcs_filesystem):
        """Test uploading a model to cloud storage."""
        # Create temp directory for testing
        with tempfile.TemporaryDirectory() as tempdir:
            hash = "abcdef1234567890"
            # Create refs/main file
            os.makedirs(os.path.join(tempdir, "refs"), exist_ok=True)
            model_rev_path = os.path.join(tempdir, "refs", "main")
            with open(model_rev_path, "w") as f:
                f.write(hash)

            # Create snapshots/<hash> folder
            model_asset_path = os.path.join(tempdir, "snapshots", hash)
            os.makedirs(model_asset_path)

            # Test uploading model
            CloudFileSystem.upload_model(tempdir, "gs://bucket/model")

            # Check that upload_files was called twice - once for model assets and once for hash file
            assert mock_gcs_filesystem.upload_files.call_count == 2

            # Verify the calls were made with correct arguments
            calls = mock_gcs_filesystem.upload_files.call_args_list
            call_paths = {
                call[0][0] for call in calls
            }  # Extract local_path from each call
            call_uris = {
                call[0][1] for call in calls
            }  # Extract bucket_uri from each call

            assert model_asset_path in call_paths
            assert model_rev_path in call_paths
            assert "gs://bucket/model" in call_uris
            assert "gs://bucket/model/hash" in call_uris


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
