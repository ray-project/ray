"""Tests for mirror config classes."""

import sys

import pytest

from ray.llm._internal.common.utils.cloud_utils import (
    CloudMirrorConfig,
    LoraMirrorConfig,
)


class TestCloudMirrorConfig:
    """Tests for the CloudMirrorConfig class."""

    def test_valid_s3_uri(self):
        """Test valid S3 URI."""
        config = CloudMirrorConfig(bucket_uri="s3://my-bucket/path")
        assert config.bucket_uri == "s3://my-bucket/path"
        assert config.storage_type == "s3"

    def test_valid_gcs_uri(self):
        """Test valid GCS URI."""
        config = CloudMirrorConfig(bucket_uri="gs://my-bucket/path")
        assert config.bucket_uri == "gs://my-bucket/path"
        assert config.storage_type == "gcs"

    def test_valid_abfss_uri(self):
        """Test valid ABFSS URI."""
        config = CloudMirrorConfig(
            bucket_uri="abfss://container@account.dfs.core.windows.net/path"
        )
        assert (
            config.bucket_uri == "abfss://container@account.dfs.core.windows.net/path"
        )
        assert config.storage_type == "abfss"

    def test_valid_azure_uri(self):
        """Test valid Azure URI."""
        config = CloudMirrorConfig(
            bucket_uri="azure://container@account.blob.core.windows.net/path"
        )
        assert (
            config.bucket_uri == "azure://container@account.blob.core.windows.net/path"
        )
        assert config.storage_type == "azure"

    def test_none_uri(self):
        """Test None URI."""
        config = CloudMirrorConfig(bucket_uri=None)
        assert config.bucket_uri is None
        assert config.storage_type is None

    def test_invalid_uri(self):
        """Test invalid URI."""
        with pytest.raises(
            ValueError, match='Got invalid value "file:///tmp" for bucket_uri'
        ):
            CloudMirrorConfig(bucket_uri="file:///tmp")

    def test_extra_files(self):
        """Test extra files configuration."""
        config = CloudMirrorConfig(
            bucket_uri="s3://bucket/path",
            extra_files=[
                {"bucket_uri": "s3://bucket/file1", "destination_path": "/dest1"},
                {"bucket_uri": "s3://bucket/file2", "destination_path": "/dest2"},
            ],
        )
        assert len(config.extra_files) == 2
        assert config.extra_files[0].bucket_uri == "s3://bucket/file1"
        assert config.extra_files[0].destination_path == "/dest1"


class TestLoraMirrorConfig:
    """Tests for the LoraMirrorConfig class."""

    def test_valid_s3_config(self):
        """Test valid S3 LoRA config."""
        config = LoraMirrorConfig(
            lora_model_id="test-model",
            bucket_uri="s3://my-bucket/lora-models",
            max_total_tokens=1000,
        )
        assert config.lora_model_id == "test-model"
        assert config.bucket_uri == "s3://my-bucket/lora-models"
        assert config.bucket_name == "my-bucket"
        assert config.bucket_path == "lora-models"

    def test_valid_abfss_config(self):
        """Test valid ABFSS LoRA config."""
        config = LoraMirrorConfig(
            lora_model_id="test-model",
            bucket_uri="abfss://container@account.dfs.core.windows.net/lora/models",
            max_total_tokens=1000,
        )
        assert config.lora_model_id == "test-model"
        assert (
            config.bucket_uri
            == "abfss://container@account.dfs.core.windows.net/lora/models"
        )
        assert config.bucket_name == "container"
        assert config.bucket_path == "lora/models"

    def test_valid_azure_config(self):
        """Test valid Azure LoRA config."""
        config = LoraMirrorConfig(
            lora_model_id="test-model",
            bucket_uri="azure://container@account.blob.core.windows.net/lora/models",
            max_total_tokens=1000,
        )
        assert config.lora_model_id == "test-model"
        assert (
            config.bucket_uri
            == "azure://container@account.blob.core.windows.net/lora/models"
        )
        assert config.bucket_name == "container"
        assert config.bucket_path == "lora/models"

    def test_bucket_path_parsing(self):
        """Test bucket path parsing for different URI formats."""
        # S3 with multiple path segments
        config = LoraMirrorConfig(
            lora_model_id="test",
            bucket_uri="s3://bucket/path/to/model",
            max_total_tokens=1000,
        )
        assert config.bucket_name == "bucket"
        assert config.bucket_path == "path/to/model"

        # ABFSS with multiple path segments
        config = LoraMirrorConfig(
            lora_model_id="test",
            bucket_uri="abfss://container@account.dfs.core.windows.net/deep/nested/path",
            max_total_tokens=1000,
        )
        assert config.bucket_name == "container"
        assert config.bucket_path == "deep/nested/path"

    def test_invalid_uri(self):
        """Test invalid URI in LoRA config."""
        with pytest.raises(
            ValueError, match='Got invalid value "file:///tmp" for bucket_uri'
        ):
            LoraMirrorConfig(
                lora_model_id="test-model",
                bucket_uri="file:///tmp",
                max_total_tokens=1000,
            )

    def test_optional_fields(self):
        """Test optional fields in LoRA config."""
        config = LoraMirrorConfig(
            lora_model_id="test-model",
            bucket_uri="s3://bucket/path",
            max_total_tokens=1000,
            sync_args=["--exclude", "*.tmp"],
        )
        assert config.max_total_tokens == 1000
        assert config.sync_args == ["--exclude", "*.tmp"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
