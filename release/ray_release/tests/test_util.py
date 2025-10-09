import sys
import os
from unittest.mock import patch
from unittest.mock import MagicMock

import pytest
import tempfile
from ray_release.util import upload_file_to_azure, upload_working_dir_to_azure


@patch("azure.storage.blob.BlobServiceClient")
@patch("azure.identity.DefaultAzureCredential")
def test_upload_file_to_azure(mock_credential, mock_blob_service_client):
    with tempfile.TemporaryDirectory() as tmp_path:
        local_file = os.path.join(tmp_path, "test.txt")
        expected_content = "test content"
        with open(local_file, "w") as f:
            f.write(expected_content)
        container = "test_container"
        account = "test_account"
        azure_path = f"abfss://{container}@{account}.dfs.core.windows.net/path/test.txt"

        # Mock Azure dependencies
        mock_credential.return_value = None
        mock_blob_service_client_instance = MagicMock()
        mock_blob_service_client.return_value = mock_blob_service_client_instance
        mock_blob_client_instance = MagicMock()
        mock_blob_service_client_instance.get_blob_client.return_value = (
            mock_blob_client_instance
        )
        mock_blob_client_instance.upload_blob.return_value = None

        upload_file_to_azure(str(local_file), azure_path)

        # Verify BlobServiceClient was called correctly
        mock_blob_service_client.assert_called_once_with(
            f"https://{account}.blob.core.windows.net", mock_credential.return_value
        )
        mock_blob_service_client_instance.get_blob_client.assert_called_once_with(
            container=container, blob="path/test.txt"
        )
        with open(local_file, "rb") as f:
            data = f.read()
            assert data.decode("utf-8") == expected_content
            mock_blob_client_instance.upload_blob.assert_called_once_with(data=data)


@patch("ray_release.util.upload_file_to_azure")
def test_upload_working_dir_to_azure(mock_upload_file_to_azure):
    with tempfile.TemporaryDirectory() as tmp_path:
        working_dir = os.path.join(tmp_path, "working_dir")
        os.makedirs(working_dir)
        with open(os.path.join(working_dir, "test.txt"), "w") as f:
            f.write("test content")
        azure_directory_uri = (
            "abfss://container@account.dfs.core.windows.net/path/working_dir"
        )
        upload_working_dir_to_azure(working_dir, azure_directory_uri)
        args = mock_upload_file_to_azure.call_args.kwargs
        assert args["local_file_path"].endswith(".zip")
        assert args["azure_file_path"].startswith(f"{azure_directory_uri}/")
        assert args["azure_file_path"].endswith(".zip")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
