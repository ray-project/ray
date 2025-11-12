import os
import sys
import tempfile
from unittest.mock import patch

import pytest

from ray_release.cloud_util import (
    _parse_abfss_uri,
    upload_file_to_azure,
    upload_working_dir_to_azure,
)


class FakeBlobServiceClient:
    def __init__(self, account_url, credential):
        self.account_url = account_url
        self.credential = credential
        self.blob_client = FakeBlobClient()

    def get_blob_client(self, container, blob):
        return self.blob_client


class FakeBlobClient:
    def __init__(self):
        self.uploaded_data = None

    def upload_blob(self, data, overwrite=True):
        self.uploaded_data = data.read()


@patch("ray_release.cloud_util.BlobServiceClient")
@patch("ray_release.cloud_util.DefaultAzureCredential")
def test_upload_file_to_azure(mock_credential, mock_blob_service_client):
    with tempfile.TemporaryDirectory() as tmp_path:
        local_file = os.path.join(tmp_path, "test.txt")
        expected_content = "test content"
        with open(local_file, "w") as f:
            f.write(expected_content)
        container = "test_container"
        account = "test_account"
        azure_path = f"abfss://{container}@{account}.dfs.core.windows.net/path/test.txt"
        fake_blob_client = FakeBlobClient()
        fake_blob_service_client = FakeBlobServiceClient(
            f"https://{account}.blob.core.windows.net", "test-credential"
        )
        fake_blob_service_client.blob_client = fake_blob_client

        upload_file_to_azure(str(local_file), azure_path, fake_blob_service_client)

        with open(local_file, "rb") as f:
            expected_data = f.read()
            assert fake_blob_client.uploaded_data == expected_data


@patch("ray_release.cloud_util.upload_file_to_azure")
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


@pytest.mark.parametrize(
    "uri, expected_account, expected_container, expected_path",
    [
        (
            "abfss://container@account.dfs.core.windows.net/path/test.txt",
            "account",
            "container",
            "path/test.txt",
        ),
        ("abfss://container@account.dfs.core.windows.net/", "account", "container", ""),
        (
            "abfss://container@account.dfs.core.windows.net/path/",
            "account",
            "container",
            "path/",
        ),
        (
            "abfss://container@account.dfs.core.windows.net/path/to/file.txt",
            "account",
            "container",
            "path/to/file.txt",
        ),
        (
            "abfss://container-name@account-123.dfs.core.windows.net/path",
            "account-123",
            "container-name",
            "path",
        ),
    ],
)
def test_parse_abfss_uri(uri, expected_account, expected_container, expected_path):
    account, container, path = _parse_abfss_uri(uri)
    assert account == expected_account
    assert container == expected_container
    assert path == expected_path


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
