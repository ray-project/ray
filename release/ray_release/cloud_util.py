import os
import random
import shutil
import string
import time
from typing import Optional, Tuple
from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from ray_release.logger import logger


def generate_tmp_cloud_storage_path() -> str:
    return "".join(random.choice(string.ascii_lowercase) for i in range(10))


def upload_file_to_azure(
    local_file_path: str,
    azure_file_path: str,
    blob_service_client: Optional[BlobServiceClient] = None,
) -> None:
    """Upload a file to Azure Blob Storage.

    Args:
        local_file_path: Path to local file to upload.
        azure_file_path: Path to file in Azure blob storage.
    """

    account, container, path = _parse_abfss_uri(azure_file_path)
    account_url = f"https://{account}.blob.core.windows.net"
    if blob_service_client is None:
        credential = DefaultAzureCredential(exclude_managed_identity_credential=True)
        blob_service_client = BlobServiceClient(account_url, credential)

    blob_client = blob_service_client.get_blob_client(container=container, blob=path)
    try:
        with open(local_file_path, "rb") as f:
            blob_client.upload_blob(data=f, overwrite=True)
    except Exception as e:
        logger.exception(f"Failed to upload file to Azure Blob Storage: {e}")
        raise


def archive_directory(directory_path: str) -> str:
    timestamp = str(int(time.time()))
    archived_filename = f"ray_release_{timestamp}.zip"
    output_path = os.path.abspath(archived_filename)
    shutil.make_archive(output_path[:-4], "zip", directory_path)
    return output_path


def upload_working_dir_to_azure(working_dir: str, azure_directory_uri: str) -> str:
    """Upload archived working directory to Azure blob storage.

    Args:
        working_dir: Path to directory to upload.
        azure_directory_uri: Path to directory in Azure blob storage.
    Returns:
        Azure blob storage path where archived directory was uploaded.
    """
    archived_file_path = archive_directory(working_dir)
    archived_filename = os.path.basename(archived_file_path)
    azure_file_path = f"{azure_directory_uri}/{archived_filename}"
    upload_file_to_azure(
        local_file_path=archived_file_path, azure_file_path=azure_file_path
    )
    return azure_file_path


def _parse_abfss_uri(uri: str) -> Tuple[str, str, str]:
    """Parse ABFSS URI to extract account, container, and path.
    ABFSS URI format: abfss://container@account.dfs.core.windows.net/path
    Returns: (account_name, container_name, path)
    """
    parsed = urlparse(uri)
    if "@" not in parsed.netloc:
        raise ValueError(
            f"Invalid ABFSS URI format: {uri}. "
            "Expected format: abfss://container@account.dfs.core.windows.net/path"
        )

    # Split netloc into container@account.dfs.core.windows.net
    container, account_part = parsed.netloc.split("@", 1)

    # Extract account name from account.dfs.core.windows.net
    account = account_part.split(".")[0]

    # Path starts with / which we keep for the blob path
    path = parsed.path.lstrip("/")

    return account, container, path


def convert_abfss_uri_to_https(uri: str) -> str:
    """Convert ABFSS URI to HTTPS URI.
    ABFSS URI format: abfss://container@account.dfs.core.windows.net/path
    Returns: HTTPS URI format: https://account.dfs.core.windows.net/container/path
    """
    account, container, path = _parse_abfss_uri(uri)
    return f"https://{account}.dfs.core.windows.net/{container}/{path}"
