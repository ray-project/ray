from typing import List, Optional, Tuple, Union

import boto3
import requests

from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)

AWS_EXECUTABLE = "aws"
GCP_EXECUTABLE = "gcloud"


def get_file_from_s3(
    object_uri: str, decode_as_utf_8: bool = True
) -> Optional[Union[str, bytes]]:
    """Download a file from an S3 bucket into memory.

    Args:
        object_uri: URI of the file we want to download
        decode_as_utf_8: If True, decode the body of the retrieved file as utf-8.

    Return: contents of file as string or bytes depending on value of
        decode_as_utf_8. If the file does not exist, returns None.
    """
    # Parse the S3 path string to extract bucket name and object key
    path_parts = object_uri.replace("s3://", "").split("/", 1)
    bucket_name = path_parts[0]
    object_key = path_parts[1]
    s3_client = boto3.client("s3")
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    except (s3_client.exceptions.NoSuchBucket, s3_client.exceptions.NoSuchKey):
        logger.info(f"URI {object_uri} does not exist.")
        return None
    body = obj["Body"].read()
    if decode_as_utf_8:
        body = body.decode("utf-8")
    return body



def get_gcs_bucket_name_and_prefix(
    bucket_uri: str, is_file: bool = False
) -> Tuple[str, str]:
    """Gets the GCS bucket name and prefix from the bucket_uri.

    The bucket name never includes a trailing slash.
    If is_file is False, the prefix always includes a trailing slash.

    Args:
        bucket_uri: The URI to the directory path or the file path on remote
            storage.
        is_file: If bucket_uri is a file path and not a directory path.

    Returns:
        Tuple containing a bucket name and the object / directory prefix.
    """

    if not bucket_uri.startswith("gs://"):
        raise ValueError(
            f'Got invalid bucket_uri "{bucket_uri}". Expected a value that '
            'starts with "gs://".'
        )

    stripped_uri = bucket_uri[len("gs://") :]
    split_uri = stripped_uri.split("/", maxsplit=1)

    bucket_name = split_uri[0]

    if len(split_uri) > 1:
        bucket_prefix = split_uri[1]
    else:
        bucket_prefix = ""

    # Ensure non-empty bucket_prefixes have a trailing slash.
    if not is_file and bucket_prefix != "" and not bucket_prefix.endswith("/"):
        bucket_prefix += "/"

    return bucket_name, bucket_prefix



def get_gcs_client():
    """Returns the default gcs client"""

    try:
        from google.cloud import storage
    except ImportError as e:
        raise ImportError(
            "You must `pip install google-cloud-storage` "
            "to download from Google Cloud Storage."
        ) from e

    return storage.Client()


def get_file_from_gcs(
    object_uri: str,
    decode_as_utf_8: bool = True,
) -> Optional[Union[str, bytes]]:
    """Download a file from a Google Cloud Storage bucket into memory.

    Args:
        object_uri: URI of the file we want to download
        decode_as_utf_8: If True, decode the body of the retrieved file as utf-8.

    Return: contents of file as string or bytes depending on value of
        decode_as_utf_8. If the file does not exist, returns None.
    """

    try:
        from google.api_core.exceptions import Forbidden, NotFound
    except ImportError as e:
        raise ImportError(
            "You must `pip install google-cloud-storage` "
            "to download from Google Cloud Storage."
        ) from e

    bucket_name, prefix = get_gcs_bucket_name_and_prefix(object_uri, is_file=True)

    storage_client = get_gcs_client()
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(prefix)
        body = blob.download_as_string()
    except NotFound:
        logger.info(f"URI {object_uri} does not exist.")
        return None
    except Forbidden:
        logger.info(f"URI {object_uri} is throwing forbidden access.")
        return None

    if decode_as_utf_8:
        body = body.decode(encoding="utf-8")
    return body


def list_subfolders_s3(folder_uri: str) -> List[str]:
    """List the subfolders of an S3 folder.

    Not recursive. Lists only the immediate children of the folder.

    Args:
        folder_uri: the folder to read.

    Return: list of subfolder names in an S3 folder. None of the subfolders
        have a trailing slash.
    """

    # Ensure that the folder_uri has a trailing slash.
    folder_uri = f"{folder_uri.rstrip('/')}/"

    # Parse the S3 path string to extract bucket name and object key
    path_parts = folder_uri.replace("s3://", "").split("/", 1)
    bucket_name = path_parts[0]
    folder_prefix = path_parts[1]
    s3_client = boto3.client("s3")
    try:
        objs = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=folder_prefix, Delimiter="/"
        )
    except (s3_client.exceptions.NoSuchBucket, s3_client.exceptions.NoSuchKey):
        logger.info(f"Folder URI {folder_uri} does not exist. No LoRA adapters found.")
        return []

    subfolders = []
    if "CommonPrefixes" in objs:
        for common_prefix_dict in objs.get("CommonPrefixes", {}):
            if "Prefix" in common_prefix_dict:
                common_prefix = common_prefix_dict["Prefix"]
                subfolder = common_prefix[len(folder_prefix) :].rstrip("/")
                subfolders.append(subfolder)

    return subfolders



def list_subfolders_gcs(folder_uri: str) -> List[str]:
    """List the subfolders of a Google Cloud Storage folder.

    Not recursive. Lists only the immediate children of the folder.

    Args:
        folder_uri: the folder to read.

    Return: list of subfolder names in a GCS folder. None of the subfolders
        have a trailing slash.
    """

    # Ensure that the folder_uri has a trailing slash.
    folder_uri = f"{folder_uri.rstrip('/')}/"

    # Parse the GCS path string to extract bucket name and object key
    path_parts = folder_uri.replace("gs://", "").split("/", 1)
    bucket_name = path_parts[0]
    folder_prefix = path_parts[1]

    gcs_client = get_gcs_client()

    try:
        from google.api_core.exceptions import Forbidden, NotFound
    except ImportError:
        logger.exception(
            "You must `pip install google-cloud-storage` "
            "to check models in Google Cloud Storage."
        )
        return []

    try:
        # NOTE (shrekris): list_blobs() has a delimiter argument that should
        # allows us to list just the children of the folder. However, that
        # argument doesn't seem to work currently. So instead, we list all
        # the children recursively and keep only the common prefixes.
        blobs = gcs_client.list_blobs(bucket_name, prefix=folder_prefix)
    except NotFound:
        logger.info(f"GCS folder URI {folder_uri} does not exist.")
        return []
    except Forbidden:
        logger.info(f"GCS folder URI {folder_uri} is throwing forbidden access.")
        return []

    subfolders = set()
    for blob in blobs:
        full_name: str = blob.name
        child_name = full_name[len(folder_prefix) :]

        if "/" in child_name:
            # This object is not a file stored directly in the folder_uri. It
            # may be a subfolder or a file in a subfolder. We extract just the
            # subfolder value.
            subfolder = child_name.split("/", 1)[0]
            subfolders.add(subfolder)

    return list(subfolders)
