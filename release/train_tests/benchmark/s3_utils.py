# Standard library imports
import io
from typing import Tuple

# Third-party imports
import boto3

# Local imports - reuse existing AWS_REGION constant
from s3_reader import AWS_REGION


def parse_s3_url(s3_url: str) -> Tuple[str, str]:
    """Parse an S3 URL into bucket and key components.

    Args:
        s3_url: S3 URL in format "s3://bucket/key"

    Returns:
        Tuple of (bucket, key)

    Raises:
        ValueError: If URL is not a valid S3 URL

    Examples:
        >>> parse_s3_url("s3://my-bucket/path/to/file.jpg")
        ('my-bucket', 'path/to/file.jpg')
    """
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 URL format: {s3_url}")
    parts = s3_url[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def create_s3_client(region_name: str = AWS_REGION) -> "boto3.client":
    """Create a boto3 S3 client with the specified region.

    Args:
        region_name: AWS region name (defaults to AWS_REGION from s3_reader)

    Returns:
        Configured boto3 S3 client
    """
    return boto3.client("s3", region_name=region_name)


def download_s3_object(s3_client, s3_url: str) -> bytes:
    """Download an object from S3 given its URL.

    Args:
        s3_client: boto3 S3 client
        s3_url: S3 URL in format "s3://bucket/key"

    Returns:
        Raw bytes of the downloaded object
    """
    bucket, key = parse_s3_url(s3_url)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def download_s3_object_as_buffer(s3_client, s3_url: str) -> io.BytesIO:
    """Download an object from S3 and return as a BytesIO buffer.

    Useful for loading images or other binary data that need a file-like object.

    Args:
        s3_client: boto3 S3 client
        s3_url: S3 URL in format "s3://bucket/key"

    Returns:
        BytesIO buffer containing the downloaded data
    """
    data = download_s3_object(s3_client, s3_url)
    return io.BytesIO(data)
