"""S3 helpers."""

from urllib.parse import urlparse


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse s3://bucket/key into (bucket, key)."""
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return parsed.netloc, parsed.path.lstrip("/")
