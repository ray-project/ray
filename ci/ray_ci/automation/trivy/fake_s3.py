"""An in-memory stand-in for the boto3 S3 client calls ScanStore makes."""

from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        # Keys whose upload fails, as when the role may not write there.
        self.denied: set[str] = set()
        # Keys whose download fails, as on a 5xx that outlasts boto3's retries.
        self.broken: set[str] = set()

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        if key in self.denied:
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
                "PutObject",
            )
        self.objects[(bucket, key)] = Path(filename).read_bytes()

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        if key in self.broken:
            raise ClientError(
                {"Error": {"Code": "InternalError", "Message": "try again"}},
                "GetObject",
            )
        if (bucket, key) not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        Path(filename).write_bytes(self.objects[(bucket, key)])

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        if Key in self.broken:
            raise ClientError(
                {"Error": {"Code": "InternalError", "Message": "try again"}},
                "HeadObject",
            )
        if (Bucket, Key) not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        return {}

    def put(self, bucket: str, key: str, data: bytes) -> None:
        self.objects[(bucket, key)] = data

    def get(self, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]
