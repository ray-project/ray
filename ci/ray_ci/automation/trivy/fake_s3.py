"""An in-memory stand-in for the boto3 S3 client calls ScanStore makes."""

from pathlib import Path
from typing import Any, Iterator

from botocore.exceptions import ClientError


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        # Keys whose upload fails, as when the role may not write there.
        self.denied: set[str] = set()

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        if key in self.denied:
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
                "PutObject",
            )
        self.objects[(bucket, key)] = Path(filename).read_bytes()

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        if (bucket, key) not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        Path(filename).write_bytes(self.objects[(bucket, key)])

    def get_paginator(self, operation: str) -> "FakeS3":
        assert operation == "list_objects_v2"
        return self

    def paginate(self, Bucket: str, Prefix: str) -> Iterator[dict[str, Any]]:
        keys = sorted(
            k for b, k in self.objects if b == Bucket and k.startswith(Prefix)
        )
        # Two pages, so callers must follow pagination.
        half = len(keys) // 2
        for page in (keys[:half], keys[half:]):
            yield {"Contents": [{"Key": k} for k in page]} if page else {}

    def put(self, bucket: str, key: str, data: bytes) -> None:
        self.objects[(bucket, key)] = data

    def get(self, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]
