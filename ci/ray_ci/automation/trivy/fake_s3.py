"""An in-memory stand-in for the boto3 S3 client calls ScanStore makes."""

from pathlib import Path

from boto3.exceptions import S3UploadFailedError
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
            # boto3 turns the upload's ClientError into S3UploadFailedError.
            raise S3UploadFailedError(
                f"Failed to upload {filename} to {bucket}/{key}: An error occurred "
                "(AccessDenied) when calling the PutObject operation: Access Denied"
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

    def put(self, bucket: str, key: str, data: bytes) -> None:
        self.objects[(bucket, key)] = data

    def get(self, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]
