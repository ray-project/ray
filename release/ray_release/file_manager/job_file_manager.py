import os
import sys

import boto3
from azure.storage.blob import BlobServiceClient
from google.cloud import storage

from ray_release.aws import RELEASE_AWS_BUCKET
from ray_release.cloud_util import generate_tmp_cloud_storage_path, get_azure_credential
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.logger import logger
from ray_release.util import (
    AZURE_CLOUD_STORAGE,
    AZURE_STORAGE_ACCOUNT,
    AZURE_STORAGE_CONTAINER,
    GS_BUCKET,
    GS_CLOUD_STORAGE,
    S3_CLOUD_STORAGE,
    exponential_backoff_retry,
)


class JobFileManager:
    def __init__(self, cluster_manager: ClusterManager):
        import anyscale

        self.cluster_manager = cluster_manager
        self.s3_client = boto3.client(S3_CLOUD_STORAGE)
        self.cloud_storage_provider = os.environ.get(
            "ANYSCALE_CLOUD_STORAGE_PROVIDER", S3_CLOUD_STORAGE
        )
        if self.cloud_storage_provider == S3_CLOUD_STORAGE:
            self.bucket = str(RELEASE_AWS_BUCKET)
        elif self.cloud_storage_provider == GS_CLOUD_STORAGE:
            self.bucket = GS_BUCKET
            self.gs_client = storage.Client()
        elif self.cloud_storage_provider == AZURE_CLOUD_STORAGE:
            self.bucket = AZURE_STORAGE_ACCOUNT
        else:
            raise RuntimeError(
                f"Non supported anyscale service provider: "
                f"{self.cloud_storage_provider}"
            )
        # Backward compatible
        if "ANYSCALE_RAY_DIR" in anyscale.__dict__:
            sys.path.insert(0, f"{anyscale.ANYSCALE_RAY_DIR}/bin")

    def _run_with_retry(self, f, initial_retry_delay_s: int = 10):
        assert callable(f)
        return exponential_backoff_retry(
            f,
            retry_exceptions=Exception,
            initial_retry_delay_s=initial_retry_delay_s,
            max_retries=3,
        )

    def _generate_tmp_cloud_storage_path(self):
        return f"tmp/{generate_tmp_cloud_storage_path()}"

    def download_from_cloud(
        self, key: str, target: str, delete_after_download: bool = False
    ):
        if self.cloud_storage_provider == S3_CLOUD_STORAGE:
            self._run_with_retry(
                lambda: self.s3_client.download_file(
                    Bucket=self.bucket,
                    Key=key,
                    Filename=target,
                )
            )
        if self.cloud_storage_provider == GS_CLOUD_STORAGE:
            bucket = self.gs_client.bucket(self.bucket)
            blob = bucket.blob(key)
            self._run_with_retry(lambda: blob.download_to_filename(target))
        if self.cloud_storage_provider == AZURE_CLOUD_STORAGE:
            account_url = f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
            credential = get_azure_credential()
            blob_service_client = BlobServiceClient(account_url, credential)
            blob_client = blob_service_client.get_blob_client(
                container=AZURE_STORAGE_CONTAINER, blob=key
            )
            with open(target, "wb") as f:
                blob_client.download_blob().readinto(f)
        if delete_after_download:
            self.delete(key)

    def _delete_gs_fn(self, key: str, recursive: bool = False):
        if recursive:
            blobs = self.gs_client.list_blobs(
                self.bucket,
                prefix=key,
            )
            for blob in blobs:
                blob.delete()
        else:
            blob = self.gs_client.bucket(self.bucket).blob(key)
            blob.delete()

    def _delete_s3_fn(self, key: str, recursive: bool = False):
        if recursive:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=key)
            for object in response["Contents"]:
                self.s3_client.delete_object(Bucket=self.bucket, Key=object["Key"])
        else:
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)

    def delete(self, key: str, recursive: bool = False):
        def delete_fn():
            if self.cloud_storage_provider == S3_CLOUD_STORAGE:
                self._delete_s3_fn(key, recursive)
                return
            if self.cloud_storage_provider == GS_CLOUD_STORAGE:
                self._delete_gs_fn(key, recursive)
                return
            if self.cloud_storage_provider == AZURE_CLOUD_STORAGE:
                # TODO(aslonnie): Implement Azure blob deletion.
                return

        try:
            self._run_with_retry(
                delete_fn,
                initial_retry_delay_s=2,
            )
        except Exception as e:
            logger.warning(f"Could not remove temporary cloud object: {e}")
