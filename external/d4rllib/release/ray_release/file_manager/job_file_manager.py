import os
import shutil
import sys
import tempfile
from typing import Optional

import boto3
from google.cloud import storage
from ray_release.aws import RELEASE_AWS_BUCKET
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import FileDownloadError, FileUploadError
from ray_release.file_manager.file_manager import FileManager
from ray_release.job_manager import JobManager
from ray_release.logger import logger
from ray_release.util import (
    exponential_backoff_retry,
    generate_tmp_cloud_storage_path,
    S3_CLOUD_STORAGE,
    GS_CLOUD_STORAGE,
    GS_BUCKET,
)


class JobFileManager(FileManager):
    def __init__(self, cluster_manager: ClusterManager):
        import anyscale

        super(JobFileManager, self).__init__(cluster_manager=cluster_manager)

        self.sdk = self.cluster_manager.sdk
        self.s3_client = boto3.client(S3_CLOUD_STORAGE)
        self.cloud_storage_provider = os.environ.get(
            "ANYSCALE_CLOUD_STORAGE_PROVIDER", S3_CLOUD_STORAGE
        )
        if self.cloud_storage_provider == S3_CLOUD_STORAGE:
            self.bucket = str(RELEASE_AWS_BUCKET)
        elif self.cloud_storage_provider == GS_CLOUD_STORAGE:
            self.bucket = GS_BUCKET
            self.gs_client = storage.Client()
        else:
            raise RuntimeError(
                f"Non supported anyscale service provider: "
                f"{self.cloud_storage_provider}"
            )
        self.job_manager = JobManager(cluster_manager)
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
        location = f"tmp/{generate_tmp_cloud_storage_path()}"
        return location

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

        if delete_after_download:
            self.delete(key)

    def download(self, source: str, target: str):
        # Attention: Only works for single files at the moment
        remote_upload_to = self._generate_tmp_cloud_storage_path()
        # remote source -> s3
        bucket_address = f"s3://{self.bucket}/{remote_upload_to}"
        retcode, _ = self._run_with_retry(
            lambda: self.job_manager.run_and_wait(
                (
                    f"pip install -q awscli && "
                    f"aws s3 cp {source} {bucket_address} "
                    "--acl bucket-owner-full-control"
                ),
                {},
            )
        )

        if retcode != 0:
            raise FileDownloadError(f"Error downloading file {source} to {target}")

        self.download_from_cloud(remote_upload_to, target, delete_after_download=True)

    def _push_local_dir(self):
        remote_upload_to = self._generate_tmp_cloud_storage_path()
        # pack local dir
        _, local_path = tempfile.mkstemp()
        shutil.make_archive(local_path, "gztar", os.getcwd())
        # local source -> s3
        self._run_with_retry(
            lambda: self.s3_client.upload_file(
                Filename=local_path + ".tar.gz",
                Bucket=self.bucket,
                Key=remote_upload_to,
            )
        )
        # remove local archive
        os.unlink(local_path)

        bucket_address = f"s3://{self.bucket}/{remote_upload_to}"
        # s3 -> remote target
        retcode, _ = self.job_manager.run_and_wait(
            f"pip install -q awscli && "
            f"aws s3 cp {bucket_address} archive.tar.gz && "
            f"tar xf archive.tar.gz ",
            {},
        )
        if retcode != 0:
            raise FileUploadError(
                f"Error uploading local dir to session "
                f"{self.cluster_manager.cluster_name}."
            )
        try:
            self._run_with_retry(
                lambda: self.s3_client.delete_object(
                    Bucket=self.bucket, Key=remote_upload_to
                ),
                initial_retry_delay_s=2,
            )
        except RuntimeError as e:
            logger.warning(f"Could not remove temporary S3 object: {e}")

    def upload(self, source: Optional[str] = None, target: Optional[str] = None):
        if source is None and target is None:
            self._push_local_dir()
            return

        assert isinstance(source, str)
        assert isinstance(target, str)

        remote_upload_to = self._generate_tmp_cloud_storage_path()

        # local source -> s3
        self._run_with_retry(
            lambda: self.s3_client.upload_file(
                Filename=source,
                Bucket=self.bucket,
                Key=remote_upload_to,
            )
        )

        # s3 -> remote target
        bucket_address = f"{S3_CLOUD_STORAGE}://{self.bucket}/{remote_upload_to}"
        retcode, _ = self.job_manager.run_and_wait(
            "pip install -q awscli && " f"aws s3 cp {bucket_address} {target}",
            {},
        )

        if retcode != 0:
            raise FileUploadError(f"Error uploading file {source} to {target}")

        self.delete(remote_upload_to)

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

        try:
            self._run_with_retry(
                delete_fn,
                initial_retry_delay_s=2,
            )
        except Exception as e:
            logger.warning(f"Could not remove temporary cloud object: {e}")
