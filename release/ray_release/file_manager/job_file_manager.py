import os
import random
import shutil
import string
import sys
import tempfile
from typing import Optional

import boto3
from ray_release.aws import RELEASE_AWS_BUCKET
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import FileDownloadError, FileUploadError
from ray_release.file_manager.file_manager import FileManager
from ray_release.job_manager import JobManager
from ray_release.logger import logger
from ray_release.util import exponential_backoff_retry


class JobFileManager(FileManager):
    def __init__(self, cluster_manager: ClusterManager):
        # import anyscale

        super(JobFileManager, self).__init__(cluster_manager=cluster_manager)

        self.sdk = self.cluster_manager.sdk
        self.s3_client = boto3.client("s3")
        self.bucket = str(RELEASE_AWS_BUCKET)
        self.job_manager = JobManager(cluster_manager)

        # sys.path.insert(0, f"{anyscale.ANYSCALE_RAY_DIR}/bin")

    def _run_with_retry(self, f, initial_retry_delay_s: int = 10):
        assert callable(f)
        return exponential_backoff_retry(
            f,
            retry_exceptions=Exception,
            initial_retry_delay_s=initial_retry_delay_s,
            max_retries=3,
        )

    def _generate_tmp_s3_path(self):
        fn = "".join(random.choice(string.ascii_lowercase) for i in range(10))
        location = f"tmp/{fn}"
        return location

    def download(self, source: str, target: str):
        # Attention: Only works for single files at the moment
        remote_upload_to = self._generate_tmp_s3_path()
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

        # s3 -> local target
        self._run_with_retry(
            lambda: self.s3_client.download_file(
                Bucket=self.bucket,
                Key=remote_upload_to,
                Filename=target,
            )
        )

        self._run_with_retry(
            lambda: self.s3_client.delete_object(
                Bucket=self.bucket, Key=remote_upload_to
            ),
            initial_retry_delay_s=2,
        )

    def _push_local_dir(self):
        remote_upload_to = self._generate_tmp_s3_path()
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
        except Exception as e:
            logger.warning(f"Could not remove temporary S3 object: {e}")

    def upload(self, source: Optional[str] = None, target: Optional[str] = None):
        if source is None and target is None:
            self._push_local_dir()
            return

        assert isinstance(source, str)
        assert isinstance(target, str)

        remote_upload_to = self._generate_tmp_s3_path()

        # local source -> s3
        self._run_with_retry(
            lambda: self.s3_client.upload_file(
                Filename=source,
                Bucket=self.bucket,
                Key=remote_upload_to,
            )
        )

        # s3 -> remote target
        bucket_address = f"s3://{self.bucket}/{remote_upload_to}"
        retcode, _ = self.job_manager.run_and_wait(
            "pip install -q awscli && " f"aws s3 cp {bucket_address} {target}",
            {},
        )

        if retcode != 0:
            raise FileUploadError(f"Error uploading file {source} to {target}")

        try:
            self._run_with_retry(
                lambda: self.s3_client.delete_object(
                    Bucket=self.bucket, Key=remote_upload_to
                ),
                initial_retry_delay_s=2,
            )
        except Exception as e:
            logger.warning(f"Could not remove temporary S3 object: {e}")


class AnyscaleJobFileManager(JobFileManager):
    def __init__(self, cluster_manager: ClusterManager):
        super().__init__(cluster_manager)
        self.upload_to = f"working_dirs/{self.cluster_manager.test_name.replace(' ', '_')}_{self._generate_tmp_s3_path()}"

    def _generate_tmp_s3_path(self):
        fn = "".join(random.choice(string.ascii_lowercase) for i in range(10))
        return fn

    @property
    def uri(self):
        return f"s3://{self.bucket}/{self.upload_to}"

    def download(self, target: str):
        # Attention: Only works for single files at the moment
        remote_upload_to = self.upload_to

        # s3 -> local target
        self._run_with_retry(
            lambda: self.s3_client.download_file(
                Bucket=self.bucket,
                Key=remote_upload_to,
                Filename=target,
            )
        )

    def _push_local_dir(self):
        remote_upload_to = remote_upload_to + "/working_dir.zip"
        # pack local dir
        _, local_path = tempfile.mkstemp()
        shutil.make_archive(local_path, "zip", os.getcwd())
        # local source -> s3
        self._run_with_retry(
            lambda: self.s3_client.upload_file(
                Filename=local_path + ".zip",
                Bucket=self.bucket,
                Key=remote_upload_to,
            )
        )
        # remove local archive
        os.unlink(local_path)
        return f"s3://{self.bucket}/{remote_upload_to}"

    def upload(self, source: Optional[str] = None, target: Optional[str] = None):
        if source is None and target is None:
            return self._push_local_dir()

        assert isinstance(source, str)
        assert isinstance(target, str)

        remote_upload_to = self.upload_to

        # local source -> s3
        self._run_with_retry(
            lambda: self.s3_client.upload_file(
                Filename=source,
                Bucket=self.bucket,
                Key=remote_upload_to,
            )
        )
        return self.bucket, remote_upload_to
