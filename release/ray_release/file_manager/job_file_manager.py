import os
import random
import shlex
import shutil
import string
import subprocess
import sys
import tempfile
import time
from typing import Optional, Dict

import anyscale
import boto3

from ray_release.aws import RELEASE_AWS_BUCKET
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import FileDownloadError, FileUploadError
from ray_release.file_manager.file_manager import FileManager
from ray_release.logger import logger
from ray_release.util import ANYSCALE_HOST


class JobFileManager(FileManager):
    def __init__(self, cluster_manager: ClusterManager):
        super(JobFileManager, self).__init__(cluster_manager=cluster_manager)

        self.sdk = self.cluster_manager.sdk
        self.s3_client = boto3.client("s3")
        self.bucket = RELEASE_AWS_BUCKET

        self.subprocess_pool: Dict[int, subprocess.Popen] = dict()
        self.start_time: Dict[int, float] = dict()
        self.counter = 0

        sys.path.insert(0, f"{anyscale.ANYSCALE_RAY_DIR}/bin")

    def _run_job(self, cmd_to_run, env_vars) -> int:
        self.counter += 1
        command_id = self.counter
        env = os.environ.copy()
        env["RAY_ADDRESS"] = self.cluster_manager.get_cluster_address()
        env.setdefault("ANYSCALE_HOST", ANYSCALE_HOST)

        full_cmd = " ".join(f"{k}={v}" for k, v in env_vars.items()) + " " + cmd_to_run
        logger.info(f"Executing {cmd_to_run} with {env_vars} via ray job submit")
        proc = subprocess.Popen(
            f"ray job submit -- bash -c {shlex.quote(full_cmd)}",
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
        )
        self.subprocess_pool[command_id] = proc
        self.start_time[command_id] = time.time()
        return command_id

    def _wait_job(self, command_id: int):
        retcode = self.subprocess_pool[command_id].wait()
        duration = time.time() - self.start_time[command_id]
        return retcode, duration

    def _generate_tmp_s3_path(self):
        fn = "".join(random.choice(string.ascii_lowercase) for i in range(10))
        location = f"tmp/{fn}"
        return location

    def download(self, source: str, target: str):
        # Attention: Only works for single files at the moment
        remote_upload_to = self._generate_tmp_s3_path()
        # remote source -> s3
        bucket_address = f"s3://{self.bucket}/{remote_upload_to}"
        cid = self._run_job(
            (
                f"pip install -q awscli && "
                f"aws s3 cp {source} {bucket_address} "
                "--acl bucket-owner-full-control"
            ),
            {},
        )
        retcode, duration = self._wait_job(cid)

        if retcode != 0:
            raise FileDownloadError(f"Error downloading file {source} to {target}")

        # s3 -> local target
        self.s3_client.download_file(
            Bucket=self.bucket,
            Key=remote_upload_to,
            Filename=target,
        )
        self.s3_client.delete_object(Bucket=self.bucket, Key=remote_upload_to)

    def _push_local_dir(self):
        remote_upload_to = self._generate_tmp_s3_path()
        # pack local dir
        _, local_path = tempfile.mkstemp()
        shutil.make_archive(local_path, "gztar", os.getcwd())
        # local source -> s3
        self.s3_client.upload_file(
            Filename=local_path + ".tar.gz",
            Bucket=self.bucket,
            Key=remote_upload_to,
        )
        # remove local archive
        os.unlink(local_path)

        bucket_address = f"s3://{self.bucket}/{remote_upload_to}"
        # s3 -> remote target
        cid = self._run_job(
            f"pip install -q awscli && "
            f"aws s3 cp {bucket_address} archive.tar.gz && "
            f"tar xf archive.tar.gz ",
            {},
        )
        retcode, duration = self._wait_job(cid)
        if retcode != 0:
            raise FileUploadError(
                f"Error uploading local dir to session "
                f"{self.cluster_manager.cluster_name}."
            )
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=remote_upload_to)
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
        self.s3_client.upload_file(
            Filename=source,
            Bucket=self.bucket,
            Key=remote_upload_to,
        )
        # s3 -> remote target
        bucket_address = f"s3://{self.bucket}/{remote_upload_to}"
        cid = self._run_job(
            "pip install -q awscli && " f"aws s3 cp {bucket_address} {target}",
            {},
        )
        retcode, duration = self._wait_job(cid)

        if retcode != 0:
            raise FileUploadError(f"Error uploading file {source} to {target}")

        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=remote_upload_to)
        except Exception as e:
            logger.warning(f"Could not remove temporary S3 object: {e}")
