import time
import logger
import subprocess
from ray.tune import tune_function
import types
import tempfile
import os

import boto3
import botocore

s3 = boto3.resource('s3')


class Syncer(object):
    def __init__(self, local_dir, remote_dir, sync_fn=None):
        self._local_dir = local_dir
        self._remote_dir = remote_dir
        self.logfile = tempfile.NamedTemporaryFile(
            prefix="log_sync", dir=self.local_dir, suffix=".log", delete=False)

        self.last_sync_time = 0
        self.sync_process = None

        self.sync_func = None
        self.sync_cmd_tmpl = None
        if isinstance(sync_fn, types.FunctionType) or isinstance(
                sync_fn, tune_function):
            self.sync_func = sync_fn
        elif isinstance(sync_fn, str):
            self.sync_cmd_tmpl = sync_fn
        else:
            raise ValueError(type(sync_fn))

    def sync(self, source, target, force=False):
        self.last_sync_time = time.time()
        if self.sync_process:
            self.sync_process.poll()
            if self.sync_process.returncode is None:
                if force:
                    self.sync_process.kill()
                else:
                    logger.warning("Last sync is still in progress, skipping.")
                    return

        if self.remote_dir:
            if self.sync_func:
                try:
                    self.sync_func(source, target)
                except Exception:
                    logger.exception("Sync function failed.")
            else:
                final_cmd = self.get_remote_sync_cmd()
                logger.debug("Running log sync: {}".format(final_cmd))
                self.sync_process = subprocess.Popen(
                    final_cmd, shell=True, stdout=self.logfile)

    def sync_down(self, *args, **kwargs):
        self.sync(self._remote_dir, self._local_dir, *args, **kwargs)

    def sync_up(self, *args, **kwargs):
        self.sync(self._local_dir, self._remote_dir, *args, **kwargs)

    def remote_path_exists(self, remote_path):
        # self._remote_dir == "s3://<bucket>/ray_results"
        absolute_path = os.path.join(self._remote_dir, remote_path)

        try:
            s3.Object('my-bucket', 'dootdoot.jpg').load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            else:
                raise

        return True

    def close(self):
        self.logfile.close()

    def wait(self):
        if self.sync_process:
            self.sync_process.wait()

    def get_remote_sync_cmd(self):
        if self.sync_cmd_tmpl:
            local_to_remote_sync_cmd = (self.sync_cmd_tmpl.format(
                local_dir=quote(self.local_dir),
                remote_dir=quote(self.remote_dir)))
        elif self.remote_dir.startswith(S3_PREFIX):
            local_to_remote_sync_cmd = (
                "aws s3 sync {local_dir} {remote_dir}".format(
                    local_dir=quote(self.local_dir),
                    remote_dir=quote(self.remote_dir)))
        elif self.remote_dir.startswith(GCS_PREFIX):
            local_to_remote_sync_cmd = (
                "gsutil rsync -r {local_dir} {remote_dir}".format(
                    local_dir=quote(self.local_dir),
                    remote_dir=quote(self.remote_dir)))
        else:
            logger.warning("Remote sync unsupported, skipping.")
            local_to_remote_sync_cmd = None

        return local_to_remote_sync_cmd

    def sync_if_needed(self):
        if time.time() - self.last_sync_time > 300:
            self.sync_down()


def get_syncer(remote_dir, *args, **kwargs):
    if "s3://" in remote_dir:
        return S3Syncer
