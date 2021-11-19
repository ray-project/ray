import os
import shutil
import subprocess
import tempfile
from typing import Optional

from ray import logger
from ray.tune.sync_client import (S3_PREFIX, GS_PREFIX, HDFS_PREFIX,
                                  ALLOWED_REMOTE_PREFIXES)
from ray.util import PublicAPI


def is_cloud_target(target: str):
    return any(target.startswith(prefix) for prefix in ALLOWED_REMOTE_PREFIXES)


def _clear_bucket(bucket: str):
    if not is_cloud_target(bucket):
        raise ValueError(
            f"Could not clear bucket contents: "
            f"Bucket `{bucket}` is not a valid or supported cloud target.")

    try:
        if bucket.startswith(S3_PREFIX):
            subprocess.check_call(
                ["aws", "s3", "rm", "--recursive", "--quiet", bucket])
        elif bucket.startswith(GS_PREFIX):
            subprocess.check_call(["gsutil", "-m", "rm", "-f", "-r", bucket])
        elif bucket.startswith(HDFS_PREFIX):
            subprocess.check_call(["hdfs", "dfs", "-rm", "-r", bucket])

    except Exception as e:
        logger.warning(
            f"Caught exception when clearing bucket `{bucket}`: {e}")


def _download_from_bucket(bucket: str, local_path: str):
    if not is_cloud_target(bucket):
        raise ValueError(
            f"Could not download from bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target.")

    if bucket.startswith(S3_PREFIX):
        subprocess.check_call(
            ["aws", "s3", "cp", "--recursive", "--quiet", bucket, local_path])
    elif bucket.startswith(GS_PREFIX):
        subprocess.check_call(["gsutil", "-m", "cp", "-r", bucket, local_path])
    elif bucket.startswith(HDFS_PREFIX):
        subprocess.check_call(["hdfs", "dfs", "-get", bucket, local_path])


def _upload_to_bucket(bucket: str, local_path: str):
    if not is_cloud_target(bucket):
        raise ValueError(
            f"Could not download from bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target.")

    if bucket.startswith(S3_PREFIX):
        subprocess.check_call(
            ["aws", "s3", "cp", "--recursive", "--quiet", local_path, bucket])
    elif bucket.startswith(GS_PREFIX):
        subprocess.check_call(["gsutil", "-m", "cp", "-r", local_path, bucket])
    elif bucket.startswith(HDFS_PREFIX):
        subprocess.check_call(["hdfs", "dfs", "-put", local_path, bucket])


@PublicAPI(stability="beta")
class TrialCheckpoint:
    def __init__(self,
                 local_path: Optional[str] = None,
                 cloud_path: Optional[str] = None):
        self.local_path = local_path
        self.cloud_path = cloud_path

    def download(self,
                 cloud_path: Optional[str] = None,
                 local_path: Optional[str] = None,
                 overwrite: bool = False,
                 update_local_path: bool = True) -> str:
        """Download checkpoint from cloud.

        If a ``local_path`` argument is provided and ``update_local_path=True``
        the ``self.local_path`` will be overwritten.

        This will then fetch the checkpoint directory from cloud storage
        and save it to ``self.local_path``.

        Args:
            local_path (Optional[str]): Local path to save checkpoint at.
            overwrite (bool): If True, overwrites potential existing
                checkpoint. If False, exits if ``self.local_dir`` already
                exists and has files in it.
            update_local_path (bool): If True, the ``self.local_path``
                attribute will be updated by tehe ``local_path`` argument,
                if provided.

        """
        cloud_path = cloud_path or self.cloud_path
        if not cloud_path:
            raise RuntimeError(
                "Could not download trial checkpoint: No cloud "
                "path is set. Fix this by either passing a "
                "`cloud_path` to your call to `download()` or by "
                "passing a `cloud_path` into the constructor. The latter "
                "should automatically be done if you pass the correct "
                "`tune.SyncConfig`.")

        local_path = local_path or self.local_path

        if not local_path:
            raise RuntimeError(
                "Could not download trial checkpoint: No local "
                "path is set. Fix this by either passing a "
                "`local_path` to your call to `download()` or by "
                "passing a `local_path` into the constructor.")

        if update_local_path:
            self.local_path = local_path

        if not overwrite and (os.path.exists(local_path)
                              and len(os.listdir(local_path)) > 0):
            # Local path already exists and we should not overwrite,
            # so return.
            return local_path

        # Else: Actually download

        # Delete existing dir
        shutil.rmtree(local_path, ignore_errors=True)
        # Re-create
        os.makedirs(local_path, 0o755, exist_ok=True)

        # Here we trigger the actual download
        _download_from_bucket(cloud_path, local_path)

        # Local dir exists and is not empty
        return local_path

    def upload(self,
               cloud_path: Optional[str] = None,
               local_path: Optional[str] = None,
               clean_before: bool = False,
               update_cloud_path: bool = True):
        local_path = local_path or self.local_path
        if not local_path:
            raise RuntimeError("Could not upload trial checkpoint: No local "
                               "path is set. Fix this by either passing a "
                               "`local_path` to your call to `upload()` or by "
                               "passing a `local_path` into the constructor.")

        cloud_path = cloud_path or self.cloud_path
        if not cloud_path:
            raise RuntimeError(
                "Could not download trial checkpoint: No cloud "
                "path is set. Fix this by either passing a "
                "`cloud_path` to your call to `download()` or by "
                "passing a `cloud_path` into the constructor. The latter "
                "should automatically be done if you pass the correct "
                "`tune.SyncConfig`.")

        if update_cloud_path:
            self.cloud_path = cloud_path

        if clean_before:
            logger.info(
                f"Clearing bucket contents before upload: {cloud_path}")
            _clear_bucket(cloud_path)

        # Actually upload
        _upload_to_bucket(cloud_path, local_path)

        return cloud_path

    def save(self,
             path: Optional[str] = None,
             overwrite: bool = True,
             force_download: bool = False):
        """Save trial checkpoint to directory or cloud storage.

        If this trial has a local path, store to the
        """
        temp_dirs = set()
        # Per default, save cloud checkpoint
        if not path:
            if self.cloud_path and self.local_path:
                path = self.local_path
            else:
                raise RuntimeError(
                    "Cannot save trial checkpoint: No target path "
                    "specified and no default local directory available. "
                    "Please pass a `path` argument to `save()`.")

        if is_cloud_target(path):
            # Storing on cloud
            if not self.local_path and not self.cloud_path:
                raise RuntimeError(
                    f"Cannot save trial checkpoint to cloud target "
                    f"`{path}`: No existing local or cloud path was "
                    f"found. This indicates an error when loading "
                    f"the checkpoints. Please report this issue.")
            elif not self.local_path:
                # No local copy, yet. Download to temp dir
                local_path = tempfile.mkdtemp(prefix="tune_checkpoint_")
                temp_dirs.add(local_path)
            else:
                local_path = self.local_path

            if self.cloud_path:
                # Do not update local path as it might be a temp file
                local_path = self.download(
                    local_path=local_path,
                    overwrite=force_download,
                    update_local_path=False)

            # We should now have a checkpoint available locally
            if not os.path.exists(local_path) or len(
                    os.listdir(local_path)) == 0:
                raise RuntimeError(
                    f"No checkpoint found in directory `{local_path}` after "
                    f"download - maybe the bucket is empty or downloading "
                    f"failed?")

            # Only update cloud path if it wasn't set before
            cloud_path = self.upload(
                cloud_path=path,
                local_path=local_path,
                clean_before=True,
                update_cloud_path=not self.cloud_path)

            # Clean up temporary directories
            for temp_dir in temp_dirs:
                shutil.rmtree(temp_dir)

            return cloud_path

        # Else: path is a local target
        if self.local_path and not force_download:
            # If we have a local copy, use it

            if path == self.local_path:
                # Nothing to do
                return self.local_path

            # Both local, just copy tree
            shutil.copytree(self.local_path, path)
            return path

        # Else: Download
        return self.download(local_path=path, overwrite=force_download)
