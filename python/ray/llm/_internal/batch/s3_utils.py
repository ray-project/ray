# SPDX-License-Identifier: Apache-2.0
# Adapted from vllm.transformers_utils.s3_utils.

import fnmatch
import os
import shutil
import signal
import tempfile
import threading
from pathlib import Path
from typing import Optional

try:
    import boto3
except ImportError:
    boto3 = None


def _filter_allow(paths: list[str], patterns: list[str]) -> list[str]:
    return [
        path
        for path in paths
        if any(fnmatch.fnmatch(path, pattern) for pattern in patterns)
    ]


def _filter_ignore(paths: list[str], patterns: list[str]) -> list[str]:
    return [
        path
        for path in paths
        if not any(fnmatch.fnmatch(path, pattern) for pattern in patterns)
    ]


def glob(
    s3=None, path: str = "", allow_pattern: Optional[list[str]] = None
) -> list[str]:
    """List full file names from S3 path and filter by allow pattern.

    Args:
        s3: S3 client to use.
        path: The S3 path to list from.
        allow_pattern: A list of patterns of which files to pull.

    Returns:
        list[str]: List of full S3 paths allowed by the pattern
    """
    if s3 is None:
        s3 = boto3.client("s3")
    if not path.endswith("/"):
        path = path + "/"
    bucket_name, _, paths = list_files(s3, path=path, allow_pattern=allow_pattern)
    return [f"s3://{bucket_name}/{path}" for path in paths]


def list_files(
    s3,
    path: str,
    allow_pattern: Optional[list[str]] = None,
    ignore_pattern: Optional[list[str]] = None,
) -> tuple[str, str, list[str]]:
    """List files from S3 path and filter by pattern.

    Args:
        s3: S3 client to use.
        path: The S3 path to list from.
        allow_pattern: A list of patterns of which files to pull.
        ignore_pattern: A list of patterns of which files not to pull.

    Returns:
        tuple[str, str, list[str]]: A tuple where:
            - The first element is the bucket name
            - The second element is string represent the bucket
              and the prefix as a dir like string
            - The third element is a list of files allowed or
              disallowed by pattern
    """
    parts = path.removeprefix("s3://").split("/")
    prefix = "/".join(parts[1:])
    bucket_name = parts[0]

    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    paths = [obj["Key"] for obj in objects.get("Contents", [])]

    paths = _filter_ignore(paths, ["*/"])
    if allow_pattern is not None:
        paths = _filter_allow(paths, allow_pattern)

    if ignore_pattern is not None:
        paths = _filter_ignore(paths, ignore_pattern)

    return bucket_name, prefix, paths


class S3Model:
    """A class representing a S3 model mirrored into a temporary directory.

    Args:
        remove_on_close: Whether to remove the temporary directory on close
            (i.e. when the S3Model object is deleted).

    Methods:
        pull_files(): Pull model from S3 to the temporary directory.
    """

    def __init__(self, remove_on_close: bool = True) -> None:
        self.s3 = boto3.client("s3")
        self.remove_on_close = remove_on_close

        if (
            self.remove_on_close
            and threading.current_thread() is threading.main_thread()
        ):
            for sig in (signal.SIGINT, signal.SIGTERM):
                existing_handler = signal.getsignal(sig)
                signal.signal(sig, self._close_by_signal(existing_handler))

        self.dir = tempfile.mkdtemp()

    def __del__(self):
        if self.remove_on_close:
            self._close()

    def _close(self) -> None:
        if os.path.exists(self.dir):
            shutil.rmtree(self.dir)

    def _close_by_signal(self, existing_handler=None):
        def new_handler(signum, frame):
            self._close()
            if existing_handler:
                existing_handler(signum, frame)

        return new_handler

    def pull_files(
        self,
        s3_model_path: str = "",
        allow_pattern: Optional[list[str]] = None,
        ignore_pattern: Optional[list[str]] = None,
    ) -> None:
        """
        Pull files from S3 storage into the temporary directory.

        Args:
            s3_model_path: The S3 path of the model.
            allow_pattern: A list of patterns of which files to pull.
            ignore_pattern: A list of patterns of which files not to pull.

        """
        if not s3_model_path.endswith("/"):
            s3_model_path = s3_model_path + "/"

        bucket_name, base_dir, files = list_files(
            self.s3, s3_model_path, allow_pattern, ignore_pattern
        )
        if len(files) == 0:
            return

        for file in files:
            destination_file = os.path.join(
                self.dir, file.removeprefix(base_dir).lstrip("/")
            )
            local_dir = Path(destination_file).parent
            os.makedirs(local_dir, exist_ok=True)
            self.s3.download_file(bucket_name, file, destination_file)
