import os
import random
import shutil
import string
import sys
import tempfile
from typing import Optional

import anyscale
import boto3

from ray_release.aws import RELEASE_AWS_BUCKET
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.job_manager import JobManager
from ray_release.exception import FileDownloadError, FileUploadError
from ray_release.file_manager.file_manager import FileManager
from ray_release.logger import logger
from ray_release.util import exponential_backoff_retry


class VmStackFileManager(FileManager):
    def __init__(self, cluster_manager: ClusterManager):
        super(VmStackFileManager, self).__init__(cluster_manager=cluster_manager)

    def download(self, source: str, target: str):
        raise NotImplementedError

    def upload(self, source: Optional[str] = None, target: Optional[str] = None):
        raise NotImplementedError
