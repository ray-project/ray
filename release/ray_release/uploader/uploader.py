import abc
import os
from typing import Optional

from ray_release.cluster_manager.session_manager import ClusterManager

RAY_REMOTE_HOME = "/home/ray"


class Uploader(abc.ABC):
    def __init__(self, session_manager: ClusterManager):
        self.session_manager = session_manager

    @staticmethod
    def get_target_dir(source_dir: str, target_dir: Optional[str] = None):
        if target_dir:
            return target_dir

        target_dir_basename = os.path.basename(source_dir)
        return f"{RAY_REMOTE_HOME}/{target_dir_basename}"

    def upload(self, source_dir: str, target_dir: Optional[str] = None):
        """Upload source_dir to target_dir.

        Infers target dir from basename if not stated.
        """
        raise NotImplementedError
