import abc
from typing import Optional

from ray_release.cluster_manager.cluster_manager import ClusterManager

RAY_REMOTE_HOME = "/home/ray"


class FileManager(abc.ABC):
    def __init__(self, cluster_manager: ClusterManager):
        self.cluster_manager = cluster_manager

    # @staticmethod
    # def get_target(source: str, target: Optional[str] = None):
    #     if target:
    #         return target
    #
    #     target_dir_basename = os.path.basename(source)
    #     return f"{RAY_REMOTE_HOME}/{target_dir_basename}"

    def upload(self,
               source: Optional[str] = None,
               target: Optional[str] = None):
        """Upload source to target.

        Infers target dir from basename if not stated.
        """
        raise NotImplementedError

    def download(self, source: str, target: str):
        """Download source_dir to target_dir."""
        raise NotImplementedError
