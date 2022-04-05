import abc
from typing import Optional

from ray_release.cluster_manager.cluster_manager import ClusterManager


class FileManager(abc.ABC):
    def __init__(self, cluster_manager: ClusterManager):
        self.cluster_manager = cluster_manager

    def upload(self, source: Optional[str] = None, target: Optional[str] = None):
        """Upload source to target.

        Infers target dir from basename if not stated.
        """
        raise NotImplementedError

    def download(self, source: str, target: str):
        """Download source_dir to target_dir."""
        raise NotImplementedError
