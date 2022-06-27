from ray_release.cluster_manager.cluster_manager import ClusterManager
from typing import Dict, Any, Optional


class AwsVmClusterManager(ClusterManager):

    def __init__(self, a, b):
        super(ClusterManager, self).__init__()
        #super(ClusterManager, self).__init__(test_name='TODO', project_id='TODO', sdk='TODO')

    def create_cluster_env(self, _repeat: bool = True):
        raise NotImplementedError

    def build_cluster_env(self, timeout: float):
        raise NotImplementedError

    def fetch_build_info(self):
        raise NotImplementedError

    def create_cluster_compute(self, _repeat: bool = True):
        raise NotImplementedError

    def build_configs(self, timeout: float):
        pass

    def delete_configs(self):
        raise NotImplementedError

    def start_cluster(self, timeout: float):
        # TODO
        pass

    def terminate_cluster(self, wait: bool):
        raise NotImplementedError

    def get_cluster_address(self) -> str:
        raise NotImplementedError
    
    def get_cluster_url(self) -> Optional[str]:
        return None
