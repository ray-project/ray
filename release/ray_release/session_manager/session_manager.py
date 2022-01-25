import abc
from typing import Dict, Any

from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK

from ray_release.util import dict_hash


class SessionManager(abc.ABC):
    def __init__(self, sdk: AnyscaleSDK, project_id: str, test_name: str):
        self.sdk = sdk

        self.project_id = project_id
        self.test_name = test_name

        self.cluster_name = (
            f"{test_name}_1234"  # Todo
        )
        self.cluster_id = None

        self.cluster_env = None
        self.cluster_env_name = None
        self.cluster_env_id = None
        self.cluster_env_build_id = None

        self.cluster_compute = None
        self.cluster_compute_name = None
        self.cluster_compute_id = None

    def set_cluster_env(self, cluster_env: Dict[str, Any]):
        self.cluster_env = cluster_env
        self.cluster_env_name = (f"{self.project_id}__env__{self.test_name}__"
                                 f"{dict_hash(cluster_env)}")

    def set_cluster_compute(self, cluster_compute: Dict[str, Any]):
        self.cluster_compute = cluster_compute
        self.cluster_compute_name = (
            f"{self.project_id}__compute__{self.test_name}__"
            f"{dict_hash(cluster_compute)}")

    def build_configs(self, timeout: float = 30.):
        raise NotImplementedError

    def delete_configs(self):
        raise NotImplementedError

    def start_session(self):
        raise NotImplementedError

    def terminate_cluster(self):
        raise NotImplementedError

    def get_cluster_address(self) -> str:
        raise NotImplementedError
