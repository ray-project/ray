import abc
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray_release.aws import (
    add_tags_to_aws_config,
    RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING,
)
from ray_release.anyscale_util import get_project_name
from ray_release.config import DEFAULT_AUTOSUSPEND_MINS, DEFAULT_MAXIMUM_UPTIME_MINS
from ray_release.test import Test
from ray_release.exception import CloudInfoError
from ray_release.util import anyscale_cluster_url, dict_hash, get_anyscale_sdk
from ray_release.logger import logger

if TYPE_CHECKING:
    from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK


class ClusterManager(abc.ABC):
    def __init__(
        self,
        test: Test,
        project_id: str,
        sdk: Optional["AnyscaleSDK"] = None,
        smoke_test: bool = False,
    ):
        self.sdk = sdk or get_anyscale_sdk()

        self.test = test
        self.smoke_test = smoke_test
        self.project_id = project_id
        self.project_name = get_project_name(self.project_id, self.sdk)

        self.cluster_name = (
            f"{test.get_name()}{'-smoke-test' if smoke_test else ''}_{int(time.time())}"
        )
        self.cluster_id = None

        self.cluster_env = None
        self.cluster_env_name = None
        self.cluster_env_id = None
        self.cluster_env_build_id = None

        self.cluster_compute = None
        self.cluster_compute_name = None
        self.cluster_compute_id = None
        self.cloud_provider = None

        self.autosuspend_minutes = DEFAULT_AUTOSUSPEND_MINS
        self.maximum_uptime_minutes = DEFAULT_MAXIMUM_UPTIME_MINS

    def set_cluster_env(self):
        byod_image_name_normalized = (
            self.test.get_anyscale_byod_image()
            .replace("/", "_")
            .replace(":", "_")
            .replace(".", "_")
        )
        self.cluster_env_name = (
            f"{byod_image_name_normalized}"
            f"__env__{dict_hash(self.test.get_byod_runtime_env())}"
        )

    def set_cluster_compute(
        self,
        cluster_compute: Dict[str, Any],
        extra_tags: Optional[Dict[str, str]] = None,
    ):
        extra_tags = extra_tags or {}
        self.cluster_compute = cluster_compute
        self.cluster_compute.setdefault(
            "idle_termination_minutes", self.autosuspend_minutes
        )
        self.cluster_compute.setdefault(
            "maximum_uptime_minutes", self.maximum_uptime_minutes
        )
        self.cloud_provider = self._get_cloud_provider(cluster_compute)
        self.cluster_compute = self._annotate_cluster_compute(
            self.cluster_compute,
            cloud_provider=self.cloud_provider,
            extra_tags=extra_tags,
        )

        self.cluster_compute_name = (
            f"{self.project_name}_{self.project_id[4:8]}"
            f"__compute__{self.test.get_name()}__"
            f"{dict_hash(self.cluster_compute)}"
        )

    def _get_cloud_provider(self, cluster_compute: Dict[str, Any]) -> Optional[str]:
        if not cluster_compute or "cloud_id" not in cluster_compute:
            return None
        try:
            return self.sdk.get_cloud(cluster_compute["cloud_id"]).result.provider
        except Exception as e:
            raise CloudInfoError(f"Could not obtain cloud information: {e}") from e

    def _annotate_cluster_compute(
        self,
        cluster_compute: Dict[str, Any],
        cloud_provider: str,
        extra_tags: Dict[str, str],
    ) -> Dict[str, Any]:
        if not extra_tags or cloud_provider != "AWS":
            return cluster_compute

        cluster_compute = cluster_compute.copy()
        aws = cluster_compute.get("aws", {})
        cluster_compute["aws"] = add_tags_to_aws_config(
            aws, extra_tags, RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
        )
        return cluster_compute

    def build_configs(self, timeout: float = 30.0):
        raise NotImplementedError

    def delete_configs(self):
        raise NotImplementedError

    def start_cluster(self, timeout: float = 600.0):
        raise NotImplementedError

    def terminate_cluster(self, wait: bool = False):
        try:
            self.terminate_cluster_ex(wait=False)
        except Exception as e:
            logger.exception(f"Could not terminate cluster: {e}")

    def terminate_cluster_ex(self, wait: bool = False):
        raise NotImplementedError

    def get_cluster_address(self) -> str:
        raise NotImplementedError

    def get_cluster_url(self) -> Optional[str]:
        if not self.project_id or not self.cluster_id:
            return None
        return anyscale_cluster_url(self.project_id, self.cluster_id)
