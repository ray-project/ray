import abc
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray_release.anyscale_util import get_project_name
from ray_release.aws import (
    RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING,
    add_tags_to_aws_config,
)
from ray_release.config import DEFAULT_AUTOSUSPEND_MINS, DEFAULT_MAXIMUM_UPTIME_MINS
from ray_release.test import Test
from ray_release.util import dict_hash, get_anyscale_sdk

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

        self.cluster_env = None
        self.cluster_env_name = None
        self.cluster_env_id: Optional[str] = None
        self.cluster_env_build_id: Optional[str] = None

        self.cluster_compute = None
        self.cluster_compute_name = None
        self.cluster_compute_id = None
        self.cloud_provider = test.get_cloud_env()

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
        self.cluster_compute = self._annotate_cluster_compute(
            self.cluster_compute,
            extra_tags=extra_tags,
        )

        self.cluster_compute_name = (
            f"{self.project_name}_{self.project_id[4:8]}"
            f"__compute__{self.test.get_name()}__"
            f"{dict_hash(self.cluster_compute)}"
        )

    def _annotate_cluster_compute(
        self,
        cluster_compute: Dict[str, Any],
        extra_tags: Dict[str, str],
    ) -> Dict[str, Any]:
        if not extra_tags or self.cloud_provider != "aws":
            return cluster_compute

        cluster_compute = cluster_compute.copy()
        if "aws" in cluster_compute:
            raise ValueError(
                "aws field is invalid in compute config, "
                "use advanced_configurations_json instead"
            )
        aws = cluster_compute.get("advanced_configurations_json", {})
        cluster_compute["advanced_configurations_json"] = add_tags_to_aws_config(
            aws, extra_tags, RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
        )
        return cluster_compute

    def build_configs(self, timeout: float = 30.0):
        raise NotImplementedError
