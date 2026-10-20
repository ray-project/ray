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
        self.cluster_env_name = byod_image_name_normalized

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
        is_new_schema = self.test.uses_anyscale_sdk_2026()
        self.cluster_compute = self._annotate_cluster_compute(
            self.cluster_compute,
            extra_tags=extra_tags,
            is_new_schema=is_new_schema,
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
        is_new_schema: bool = False,
    ) -> Dict[str, Any]:
        if not extra_tags or self.cloud_provider != "aws":
            return cluster_compute

        cluster_compute = cluster_compute.copy()

        if is_new_schema:
            # Anyscale picks an effective advanced_instance_config per node
            # group: when a per-group spec is set, it replaces (does not
            # merge with) the cluster-level base. So billing tags must land
            # wherever the effective spec actually comes from. Rules:
            #   - No advanced_instance_config anywhere -> tag base only
            #     (auto-create base spec to hold tags).
            #   - Base only -> tag base only.
            #   - Per-group only (head and/or workers, no base) -> tag those
            #     per-group specs only. Do NOT auto-create a base spec; that
            #     would tag a base that no node group uses and could
            #     confuse readers.
            #   - Base AND per-group -> tag everywhere. Both are
            #     load-bearing: the base covers node groups without an
            #     override, the per-group covers node groups with one.
            # Normalize missing/null head_node and worker_nodes to empty
            # containers so the downstream checks don't need to special-case
            # them. When head is missing/null the resulting `head` is a
            # fresh `{}` we never mutate (has_head_aic is False); when
            # head_node is a real dict we mutate that dict in place below.
            head = cluster_compute.get("head_node") or {}
            workers = cluster_compute.get("worker_nodes") or []
            has_head_aic = "advanced_instance_config" in head
            has_worker_aic = any(
                isinstance(w, dict) and "advanced_instance_config" in w for w in workers
            )
            has_base_aic = "advanced_instance_config" in cluster_compute
            should_tag_base = has_base_aic or not (has_head_aic or has_worker_aic)
            if should_tag_base:
                aws = cluster_compute.get("advanced_instance_config", {})
                cluster_compute["advanced_instance_config"] = add_tags_to_aws_config(
                    aws, extra_tags, RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING
                )
            if has_head_aic:
                head["advanced_instance_config"] = add_tags_to_aws_config(
                    head["advanced_instance_config"],
                    extra_tags,
                    RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING,
                )
            for worker in workers:
                if isinstance(worker, dict) and "advanced_instance_config" in worker:
                    worker["advanced_instance_config"] = add_tags_to_aws_config(
                        worker["advanced_instance_config"],
                        extra_tags,
                        RELEASE_AWS_RESOURCE_TYPES_TO_TRACK_FOR_BILLING,
                    )
        else:
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
