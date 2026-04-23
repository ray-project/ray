import json
import os
import sys
import time
import unittest
from typing import Any, Dict
from unittest.mock import MagicMock

import jsonschema
import pytest

from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.ippr_provider import (
    KubeRayIPPRProvider,
)
from ray.autoscaler.v2.schema import IPPRStatus
from ray.autoscaler.v2.tests.test_node_provider import (
    MockKubernetesHttpApiClient,
)


def _make_ray_cluster_with_ippr(
    groups_spec: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    # Minimal RayCluster CR with head + one worker group and IPPR annotation
    head_container = {
        "name": "ray-head",
        "resources": {
            "requests": {"cpu": "1", "memory": "2Gi"},
            "limits": {"cpu": "2", "memory": "4Gi"},
        },
        "resizePolicy": [
            {"resourceName": "cpu", "restartPolicy": "NotRequired"},
            {"resourceName": "memory", "restartPolicy": "NotRequired"},
        ],
    }
    worker_container = {
        "name": "ray-worker",
        "resources": {
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "1500m", "memory": "2Gi"},
        },
        "resizePolicy": [
            {"resourceName": "cpu", "restartPolicy": "NotRequired"},
            {"resourceName": "memory", "restartPolicy": "NotRequired"},
        ],
    }

    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayCluster",
        "metadata": {
            "name": "test-raycluster",
            "annotations": {"ray.io/ippr": json.dumps({"groups": groups_spec})},
        },
        "spec": {
            "headGroupSpec": {
                "groupName": "headgroup",
                "template": {"spec": {"containers": [head_container]}},
            },
            "workerGroupSpecs": [
                {
                    "groupName": "small-group",
                    "template": {"spec": {"containers": [worker_container]}},
                }
            ],
        },
    }


class TestKubeRayIPPRProvider(unittest.TestCase):
    def setUp(self):
        self.gcs = MagicMock()
        self.k8s = MockKubernetesHttpApiClient({"items": []}, {"spec": {}})
        self.provider = KubeRayIPPRProvider(
            gcs_client=self.gcs, k8s_api_client=self.k8s
        )

    def test_validate_noop_when_annotation_missing_or_none(self):
        # None ray_cluster is no-op
        self.provider.validate_and_set_ippr_specs(None)
        assert self.provider.get_ippr_specs().groups == {}

        # Removing the annotation should clear previously loaded specs.
        rc = _make_ray_cluster_with_ippr(
            {
                "headgroup": {
                    "max-cpu": 4,
                    "max-memory": "8Gi",
                    "resize-timeout": 60,
                }
            }
        )
        self.provider.validate_and_set_ippr_specs(rc)
        assert self.provider.get_ippr_specs().groups != {}

        # RayCluster without ippr annotation disables IPPR.
        rc = _make_ray_cluster_with_ippr({})
        rc["metadata"]["annotations"].pop("ray.io/ippr")
        self.provider.validate_and_set_ippr_specs(rc)
        assert self.provider.get_ippr_specs().groups == {}

    def test_validate_and_set_ippr_specs_success(self):
        rc = _make_ray_cluster_with_ippr(
            {
                "headgroup": {
                    "max-cpu": 4,
                    "max-memory": "8Gi",
                    "resize-timeout": 60,
                },
                "small-group": {
                    "max-cpu": 3,
                    "max-memory": "4Gi",
                    "resize-timeout": 30,
                },
            }
        )

        self.provider.validate_and_set_ippr_specs(rc)
        specs = self.provider.get_ippr_specs()

        head = specs.groups["headgroup"]
        small = specs.groups["small-group"]

        assert head.min_cpu == 2.0
        assert head.max_cpu == 4.0
        assert head.min_memory == 4 * 1024 * 1024 * 1024
        assert head.max_memory == 8 * 1024 * 1024 * 1024
        assert head.resize_timeout == 60

        assert small.min_cpu == 1.5
        assert small.max_cpu == 3.0
        assert small.min_memory == 2 * 1024 * 1024 * 1024
        assert small.max_memory == 4 * 1024 * 1024 * 1024
        assert small.resize_timeout == 30

    def test_invalid_ippr_specs_missing_fields(self):
        rc = _make_ray_cluster_with_ippr({"small-group": {}})
        with pytest.raises(jsonschema.ValidationError):
            self.provider.validate_and_set_ippr_specs(rc)

    def test_invalid_ippr_specs_missing_resize_timeout(self):
        rc = _make_ray_cluster_with_ippr(
            {
                "small-group": {
                    "max-cpu": 2,
                    "max-memory": 2147483648,
                }
            }
        )
        with pytest.raises(jsonschema.ValidationError):
            self.provider.validate_and_set_ippr_specs(rc)

    def test_invalid_ippr_specs_missing_max_cpu(self):
        rc = _make_ray_cluster_with_ippr(
            {
                "small-group": {
                    "max-memory": 2147483648,
                    "resize-timeout": 10,
                }
            }
        )
        with pytest.raises(jsonschema.ValidationError):
            self.provider.validate_and_set_ippr_specs(rc)

    def test_invalid_ippr_specs_missing_max_memory(self):
        rc = _make_ray_cluster_with_ippr(
            {
                "small-group": {
                    "max-cpu": 2,
                    "resize-timeout": 10,
                }
            }
        )
        with pytest.raises(jsonschema.ValidationError):
            self.provider.validate_and_set_ippr_specs(rc)

    def test_validate_and_set_ippr_specs_invalid_ray_params_cpu(self):
        rc = _make_ray_cluster_with_ippr(
            {
                "small-group": {
                    "max-cpu": 2,
                    "max-memory": 2147483648,
                    "resize-timeout": 10,
                }
            }
        )
        # Inject forbidden rayStartParams
        rc["spec"]["workerGroupSpecs"][0]["rayStartParams"] = {"num-cpus": "2"}

        with pytest.raises(ValueError):
            self.provider.validate_and_set_ippr_specs(rc)

    def test_validate_and_set_ippr_specs_invalid_ray_params_memory(self):
        rc = _make_ray_cluster_with_ippr(
            {
                "small-group": {
                    "max-cpu": 2,
                    "max-memory": 2147483648,
                    "resize-timeout": 10,
                }
            }
        )
        # Inject forbidden rayStartParams
        rc["spec"]["workerGroupSpecs"][0]["rayStartParams"] = {"memory": "200000"}

        with pytest.raises(ValueError):
            self.provider.validate_and_set_ippr_specs(rc)

    def test_validate_and_set_ippr_specs_missing_cpu_request(self):
        rc = _make_ray_cluster_with_ippr(
            {
                "small-group": {
                    "max-cpu": 2,
                    "max-memory": 2147483648,
                    "resize-timeout": 10,
                }
            }
        )
        # Remove required cpu request in pod template
        rc["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0][
            "resources"
        ] = {"requests": {"memory": "1Gi"}}

        with pytest.raises(ValueError):
            self.provider.validate_and_set_ippr_specs(rc)

    def test_validate_and_set_ippr_specs_missing_memory_request(self):
        rc = _make_ray_cluster_with_ippr(
            {
                "small-group": {
                    "max-cpu": 2,
                    "max-memory": 2147483648,
                    "resize-timeout": 10,
                }
            }
        )
        # Remove required memory request in pod template
        rc["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0][
            "resources"
        ] = {"requests": {"cpu": "1"}}

        with pytest.raises(ValueError):
            self.provider.validate_and_set_ippr_specs(rc)

    def test_validate_and_set_ippr_specs_invalid_resize_policy(self):
        rc = _make_ray_cluster_with_ippr(
            {
                "small-group": {
                    "max-cpu": 2,
                    "max-memory": 2147483648,
                    "resize-timeout": 10,
                }
            }
        )
        rc["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0][
            "resizePolicy"
        ] = [{"resourceName": "cpu", "restartPolicy": "RestartContainer"}]

        with pytest.raises(ValueError):
            self.provider.validate_and_set_ippr_specs(rc)

    def test_sync_with_raylets_calls_gcs_and_clears_resizing_at_on_pod(self):
        Gi = 1024 * 1024 * 1024
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)
        spec = self.provider.get_ippr_specs().groups["small-group"]
        raylet_hex = "0" * 56
        st = IPPRStatus(
            cloud_instance_id="ray-worker-1",
            spec=spec,
            current_cpu=2.0,
            current_memory=4 * Gi,
            desired_cpu=2.0,
            desired_memory=4 * Gi,
            resizing_at=123,
            raylet_id=raylet_hex,
        )
        self.provider._ippr_statuses["ray-worker-1"] = st
        assert st.need_sync_with_raylet()

        self.gcs.resize_raylet_resource_instances.return_value = {
            "CPU": 2.0,
            "memory": 4 * Gi,
        }

        self.provider.sync_with_raylets()

        self.gcs.resize_raylet_resource_instances.assert_called_once_with(
            raylet_hex,
            {"CPU": 2.0, "memory": 4 * Gi},
        )

        ann_payload = self.k8s.get_patches("pods/ray-worker-1")
        assert ann_payload is not None
        parsed = json.loads(
            ann_payload["metadata"]["annotations"]["ray.io/ippr-status"]
        )
        assert parsed["raylet-id"] == raylet_hex
        assert parsed["resizing-at"] is None

    def test_sync_with_raylets_missing_raylet_address_noop(self):
        Gi = 1024 * 1024 * 1024
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)
        spec = self.provider.get_ippr_specs().groups["small-group"]
        raylet_hex = "0" * 56
        st = IPPRStatus(
            cloud_instance_id="ray-worker-1",
            spec=spec,
            current_cpu=2.0,
            current_memory=4 * Gi,
            desired_cpu=2.0,
            desired_memory=4 * Gi,
            resizing_at=int(time.time()),
            raylet_id=raylet_hex,
        )
        self.provider._ippr_statuses["ray-worker-1"] = st
        self.gcs.resize_raylet_resource_instances.side_effect = ValueError(
            f"Raylet {raylet_hex} is not alive."
        )
        self.provider.sync_with_raylets()

        with pytest.raises(KeyError):
            _ = self.k8s.get_patches("pods/ray-worker-1")


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
