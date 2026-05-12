import json
import os
import sys
import time
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import jsonschema
import pytest

from ray.autoscaler._private.kuberay.node_provider import (
    KUBERAY_KIND_HEAD,
    KUBERAY_KIND_WORKER,
    KUBERAY_LABEL_KEY_KIND,
    KUBERAY_LABEL_KEY_TYPE,
)
from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.ippr_provider import (
    KubeRayIPPRProvider,
)
from ray.autoscaler.v2.schema import IPPRStatus
from ray.autoscaler.v2.tests.test_node_provider import (
    MockKubernetesHttpApiClient,
)

# Shared size units
Gi = 1024 * 1024 * 1024


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


def _make_pod(
    name: str,
    group: str,
    kind: str,
    container_name: str,
    status_requests: Dict[str, Any],
    status_limits: Dict[str, Any],
    spec_requests: Dict[str, Any],
    spec_limits: Dict[str, Any],
    conditions: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "metadata": {
            "name": name,
            "labels": {
                KUBERAY_LABEL_KEY_TYPE: group,
                KUBERAY_LABEL_KEY_KIND: kind,
            },
            "annotations": {},
        },
        "spec": {
            "containers": [
                {
                    "name": container_name,
                    "resources": {"requests": spec_requests, "limits": spec_limits},
                }
            ]
        },
        "status": {
            "containerStatuses": [
                {
                    "name": container_name,
                    "resources": {"requests": status_requests, "limits": status_limits},
                }
            ],
            "conditions": conditions or [],
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
        assert head.min_memory == 4 * Gi
        assert head.max_memory == 8 * Gi
        assert head.resize_timeout == 60

        assert small.min_cpu == 1.5
        assert small.max_cpu == 3.0
        assert small.min_memory == 2 * Gi
        assert small.max_memory == 4 * Gi
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

    def test_sync_ippr_status_from_pods_basic(self):
        # Load valid specs first
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 4, "max-memory": "8Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        # Make pods
        pods = [
            _make_pod(
                name="ray-head",
                group="headgroup",
                kind=KUBERAY_KIND_HEAD,
                container_name="ray-head",
                status_requests={"cpu": "2", "memory": "4Gi"},
                status_limits={"cpu": "2", "memory": "4Gi"},
                spec_requests={"cpu": "2", "memory": "4Gi"},
                spec_limits={"cpu": "2", "memory": "4Gi"},
            ),
            _make_pod(
                name="ray-worker-1",
                group="small-group",
                kind=KUBERAY_KIND_WORKER,
                container_name="ray-worker",
                status_requests={"cpu": "500m", "memory": "1Gi"},
                status_limits={"cpu": "1500m", "memory": "2Gi"},
                spec_requests={"cpu": "500m", "memory": "1Gi"},
                spec_limits={"cpu": "1500m", "memory": "2Gi"},
            ),
        ]

        self.provider.sync_ippr_status_from_pods(pods)
        statuses = self.provider.get_ippr_statuses()
        assert "ray-head" not in statuses  # IPPR not enabled for head group

        st = statuses["ray-worker-1"]
        st.raylet_id = "abc"
        assert st.cloud_instance_id == "ray-worker-1"
        assert st.spec == self.provider.get_ippr_specs().groups["small-group"]
        assert st.desired_cpu == 1.5
        assert st.desired_memory == 2 * Gi
        assert st.current_cpu == 1.5
        assert st.current_memory == 2 * Gi
        assert st.resizing_at is None
        assert st.k8s_resize_status is None
        assert st.k8s_resize_message is None
        assert st.suggested_max_cpu is None
        assert st.suggested_max_memory is None

        assert st.is_k8s_resize_finished()
        assert st.can_resize_up()
        assert not st.is_in_progress()
        assert not st.has_resize_request_to_send()

    def test_sync_ippr_status_from_pods_clears_stale_cache_when_specs_empty(self):
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 4, "max-memory": "8Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "500m", "memory": "1Gi"},
            status_limits={"cpu": "1500m", "memory": "2Gi"},
            spec_requests={"cpu": "500m", "memory": "1Gi"},
            spec_limits={"cpu": "1500m", "memory": "2Gi"},
        )
        self.provider.sync_ippr_status_from_pods([pod])
        assert self.provider.get_ippr_statuses() != {}
        assert self.provider._container_resources != {}

        rc = _make_ray_cluster_with_ippr({})
        self.provider.validate_and_set_ippr_specs(rc)
        assert self.provider.get_ippr_specs().groups == {}

        self.provider.sync_ippr_status_from_pods([pod])
        assert self.provider.get_ippr_statuses() == {}
        assert self.provider._container_resources == {}

    def test_do_ippr_requests_upsize_limits(self):
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "500m", "memory": "1Gi"},
            status_limits={"cpu": "1500m", "memory": "2Gi"},
            spec_requests={"cpu": "500m", "memory": "1Gi"},
            spec_limits={"cpu": "1500m", "memory": "2Gi"},
        )
        self.provider.sync_ippr_status_from_pods([pod])

        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        st.raylet_id = "abc"
        # Desired upsize: from (cpu:1.5, mem:2Gi) to (cpu:4, mem:8Gi)
        st.queue_resize_request(desired_cpu=4.0, desired_memory=8 * Gi)

        self.provider.do_ippr_requests([st])

        # One resize patch and one annotation patch expected
        patch_ops = self.k8s.get_patches("pods/ray-worker-1/resize")
        # Expect limits updated to desired and requests preserve gap (1.5-0.5 = 1)
        # CPU
        cpu_limits = next(p for p in patch_ops if p["path"].endswith("limits/cpu"))
        cpu_requests = next(p for p in patch_ops if p["path"].endswith("requests/cpu"))
        assert cpu_limits["value"] == 4.0
        assert cpu_requests["value"] == 3.0
        # Memory: gap 2Gi - 1Gi = 1Gi → requests 8Gi - 1Gi = 7Gi
        mem_limits = next(p for p in patch_ops if p["path"].endswith("limits/memory"))
        mem_requests = next(
            p for p in patch_ops if p["path"].endswith("requests/memory")
        )
        assert mem_limits["value"] == 8 * Gi
        assert mem_requests["value"] == 7 * Gi

        ann_payload = self.k8s.get_patches("pods/ray-worker-1")
        ippr_status_json = ann_payload["metadata"]["annotations"]["ray.io/ippr-status"]
        parsed = json.loads(ippr_status_json)
        assert parsed["raylet-id"] == "abc"
        assert isinstance(parsed["resizing-at"], int)

    def test_do_ippr_requests_upsize_requests(self):
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "500m", "memory": "1Gi"},
            status_limits={},
            spec_requests={"cpu": "500m", "memory": "1Gi"},
            spec_limits={},
        )
        self.provider.sync_ippr_status_from_pods([pod])

        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        st.raylet_id = "abc"
        # Desired upsize: from (cpu:0.5, mem:1Gi) to (cpu:4, mem:8Gi)
        st.queue_resize_request(desired_cpu=4.0, desired_memory=8 * Gi)

        self.provider.do_ippr_requests([st])

        # One resize patch and one annotation patch expected
        patch_ops = self.k8s.get_patches("pods/ray-worker-1/resize")
        cpu_limits = next(
            (p for p in patch_ops if p["path"].endswith("limits/cpu")), None
        )
        assert cpu_limits is None
        cpu_requests = next(p for p in patch_ops if p["path"].endswith("requests/cpu"))
        assert cpu_requests["value"] == 4.0
        mem_limits = next(
            (p for p in patch_ops if p["path"].endswith("limits/memory")), None
        )
        assert mem_limits is None
        mem_requests = next(
            p for p in patch_ops if p["path"].endswith("requests/memory")
        )
        assert mem_requests["value"] == 8 * Gi

        ann_payload = self.k8s.get_patches("pods/ray-worker-1")
        ippr_status_json = ann_payload["metadata"]["annotations"]["ray.io/ippr-status"]
        parsed = json.loads(ippr_status_json)
        assert parsed["raylet-id"] == "abc"
        assert isinstance(parsed["resizing-at"], int)

    def test_do_ippr_requests_downsize(self):
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "2", "memory": "4Gi"},
            status_limits={},
            spec_requests={"cpu": "2", "memory": "4Gi"},
            spec_limits={},
        )
        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        st.raylet_id = "0" * 56
        # Downsize: current (2 cores, 4Gi) -> desired (1 core, 2Gi)
        st.queue_resize_request(desired_cpu=1.0, desired_memory=2 * Gi)
        self.gcs.resize_raylet_resource_instances.return_value = {
            "CPU": 1.5,
            "memory": 2.5 * Gi,
        }

        self.provider.do_ippr_requests([st])

        self.gcs.resize_raylet_resource_instances.assert_called_once_with(
            st.raylet_id,
            {"CPU": 1.0, "memory": 2 * Gi},
        )

        patch_ops = self.k8s.get_patches("pods/ray-worker-1/resize")
        cpu_requests = next(p for p in patch_ops if p["path"].endswith("requests/cpu"))
        mem_requests = next(
            p for p in patch_ops if p["path"].endswith("requests/memory")
        )
        assert cpu_requests["value"] == 1.5
        assert mem_requests["value"] == 2.5 * Gi

    def test_sync_ippr_status_pending_deferred_cpu_sets_suggestions(self):
        # Load specs
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "16Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        # CPU: status limits=2, requests=1 → diff=1 core. Remaining = 9 - 6 = 3.
        # suggested_max_cpu = remaining + diff = 4
        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": "2Gi"},
            status_limits={"cpu": "2", "memory": "4Gi"},
            spec_requests={"cpu": "7", "memory": "14Gi"},
            spec_limits={"cpu": "8", "memory": "16Gi"},
            conditions=[
                {
                    "type": "PodResizePending",
                    "status": "True",
                    "reason": "Deferred",
                    "message": (
                        "Node didn't have enough resource: cpu, requested: 7000, "
                        "used: 6000, capacity: 9000"
                    ),
                }
            ],
        )
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"suggested-max-memory": 2 * Gi, "raylet-id": "0" * 56}
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.has_resize_request_to_send()
        assert st.suggested_max_cpu == 4.0
        assert st.suggested_max_memory == 2 * Gi

    def test_sync_ippr_status_pending_deferred_memory_sets_suggestions(self):
        # Load specs
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        # Memory: status limits=8Gi, requests=2Gi → diff=6Gi. Remaining = 10Gi - 4Gi = 6Gi.
        # suggested_max_memory = remaining + diff = 12Gi.
        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": str(2 * Gi)},
            status_limits={"cpu": "2", "memory": str(8 * Gi)},
            spec_requests={"cpu": "7", "memory": str(26 * Gi)},
            spec_limits={"cpu": "8", "memory": str(32 * Gi)},
            conditions=[
                {
                    "type": "PodResizePending",
                    "status": "True",
                    "reason": "Deferred",
                    "message": (
                        f"Node didn't have enough resource: memory, requested: {26 * Gi}, "
                        f"used: {4 * Gi}, capacity: {10 * Gi}"
                    ),
                }
            ],
        )
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"suggested-max-cpu": 1.5, "raylet-id": "0" * 56}
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.has_resize_request_to_send()
        assert st.suggested_max_memory == 12 * Gi
        assert st.suggested_max_cpu == 1.5

    def test_sync_ippr_status_pending_infeasible_cpu_sets_suggestions(self):
        # Load specs
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "16Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        # CPU: status limits=2, requests=1 → diff=1 core. Remaining = 9000m.
        # suggested_max_cpu = 9000/1000 + 1 = 10.
        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1000m", "memory": "2Gi"},
            status_limits={"cpu": "2000m", "memory": "4Gi"},
            spec_requests={"cpu": "7", "memory": "14Gi"},
            spec_limits={"cpu": "8", "memory": "16Gi"},
            conditions=[
                {
                    "type": "PodResizePending",
                    "status": "True",
                    "reason": "Infeasible",
                    "message": (
                        "Node didn't have enough capacity: cpu, requested: 8000, capacity: 9000"
                    ),
                }
            ],
        )
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"suggested-max-memory": 2 * Gi, "raylet-id": "0" * 56}
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.has_resize_request_to_send()
        assert st.suggested_max_cpu == 10.0
        assert st.suggested_max_memory == 2 * Gi

    def test_sync_ippr_status_pending_infeasible_memory_sets_suggestions(self):
        # Load specs
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "64Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        # Memory: status limits=8Gi, requests=2Gi → diff=6Gi. Remaining = capacity (12Gi).
        # suggested_max_memory = 12Gi + 6Gi = 18Gi.
        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": str(2 * Gi)},
            status_limits={"cpu": "2", "memory": str(8 * Gi)},
            spec_requests={"cpu": "7", "memory": str(58 * Gi)},
            spec_limits={"cpu": "8", "memory": str(64 * Gi)},
            conditions=[
                {
                    "type": "PodResizePending",
                    "status": "True",
                    "reason": "Infeasible",
                    "message": (
                        f"Node didn't have enough capacity: memory, requested: {58 * Gi}, capacity: {12 * Gi}"
                    ),
                }
            ],
        )
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"suggested-max-cpu": 2, "raylet-id": "0" * 56}
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.has_resize_request_to_send()
        assert st.suggested_max_memory == 18 * Gi
        assert st.suggested_max_cpu == 2.0

    def test_sync_ippr_status_pending_infeasible_memory_sets_suggestions_with_sidecars(
        self,
    ):
        # Load specs
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "64Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        # Memory: status limits=8Gi, requests=2Gi → diff=6Gi. Remaining = capacity (12Gi).
        # suggested_max_memory = 12Gi + 6Gi = 18Gi.
        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": str(2 * Gi)},
            status_limits={"cpu": "2", "memory": str(8 * Gi)},
            spec_requests={"cpu": "7", "memory": str(58 * Gi)},
            spec_limits={"cpu": "8", "memory": str(64 * Gi)},
            conditions=[
                {
                    "type": "PodResizePending",
                    "status": "True",
                    "reason": "Infeasible",
                    "message": (
                        f"Node didn't have enough capacity: memory, requested: {58 * Gi}, capacity: {12 * Gi}"
                    ),
                }
            ],
        )
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"suggested-max-cpu": 2, "raylet-id": "0" * 56}
        )
        pod["status"]["containerStatuses"].append(
            {
                "name": "sidecar",
                "resources": {
                    "requests": {"memory": str(1 * Gi)},
                },
            }
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.has_resize_request_to_send()
        assert st.suggested_max_memory == 18 * Gi - 1 * Gi
        assert st.suggested_max_cpu == 2.0

    def test_pending_message_unexpected_no_suggestions(self):
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "16Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": "2Gi"},
            status_limits={"cpu": "2", "memory": "4Gi"},
            spec_requests={"cpu": "7", "memory": "14Gi"},
            spec_limits={"cpu": "8", "memory": "16Gi"},
            conditions=[
                {
                    "type": "PodResizePending",
                    "status": "True",
                    "reason": "Deferred",
                    "message": "some unexpected format",
                }
            ],
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.suggested_max_cpu is None
        assert st.suggested_max_memory is None

    def test_block_errored_ippr(self):
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "16Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": "2Gi"},
            status_limits={"cpu": "2", "memory": "4Gi"},
            spec_requests={"cpu": "7", "memory": "14Gi"},
            spec_limits={"cpu": "8", "memory": "16Gi"},
            conditions=[
                {
                    "type": "PodResizeInProgress",
                    "status": "True",
                    "reason": "Error",
                    "message": "random error",
                }
            ],
        )
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"raylet-id": "0" * 56}
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.has_resize_request_to_send()
        assert not st.can_resize_up()
        assert st.desired_cpu == 2.0
        assert st.desired_memory == 4 * Gi
        assert st.last_failed_at is not None
        assert st.last_failed_reason == "random error"

    def test_block_timeout_ippr(self):
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "16Gi", "resize-timeout": 10}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": "2Gi"},
            status_limits={"cpu": "2", "memory": "4Gi"},
            spec_requests={"cpu": "7", "memory": "14Gi"},
            spec_limits={"cpu": "8", "memory": "16Gi"},
            conditions=[],
        )
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"raylet-id": "0" * 56, "resizing-at": time.time() - 20}
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.has_resize_request_to_send()
        assert not st.can_resize_up()
        assert st.desired_cpu == 2.0
        assert st.desired_memory == 4 * Gi
        assert st.last_failed_at is not None
        assert st.last_failed_reason == "Pod resize timed out"

    def test_do_ippr_requests_revert_failed_ippr(self):
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "16Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": "2Gi"},
            status_limits={"cpu": "2", "memory": "4Gi"},
            spec_requests={"cpu": "7", "memory": "14Gi"},
            spec_limits={"cpu": "8", "memory": "16Gi"},
            conditions=[
                {
                    "type": "PodResizeInProgress",
                    "status": "True",
                    "reason": "Error",
                    "message": "random error",
                }
            ],
        )
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"raylet-id": "0" * 56}
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        self.provider.do_ippr_requests([st])

        patch_ops = self.k8s.get_patches("pods/ray-worker-1/resize")
        cpu_limits = next(p for p in patch_ops if p["path"].endswith("limits/cpu"))
        cpu_requests = next(p for p in patch_ops if p["path"].endswith("requests/cpu"))
        mem_limits = next(p for p in patch_ops if p["path"].endswith("limits/memory"))
        mem_requests = next(
            p for p in patch_ops if p["path"].endswith("requests/memory")
        )
        assert cpu_limits["value"] == 2.0
        assert cpu_requests["value"] == 1.0
        assert mem_limits["value"] == 4 * Gi
        assert mem_requests["value"] == 2 * Gi

        ann_payload = self.k8s.get_patches("pods/ray-worker-1")
        ippr_status_json = ann_payload["metadata"]["annotations"]["ray.io/ippr-status"]
        parsed = json.loads(ippr_status_json)
        assert parsed["last-failed-reason"] == "random error"
        assert parsed["last-failed-at"] == st.last_failed_at

    def test_existing_last_failed_annotation_blocks_future_ippr(self):
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "16Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": "2Gi"},
            status_limits={"cpu": "2", "memory": "4Gi"},
            spec_requests={"cpu": "2", "memory": "4Gi"},
            spec_limits={"cpu": "2", "memory": "4Gi"},
            conditions=[],
        )
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {
                "raylet-id": "0" * 56,
                "last-failed-at": 123,
                "last-failed-reason": "random error",
            }
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.last_failed_at == 123
        assert st.last_failed_reason == "random error"
        assert not st.has_resize_request_to_send()
        assert not st.can_resize_up()

        with pytest.raises(KeyError):
            _ = self.k8s.get_patches("pods/ray-worker-1")

    def test_do_ippr_requests_downsize_error_skips_patch(self):
        # Setup specs and pod
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)
        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "2", "memory": "4Gi"},
            status_limits={"cpu": "2", "memory": "4Gi"},
            spec_requests={"cpu": "2", "memory": "4Gi"},
            spec_limits={"cpu": "2", "memory": "4Gi"},
        )
        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        st.raylet_id = "0" * 56
        st.queue_resize_request(desired_cpu=1.0, desired_memory=2 * Gi)
        self.gcs.resize_raylet_resource_instances.side_effect = RuntimeError("rpc fail")

        self.provider.do_ippr_requests([st])

        # Should skip issuing K8s resize patch due to failure
        with pytest.raises(KeyError):
            _ = self.k8s.get_patches("pods/ray-worker-1/resize")

    def test_do_ippr_requests_memory_limit(self):
        # Setup specs and pod with memory limits present; k8s 1.35 supports downsize memory limit.
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)
        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "1", "memory": "8Gi"},
            status_limits={"cpu": "2", "memory": "8Gi"},
            spec_requests={"cpu": "1", "memory": "8Gi"},
            spec_limits={"cpu": "2", "memory": "8Gi"},
        )
        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        st.raylet_id = "0" * 56
        # Desired memory below spec limit (8Gi)
        st.queue_resize_request(desired_cpu=2.0, desired_memory=2 * Gi)
        self.gcs.resize_raylet_resource_instances.return_value = {
            "CPU": 2.0,
            "memory": 2 * Gi,
        }

        self.provider.do_ippr_requests([st])

        patch_ops = self.k8s.get_patches("pods/ray-worker-1/resize")
        mem_limits = next(p for p in patch_ops if p["path"].endswith("limits/memory"))
        # Limit must drop below spec limit (8Gi)
        assert mem_limits["value"] == 2 * Gi
        self.gcs.resize_raylet_resource_instances.assert_called_once_with(
            st.raylet_id,
            {"CPU": 2.0, "memory": 2 * Gi},
        )

    def test_do_ippr_requests_mixed_cpu_up_memory_down_raylet_payload(self):
        """CPU upsize + memory down: raylet pre-update must not advertise extra CPU."""
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)
        pod = _make_pod(
            name="ray-worker-1",
            group="small-group",
            kind=KUBERAY_KIND_WORKER,
            container_name="ray-worker",
            status_requests={"cpu": "2", "memory": "8Gi"},
            status_limits={},
            spec_requests={"cpu": "2", "memory": "8Gi"},
            spec_limits={},
        )
        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        st.raylet_id = "0" * 56
        st.queue_resize_request(desired_cpu=4.0, desired_memory=2 * Gi)
        self.gcs.resize_raylet_resource_instances.return_value = {
            "CPU": 2.0,
            "memory": 2 * Gi,
        }

        self.provider.do_ippr_requests([st])

        self.gcs.resize_raylet_resource_instances.assert_called_once_with(
            st.raylet_id,
            {"CPU": 2.0, "memory": 2 * Gi},
        )
        patch_ops = self.k8s.get_patches("pods/ray-worker-1/resize")
        cpu_requests = next(p for p in patch_ops if p["path"].endswith("requests/cpu"))
        mem_requests = next(
            p for p in patch_ops if p["path"].endswith("requests/memory")
        )
        assert cpu_requests["value"] == 4.0
        assert mem_requests["value"] == 2 * Gi


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
