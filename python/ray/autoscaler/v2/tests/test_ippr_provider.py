import json
import os
import sys
import time
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from ray._raylet import NodeID
from ray.autoscaler._private.kuberay.node_provider import (
    KUBERAY_KIND_HEAD,
    KUBERAY_KIND_WORKER,
    KUBERAY_LABEL_KEY_KIND,
    KUBERAY_LABEL_KEY_TYPE,
)
from ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.ippr_provider import (
    KubeRayIPPRProvider,
)
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

        # RayCluster without ippr annotation is no-op
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

    def test_sync_ippr_status_from_pods_and_accessors(self):
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
        assert st.cloud_instance_id == "ray-worker-1"
        assert st.spec == self.provider.get_ippr_specs().groups["small-group"]
        assert st.desired_cpu == 1.5
        assert st.desired_memory == 2 * Gi
        assert st.current_cpu == 1.5
        assert st.current_memory == 2 * Gi
        assert st.raylet_id is None
        assert st.resized_at is None
        assert st.resized_status is None
        assert st.resized_message is None
        assert st.suggested_max_cpu is None
        assert st.suggested_max_memory is None

        assert st.is_pod_resized_finished()
        assert st.can_resize_up()
        assert not st.is_in_progress()
        assert not st.has_resize_request_to_send()

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
        # Desired upsize: from (cpu:2, mem:4Gi) to (cpu:4, mem:8Gi)
        st.queue_resize_request(
            raylet_id="abc", desired_cpu=4.0, desired_memory=8 * 1024 * 1024 * 1024
        )

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
        assert mem_limits["value"] == 8 * 1024 * 1024 * 1024
        assert mem_requests["value"] == 7 * 1024 * 1024 * 1024

        ann_payload = self.k8s.get_patches("pods/ray-worker-1")
        ippr_status_json = ann_payload["metadata"]["annotations"]["ray.io/ippr-status"]
        parsed = json.loads(ippr_status_json)
        assert parsed["raylet-id"] == "abc"
        assert isinstance(parsed["resized-at"], int)

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
        # Desired upsize: from (cpu:0.5, mem:1Gi) to (cpu:4, mem:8Gi)
        st.queue_resize_request(
            raylet_id="abc", desired_cpu=4.0, desired_memory=8 * 1024 * 1024 * 1024
        )

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
        assert mem_requests["value"] == 8 * 1024 * 1024 * 1024

        ann_payload = self.k8s.get_patches("pods/ray-worker-1")
        ippr_status_json = ann_payload["metadata"]["annotations"]["ray.io/ippr-status"]
        parsed = json.loads(ippr_status_json)
        assert parsed["raylet-id"] == "abc"
        assert isinstance(parsed["resized-at"], int)

    @patch(
        "ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.ippr_provider.RayletPXIClient"
    )
    def test_do_ippr_requests_downsize(self, mock_raylet_client_cls):
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
        # Downsize: current (2 cores, 4Gi) -> desired (1 core, 2Gi)
        st.queue_resize_request(
            raylet_id="0" * 56, desired_cpu=1.0, desired_memory=2 * 1024 * 1024 * 1024
        )

        class _NodeInfo:
            node_manager_address = "127.0.0.1"
            node_manager_port = 5555

        async def _fake_async_get_all_node_info(node_id):
            assert node_id == NodeID.from_hex(st.raylet_id)
            return {node_id: _NodeInfo()}

        self.gcs.async_get_all_node_info = _fake_async_get_all_node_info
        mock_raylet_client = MagicMock()
        mock_raylet_client.resize_local_resource_instances.return_value = {
            "CPU": 1.5,
            "memory": 2.5 * Gi,
        }
        mock_raylet_client_cls.create.return_value = mock_raylet_client

        self.provider.do_ippr_requests([st])

        assert mock_raylet_client.resize_local_resource_instances.call_count == 1
        sent_req = mock_raylet_client.resize_local_resource_instances.call_args[0][0]
        assert sent_req == {"CPU": 1.0, "memory": 2 * Gi}

        patch_ops = self.k8s.get_patches("pods/ray-worker-1/resize")
        cpu_requests = next(p for p in patch_ops if p["path"].endswith("requests/cpu"))
        mem_requests = next(
            p for p in patch_ops if p["path"].endswith("requests/memory")
        )
        assert cpu_requests["value"] == 1.5
        assert mem_requests["value"] == 2.5 * 1024 * 1024 * 1024

    @patch(
        "ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.ippr_provider.RayletPXIClient"
    )
    def test_sync_with_raylets(self, mock_raylet_client_cls):
        # Pretend a resize finished and current == desired → should sync with raylet
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
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"raylet-id": "0" * 56, "resized-at": 123}
        )

        class _NodeInfo:
            node_manager_address = "127.0.0.1"
            node_manager_port = 5555

        async def _fake_async_get_all_node_info(node_id):
            assert node_id == NodeID.from_hex("0" * 56)
            return {node_id: _NodeInfo()}

        self.gcs.async_get_all_node_info = _fake_async_get_all_node_info
        mock_raylet_client = MagicMock()
        mock_raylet_client_cls.create.return_value = mock_raylet_client
        # Populate provider's statuses from pod
        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.is_pod_resized_finished()
        assert st.need_sync_with_raylet()

        self.provider.sync_with_raylets()

        assert mock_raylet_client.resize_local_resource_instances.call_count == 1
        sent_req = mock_raylet_client.resize_local_resource_instances.call_args[0][0]
        assert sent_req == {"CPU": 2.0, "memory": 4 * Gi}

        ann_payload = self.k8s.get_patches("pods/ray-worker-1")
        assert ann_payload is not None
        parsed = json.loads(
            ann_payload["metadata"]["annotations"]["ray.io/ippr-status"]
        )
        assert parsed["raylet-id"] == "0" * 56
        assert parsed["resized-at"] is None

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

    def test_revert_errored_ippr(self):
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
        assert st.desired_cpu == 2.0
        assert st.desired_memory == 4 * Gi

    def test_revert_timeout_ippr(self):
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
            {"raylet-id": "0" * 56, "resized-at": time.time() - 20}
        )

        self.provider.sync_ippr_status_from_pods([pod])
        st = self.provider.get_ippr_statuses()["ray-worker-1"]
        assert st.has_resize_request_to_send()
        assert st.desired_cpu == 2.0
        assert st.desired_memory == 4 * Gi

    @patch(
        "ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.ippr_provider.RayletPXIClient"
    )
    def test_do_ippr_requests_downsize_error_skips_patch(self, mock_raylet_client_cls):
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
        st.queue_resize_request(
            raylet_id="0" * 56, desired_cpu=1.0, desired_memory=2 * 1024 * 1024 * 1024
        )

        class _NodeInfo:
            node_manager_address = "127.0.0.1"
            node_manager_port = 5555

        async def _fake_async_get_all_node_info(node_id):
            assert node_id == NodeID.from_hex(st.raylet_id)
            return {node_id: _NodeInfo()}

        self.gcs.async_get_all_node_info = _fake_async_get_all_node_info
        mock_raylet_client = MagicMock()
        mock_raylet_client.resize_local_resource_instances.side_effect = RuntimeError(
            "rpc fail"
        )
        mock_raylet_client_cls.create.return_value = mock_raylet_client

        self.provider.do_ippr_requests([st])

        # Should skip issuing K8s resize patch due to failure
        with pytest.raises(KeyError):
            _ = self.k8s.get_patches("pods/ray-worker-1/resize")

    @patch(
        "ray.autoscaler.v2.instance_manager.cloud_providers.kuberay.ippr_provider.RayletPXIClient"
    )
    def test_do_ippr_requests_memory_limit_not_below_spec_limit(
        self, mock_raylet_client_cls
    ):
        # Setup specs and pod with memory limits present; request downsize below spec limit
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
        # Desired memory below spec limit (8Gi)
        st.queue_resize_request(
            raylet_id="0" * 56, desired_cpu=2.0, desired_memory=2 * 1024 * 1024 * 1024
        )

        class _NodeInfo:
            node_manager_address = "127.0.0.1"
            node_manager_port = 5555

        async def _fake_async_get_all_node_info(node_id):
            assert node_id == NodeID.from_hex(st.raylet_id)
            return {node_id: _NodeInfo()}

        self.gcs.async_get_all_node_info = _fake_async_get_all_node_info
        mock_raylet_client = MagicMock()
        mock_raylet_client.resize_local_resource_instances.return_value = {
            "CPU": 2.0,
            "memory": 2 * Gi,
        }
        mock_raylet_client_cls.create.return_value = mock_raylet_client

        self.provider.do_ippr_requests([st])

        patch_ops = self.k8s.get_patches("pods/ray-worker-1/resize")
        mem_limits = next(p for p in patch_ops if p["path"].endswith("limits/memory"))
        # Limit must not drop below spec limit (8Gi)
        assert mem_limits["value"] == 8 * 1024 * 1024 * 1024
        assert mock_raylet_client.resize_local_resource_instances.call_count == 1
        sent_req = mock_raylet_client.resize_local_resource_instances.call_args[0][0]
        assert sent_req == {"CPU": 2.0, "memory": 2 * Gi}

    def test_sync_with_raylets_missing_raylet_address_noop(self):
        # Prepare pod that needs sync (desired == current and resized_at set),
        # but GCS returns no address
        rc = _make_ray_cluster_with_ippr(
            {"small-group": {"max-cpu": 8, "max-memory": "32Gi", "resize-timeout": 60}}
        )
        self.provider.validate_and_set_ippr_specs(rc)

        raylet_hex = "0" * 56
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
        pod["metadata"]["annotations"]["ray.io/ippr-status"] = json.dumps(
            {"raylet-id": raylet_hex, "resized-at": 123}
        )

        async def _fake_async_get_all_node_info(node_id):
            assert node_id == NodeID.from_hex(raylet_hex)
            return {}

        self.gcs.async_get_all_node_info = _fake_async_get_all_node_info
        self.provider.sync_ippr_status_from_pods([pod])
        self.provider.sync_with_raylets()

        # No annotation patch should be issued
        with pytest.raises(KeyError):
            _ = self.k8s.get_patches("pods/ray-worker-1")


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
