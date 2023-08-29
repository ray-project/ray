import copy
import os
import json
import shutil
import tempfile
import time
from typing import Optional
import unittest
from dataclasses import asdict
from datetime import datetime
from time import sleep
from unittest import mock

import pytest
from ray.autoscaler.v2.schema import (
    ClusterStatus,
    NodeInfo,
    NodeUsage,
    ResourceDemandSummary,
)
from ray.autoscaler.v2.sdk import ResourceDemandSchedulerV2
import yaml

import ray
import ray._private.ray_constants
from ray._private.gcs_utils import PlacementGroupTableData
from ray._private.test_utils import same_elements
from ray.autoscaler._private.node_provider_availability_tracker import (
    NodeAvailabilityRecord,
    NodeAvailabilitySummary,
    UnavailableNodeInformation,
)
from ray.autoscaler._private.autoscaler import AutoscalerSummary
from ray.autoscaler._private.commands import get_or_create_head_node
from ray.autoscaler._private.constants import (
    AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE,
    AUTOSCALER_UTILIZATION_SCORER_KEY,
)
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.providers import _NODE_PROVIDERS, _clear_provider_cache
from ray.autoscaler._private.resource_demand_scheduler import (
    ResourceDemandScheduler,
    _add_min_workers_nodes,
    _resource_based_utilization_scorer,
    _default_utilization_scorer,
    get_bin_pack_residual,
)
from ray.autoscaler._private.resource_demand_scheduler import get_nodes_for as _get
from ray.autoscaler._private.util import (
    LoadMetricsSummary,
    format_info_string,
    is_placement_group_resource,
)
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_UNINITIALIZED,
    STATUS_UP_TO_DATE,
    STATUS_WAITING_FOR_SSH,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.core.generated.common_pb2 import Bundle, PlacementStrategy
from ray.tests.test_autoscaler import (
    MULTI_WORKER_CLUSTER,
    TYPES_A,
    MockAutoscaler,
    MockNodeInfoStub,
    MockProcessRunner,
    MockProvider,
    fill_in_raylet_ids,
    mock_raylet_id,
)
from functools import partial

EMPTY_AVAILABILITY_SUMMARY = NodeAvailabilitySummary({})


def make_node_info(
    ray_node_type_name: str,
    node_status: str,
    resource_usage: NodeUsage,
    instance_id: str = "fake-instance-id",
    instance_type_name: str = "fake-instance-type",
    ip_address: str = "999.999.999.999",
    node_id: str = "0000001",
    failure_detail: Optional[str] = None,
    details: Optional[str] = None,
) -> NodeInfo:

    return NodeInfo(
        ray_node_type_name=ray_node_type_name,
        node_status=node_status,
        resource_usage=resource_usage,
        instance_id=instance_id,
        instance_type_name=instance_type_name,
        ip_address=ip_address,
        node_id=node_id,
        failure_detail=failure_detail,
        details=details,
    )


def test_get_nodes_to_launch_with_min_workers():
    new_types = copy.deepcopy(TYPES_A)
    new_types["p2.8xlarge"]["min_workers"] = 2
    scheduler = ResourceDemandSchedulerV2(
        new_types,
        3,
        head_node_type="p2.8xlarge",
        upscaling_speed=1,
    )

    head_node = make_node_info(
        ray_node_type_name="p2.8xlarge",
        node_status="RUNNING",
        resource_usage=NodeUsage.from_total_resources(
            new_types["p2.8xlarge"]["resources"]
        ),
    )
    resource_demands = ResourceDemandSummary()
    resource_demands.with_task_actor_demand([{"GPU": 8}])
    cluster_status = ClusterStatus(
        resource_demands=resource_demands,
        healthy_nodes=[head_node],
    )

    to_launch, rem = scheduler.get_nodes_to_launch(
        pending_instances=[],
        cluster_status=cluster_status,
        node_availability_summary=EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch == {"p2.8xlarge": 2}
    assert not rem

    resource_demands = ResourceDemandSummary()
    resource_demands.with_task_actor_demand([{"GPU": 8}] * 6)
    cluster_status = ClusterStatus(
        resource_demands=resource_demands,
        healthy_nodes=[head_node],
    )

    to_launch, rem = scheduler.get_nodes_to_launch(
        pending_instances=[],
        cluster_status=cluster_status,
        node_availability_summary=EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch == {"p2.8xlarge": 3}
    assert rem == [{"GPU": 8}, {"GPU": 8}]


def test_get_nodes_to_launch_with_min_workers_and_bin_packing():
    new_types = copy.deepcopy(TYPES_A)
    new_types["p2.8xlarge"]["min_workers"] = 2
    scheduler = ResourceDemandScheduler(
        new_types,
        10,
        head_node_type="p2.8xlarge",
        upscaling_speed=1,
    )

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    # 1 free p2.8xls
    utilizations = {ip: {"GPU": 8} for ip in ips}
    # 1 more on the way
    pending_nodes = {"p2.8xlarge": 1}
    # requires 3 p2.8xls (only 2 are in cluster/pending) and 1 p2.xlarge
    demands = [{"GPU": 8}] * (len(utilizations) + 1) + [{"GPU": 1}]
    to_launch, rem = scheduler.get_nodes_to_launch(
        nodes,
        pending_nodes,
        demands,
        utilizations,
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch == {"p2.xlarge": 1}
    assert not rem

    # 3 min_workers + 1 head of p2.8xlarge covers the 3 p2.8xlarge + 1
    # p2.xlarge demand. 3 p2.8xlarge are running/pending. So we need 1 more
    # p2.8xlarge only tomeet the min_workers constraint and the demand.
    new_types["p2.8xlarge"]["min_workers"] = 3
    scheduler = ResourceDemandScheduler(
        provider,
        new_types,
        10,
        head_node_type="p2.8xlarge",
        upscaling_speed=1,
    )
    to_launch, rem = scheduler.get_nodes_to_launch(
        nodes,
        pending_nodes,
        demands,
        utilizations,
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    # Make sure it does not return [("p2.8xlarge", 1), ("p2.xlarge", 1)]
    assert to_launch == {"p2.8xlarge": 1}
    assert not rem


def test_get_nodes_to_launch_limits():
    provider = MockProvider()
    scheduler = ResourceDemandScheduler(
        provider,
        TYPES_A,
        3,
        head_node_type="p2.8xlarge",
        upscaling_speed=1,
    )

    provider.create_node(
        {},
        {TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"},
        2,
    )

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    utilizations = {ip: {"GPU": 8} for ip in ips}

    to_launch, rem = scheduler.get_nodes_to_launch(
        nodes,
        {"p2.8xlarge": 1},
        [{"GPU": 8}] * 2,
        utilizations,
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch == {}
    assert not rem

    to_launch, rem = scheduler.get_nodes_to_launch(
        nodes,
        {"p2.8xlarge": 1},
        [{"GPU": 8}] * 20,
        utilizations,
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch == {"p2.8xlarge": 1}
    assert rem == [{"GPU": 8}] * 16


def test_calculate_node_resources():
    provider = MockProvider()
    scheduler = ResourceDemandScheduler(
        provider,
        TYPES_A,
        10,
        head_node_type="p2.8xlarge",
        upscaling_speed=1,
    )

    provider.create_node(
        {},
        {TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE, TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"},
        2,
    )

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    # 2 free p2.8xls
    utilizations = {ip: {"GPU": 8} for ip in ips}
    # 1 more on the way
    pending_nodes = {"p2.8xlarge": 1}
    # requires 4 p2.8xls (only 3 are in cluster/pending)
    demands = [{"GPU": 8}] * (len(utilizations) + 2)
    to_launch, rem = scheduler.get_nodes_to_launch(
        nodes,
        pending_nodes,
        demands,
        utilizations,
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )

    assert to_launch == {"p2.8xlarge": 1}
    assert not rem


def test_request_resources_gpu_no_gpu_nodes():
    provider = MockProvider()
    TYPES = {
        "m5.8xlarge": {
            "node_config": {},
            "resources": {"CPU": 32},
            "max_workers": 40,
        },
    }
    scheduler = ResourceDemandScheduler(
        provider,
        TYPES,
        max_workers=100,
        head_node_type="empty_node",
        upscaling_speed=1,
    )

    # Head node
    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "m5.8xlarge",
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
        },
        1,
    )
    all_nodes = provider.non_terminated_nodes({})
    node_ips = provider.non_terminated_node_ips({})
    assert len(node_ips) == 1, node_ips

    # Fully utilized, no requests.
    avail_by_ip = {ip: {} for ip in node_ips}
    max_by_ip = {ip: {"CPU": 32} for ip in node_ips}
    # There aren't any nodes that can satisfy this demand, but we still shouldn't crash.
    demands = [{"CPU": 1, "GPU": 1}] * 1
    to_launch, rem = scheduler.get_nodes_to_launch(
        all_nodes,
        {},
        [],
        avail_by_ip,
        [],
        max_by_ip,
        demands,
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert len(to_launch) == 0, to_launch
    assert not rem

    demands = [{"CPU": 1, "GPU": 0}] * 33
    to_launch, rem = scheduler.get_nodes_to_launch(
        all_nodes,
        {},
        [],
        avail_by_ip,
        [],
        max_by_ip,
        demands,
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert len(to_launch) == 1, to_launch
    assert not rem


def test_request_resources_existing_usage():
    provider = MockProvider()
    TYPES = {
        "p2.8xlarge": {
            "node_config": {},
            "resources": {"CPU": 32, "GPU": 8},
            "max_workers": 40,
        },
    }
    scheduler = ResourceDemandScheduler(
        provider,
        TYPES,
        max_workers=100,
        head_node_type="empty_node",
        upscaling_speed=1,
    )

    # 5 nodes with 32 CPU and 8 GPU each
    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
        },
        2,
    )
    all_nodes = provider.non_terminated_nodes({})
    node_ips = provider.non_terminated_node_ips({})
    assert len(node_ips) == 2, node_ips

    # Fully utilized, no requests.
    avail_by_ip = {ip: {} for ip in node_ips}
    max_by_ip = {ip: {"GPU": 8, "CPU": 32} for ip in node_ips}
    demands = []
    to_launch, rem = scheduler.get_nodes_to_launch(
        all_nodes,
        {},
        [],
        avail_by_ip,
        [],
        max_by_ip,
        demands,
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert len(to_launch) == 0, to_launch
    assert not rem

    # Fully utilized, resource requests exactly equal.
    avail_by_ip = {ip: {} for ip in node_ips}
    demands = [{"GPU": 4}] * 4
    to_launch, rem = scheduler.get_nodes_to_launch(
        all_nodes,
        {},
        [],
        avail_by_ip,
        [],
        max_by_ip,
        demands,
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert len(to_launch) == 0, to_launch
    assert not rem

    # Fully utilized, resource requests in excess.
    avail_by_ip = {ip: {} for ip in node_ips}
    demands = [{"GPU": 4}] * 7
    to_launch, rem = scheduler.get_nodes_to_launch(
        all_nodes,
        {},
        [],
        avail_by_ip,
        [],
        max_by_ip,
        demands,
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch.get("p2.8xlarge") == 2, to_launch
    assert not rem

    # Not utilized, no requests.
    avail_by_ip = {ip: {"GPU": 4, "CPU": 32} for ip in node_ips}
    demands = []
    to_launch, rem = scheduler.get_nodes_to_launch(
        all_nodes,
        {},
        [],
        avail_by_ip,
        [],
        max_by_ip,
        demands,
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert len(to_launch) == 0, to_launch
    assert not rem

    # Not utilized, resource requests exactly equal.
    avail_by_ip = {ip: {"GPU": 4, "CPU": 32} for ip in node_ips}
    demands = [{"GPU": 4}] * 4
    to_launch, rem = scheduler.get_nodes_to_launch(
        all_nodes,
        {},
        [],
        avail_by_ip,
        [],
        max_by_ip,
        demands,
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert len(to_launch) == 0, to_launch
    assert not rem

    # Not utilized, resource requests in excess.
    avail_by_ip = {ip: {"GPU": 4, "CPU": 32} for ip in node_ips}
    demands = [{"GPU": 4}] * 7
    to_launch, rem = scheduler.get_nodes_to_launch(
        all_nodes,
        {},
        [],
        avail_by_ip,
        [],
        max_by_ip,
        demands,
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch.get("p2.8xlarge") == 2, to_launch
    assert not rem

    # Not utilized, resource requests hugely in excess.
    avail_by_ip = {ip: {"GPU": 4, "CPU": 32} for ip in node_ips}
    demands = [{"GPU": 4}] * 70
    to_launch, rem = scheduler.get_nodes_to_launch(
        all_nodes,
        {},
        [],
        avail_by_ip,
        [],
        max_by_ip,
        demands,
        EMPTY_AVAILABILITY_SUMMARY,
    )
    # This bypasses the launch rate limit.
    assert to_launch.get("p2.8xlarge") == 33, to_launch
    assert not rem


def test_backlog_queue_impact_on_binpacking_time():
    new_types = copy.deepcopy(TYPES_A)
    new_types["p2.8xlarge"]["max_workers"] = 1000
    new_types["m4.16xlarge"]["max_workers"] = 1000

    def test_backlog_queue_impact_on_binpacking_time_aux(
        num_available_nodes, time_to_assert, demand_request_shape
    ):
        provider = MockProvider()
        scheduler = ResourceDemandScheduler(
            provider,
            new_types,
            max_workers=10000,
            head_node_type="m4.16xlarge",
            upscaling_speed=1,
        )

        provider.create_node(
            {},
            {
                TAG_RAY_USER_NODE_TYPE: "m4.16xlarge",
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            },
            num_available_nodes,
        )
        # <num_available_nodes> m4.16xlarge instances.
        cpu_ips = provider.non_terminated_node_ips({})
        provider.create_node(
            {},
            {
                TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            },
            num_available_nodes,
        )
        # <num_available_nodes>  m4.16xlarge and <num_available_nodes>
        # p2.8xlarge instances.
        all_nodes = provider.non_terminated_nodes({})
        all_ips = provider.non_terminated_node_ips({})
        gpu_ips = [ip for ip in all_ips if ip not in cpu_ips]
        usage_by_ip = {}
        # 2x<num_available_nodes> free nodes (<num_available_nodes> m4.16xlarge
        # and <num_available_nodes> p2.8xlarge instances).
        for i in range(num_available_nodes):
            usage_by_ip[cpu_ips[i]] = {"CPU": 64}
            usage_by_ip[gpu_ips[i]] = {"GPU": 8, "CPU": 32}
        demands = demand_request_shape * AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE
        t1 = time.time()
        to_launch, rem = scheduler.get_nodes_to_launch(
            all_nodes,
            {},
            demands,
            usage_by_ip,
            [],
            {},
            [],
            EMPTY_AVAILABILITY_SUMMARY,
        )
        t2 = time.time()
        assert t2 - t1 < time_to_assert
        print(
            "The time took to launch",
            to_launch,
            "with number of available nodes set to",
            num_available_nodes,
            "is:",
            t2 - t1,
        )
        return to_launch

    # The assertions below use 10s but the actual time took when this test was
    # measured on 2.3 GHz 8-Core Intel (I9-9880H) Core i9 is commented inline.

    # Check the time it takes when there are 0 nodes available and the demand
    # is requires adding another ~100 nodes.
    to_launch = test_backlog_queue_impact_on_binpacking_time_aux(
        num_available_nodes=0,
        time_to_assert=10,  # real time 0.2s.
        demand_request_shape=[{"GPU": 1}, {"CPU": 1}],
    )
    # If not for the max launch concurrency the next assert should be:
    # {'m4.16xlarge': 1, 'p2.8xlarge': 125, 'p2.xlarge': 1}
    assert to_launch == {"m4.16xlarge": 1, "p2.8xlarge": 5, "p2.xlarge": 1}

    # Check the time it takes when there are 100 nodes available and the demand
    # requires another 75 nodes.
    to_launch = test_backlog_queue_impact_on_binpacking_time_aux(
        num_available_nodes=50,
        time_to_assert=10,  # real time 0.075s.
        demand_request_shape=[{"GPU": 1}, {"CPU": 2}],
    )
    # If not for the max launch concurrency the next assert should be:
    # {'p2.8xlarge': 75}.
    assert to_launch == {"p2.8xlarge": 50}

    # Check the time it takes when there are 250 nodes available and can
    # cover the demand.
    to_launch = test_backlog_queue_impact_on_binpacking_time_aux(
        num_available_nodes=125,
        time_to_assert=10,  # real time 0.06s.
        demand_request_shape=[{"GPU": 1}, {"CPU": 1}],
    )
    assert to_launch == {}

    # Check the time it takes when there are 1000 nodes available and the
    # demand requires another 1000 nodes.
    to_launch = test_backlog_queue_impact_on_binpacking_time_aux(
        num_available_nodes=500,
        time_to_assert=10,  # real time 1.32s.
        demand_request_shape=[{"GPU": 8}, {"CPU": 64}],
    )
    assert to_launch == {"m4.16xlarge": 500, "p2.8xlarge": 500}


class TestPlacementGroupScaling:
    def test_strategies(self):
        provider = MockProvider()
        scheduler = ResourceDemandScheduler(
            provider,
            TYPES_A,
            10,
            head_node_type="p2.8xlarge",
            upscaling_speed=1,
        )

        provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 2)
        # At this point our cluster has 2 p2.8xlarge instances (16 GPUs) and is
        # fully idle.
        nodes = provider.non_terminated_nodes({})

        resource_demands = [{"GPU": 4}] * 2
        pending_placement_groups = [
            # Requires a new node (only uses 2 GPUs on it though).
            PlacementGroupTableData(
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.STRICT_SPREAD,
                bundles=[
                    Bundle(unit_resources={"GPU": 2}),
                    Bundle(unit_resources={"GPU": 2}),
                    Bundle(unit_resources={"GPU": 2}),
                ],
            ),
            # Requires a new node (uses the whole node).
            PlacementGroupTableData(
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.STRICT_PACK,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 4),
            ),
            # Fits across the machines that strict spread.
            PlacementGroupTableData(
                # runs on.
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.PACK,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 2),
            ),
            # Fits across the machines that strict spread.
            PlacementGroupTableData(
                # runs on.
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.SPREAD,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 2),
            ),
        ]
        to_launch, rem = scheduler.get_nodes_to_launch(
            nodes,
            {},
            resource_demands,
            {},
            pending_placement_groups,
            {},
            [],
            EMPTY_AVAILABILITY_SUMMARY,
        )
        assert to_launch == {"p2.8xlarge": 2}
        assert not rem

    def test_many_strict_spreads(self):
        provider = MockProvider()
        scheduler = ResourceDemandScheduler(
            provider,
            TYPES_A,
            10,
            head_node_type="p2.8xlarge",
            upscaling_speed=1,
        )

        provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 2)
        # At this point our cluster has 2 p2.8xlarge instances (16 GPUs) and is
        # fully idle.
        nodes = provider.non_terminated_nodes({})

        resource_demands = [{"GPU": 1}] * 6
        pending_placement_groups = [
            # Requires a new node (only uses 2 GPUs on it though).
            PlacementGroupTableData(
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.STRICT_SPREAD,
                bundles=[Bundle(unit_resources={"GPU": 2})] * 3,
            ),
        ]
        # Each placement group will take up 2 GPUs per node, but the distinct
        # placement groups should still reuse the same nodes.
        pending_placement_groups = pending_placement_groups * 3
        to_launch, rem = scheduler.get_nodes_to_launch(
            nodes,
            {},
            resource_demands,
            {},
            pending_placement_groups,
            {},
            [],
            EMPTY_AVAILABILITY_SUMMARY,
        )
        assert to_launch == {"p2.8xlarge": 1}
        assert not rem

    def test_packing(self):
        provider = MockProvider()
        scheduler = ResourceDemandScheduler(
            provider,
            TYPES_A,
            10,
            head_node_type="p2.8xlarge",
            upscaling_speed=1,
        )

        provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 1)
        # At this point our cluster has 1 p2.8xlarge instances (8 GPUs) and is
        # fully idle.
        nodes = provider.non_terminated_nodes({})

        resource_demands = [{"GPU": 1}] * 2
        pending_placement_groups = [
            PlacementGroupTableData(
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.STRICT_PACK,
                bundles=[Bundle(unit_resources={"GPU": 2})] * 3,
            ),
        ]
        # The 2 resource demand gpus should still be packed onto the same node
        # as the 6 GPU placement group.
        to_launch, rem = scheduler.get_nodes_to_launch(
            nodes,
            {},
            resource_demands,
            {},
            pending_placement_groups,
            {},
            [],
            EMPTY_AVAILABILITY_SUMMARY,
        )
        assert to_launch == {}
        assert not rem


def test_get_concurrent_resource_demand_to_launch():
    node_types = copy.deepcopy(TYPES_A)
    node_types["p2.8xlarge"]["min_workers"] = 1
    node_types["p2.8xlarge"]["max_workers"] = 10
    node_types["m4.large"]["min_workers"] = 2
    node_types["m4.large"]["max_workers"] = 100
    provider = MockProvider()
    scheduler = ResourceDemandScheduler(
        provider,
        node_types,
        200,
        head_node_type="empty_node",
        upscaling_speed=1,
    )
    # Sanity check.
    assert len(provider.non_terminated_nodes({})) == 0

    # Sanity check.
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        {}, [], [], {}, {}, {}
    )
    assert updated_to_launch == {}

    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        },
        1,
    )
    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "m4.large",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        },
        2,
    )

    # All nodes so far are pending/launching here.
    to_launch = {"p2.8xlarge": 4, "m4.large": 40}
    non_terminated_nodes = provider.non_terminated_nodes({})
    pending_launches_nodes = {"p2.8xlarge": 1, "m4.large": 1}
    connected_nodes = []  # All the non_terminated_nodes are not connected yet.
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes, pending_launches_nodes, {}, {}
    )
    # Note: we have 2 pending/launching gpus, 3 pending/launching cpus,
    # 0 running gpu, and 0 running cpus.
    assert updated_to_launch == {"p2.8xlarge": 3, "m4.large": 2}

    # Test min_workers bypass max launch limit.
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch,
        connected_nodes,
        non_terminated_nodes,
        pending_launches_nodes,
        adjusted_min_workers={"m4.large": 40},
        placement_group_nodes={},
    )
    assert updated_to_launch == {"p2.8xlarge": 3, "m4.large": 40}
    # Test placement groups bypass max launch limit.
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch,
        connected_nodes,
        non_terminated_nodes,
        pending_launches_nodes,
        {},
        placement_group_nodes={"m4.large": 40},
    )
    assert updated_to_launch == {"p2.8xlarge": 3, "m4.large": 40}
    # Test combining min_workers and placement groups bypass max launch limit.
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch,
        connected_nodes,
        non_terminated_nodes,
        pending_launches_nodes,
        adjusted_min_workers={"m4.large": 25},
        placement_group_nodes={"m4.large": 15},
    )
    assert updated_to_launch == {"p2.8xlarge": 3, "m4.large": 40}

    # This starts the min workers only, so we have no more pending workers.
    # The workers here are either running (connected) or in
    # pending_launches_nodes (i.e., launching).
    connected_nodes = [
        provider.internal_ip(node_id) for node_id in non_terminated_nodes
    ]
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes, pending_launches_nodes, {}, {}
    )
    # Note that here we have 1 launching gpu, 1 launching cpu,
    # 1 running gpu, and 2 running cpus.
    assert updated_to_launch == {"p2.8xlarge": 4, "m4.large": 4}

    # Launch the nodes. Note, after create_node the node is pending.
    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        },
        5,
    )
    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "m4.large",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        },
        5,
    )

    # Continue scaling.
    non_terminated_nodes = provider.non_terminated_nodes({})
    to_launch = {"m4.large": 36}  # No more gpus are necessary
    pending_launches_nodes = {}  # No pending launches
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes, pending_launches_nodes, {}, {}
    )
    # Note: we have 5 pending cpus. So we are not allowed to start any.
    # Still only 2 running cpus.
    assert updated_to_launch == {}

    # All the non_terminated_nodes are connected here.
    connected_nodes = [
        provider.internal_ip(node_id) for node_id in non_terminated_nodes
    ]
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes, pending_launches_nodes, {}, {}
    )
    # Note: that here we have 7 running cpus and nothing pending/launching.
    assert updated_to_launch == {"m4.large": 7}

    # Launch the nodes. Note, after create_node the node is pending.
    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "m4.large",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        },
        7,
    )

    # Continue scaling.
    non_terminated_nodes = provider.non_terminated_nodes({})
    to_launch = {"m4.large": 29}
    pending_launches_nodes = {"m4.large": 1}
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes, pending_launches_nodes, {}, {}
    )
    # Note: we have 8 pending/launching cpus and only 7 running.
    # So we should not launch anything (8 < 7).
    assert updated_to_launch == {}

    # All the non_terminated_nodes are connected here.
    connected_nodes = [
        provider.internal_ip(node_id) for node_id in non_terminated_nodes
    ]
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes, pending_launches_nodes, {}, {}
    )
    # Note: that here we have 14 running cpus and 1 launching.
    assert updated_to_launch == {"m4.large": 13}


def test_get_concurrent_resource_demand_to_launch_with_upscaling_speed():
    node_types = copy.deepcopy(TYPES_A)
    node_types["p2.8xlarge"]["min_workers"] = 1
    node_types["p2.8xlarge"]["max_workers"] = 10
    node_types["m4.large"]["min_workers"] = 2
    node_types["m4.large"]["max_workers"] = 100

    def create_provider():
        provider = MockProvider()
        provider.create_node(
            {},
            {
                TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
            },
            0,
        )
        provider.create_node(
            {},
            {
                TAG_RAY_USER_NODE_TYPE: "m4.large",
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
            },
            0,
        )
        return provider

    # Test default behaviour limits to 5 inital nodes
    slow_scheduler = ResourceDemandScheduler(
        create_provider(),
        node_types,
        200,
        head_node_type="empty_node",
        upscaling_speed=1,
    )

    to_launch = slow_scheduler._get_concurrent_resource_demand_to_launch(
        {"m4.large": 50},
        [],
        slow_scheduler.provider.non_terminated_nodes({}),
        {},
        {},
        {},
    )
    assert to_launch == {"m4.large": 5}

    # Test upscaling_speed is respected
    mid_scheduler = ResourceDemandScheduler(
        create_provider(),
        node_types,
        200,
        head_node_type="empty_node",
        upscaling_speed=25,
    )

    to_launch = mid_scheduler._get_concurrent_resource_demand_to_launch(
        {"m4.large": 50},
        [],
        mid_scheduler.provider.non_terminated_nodes({}),
        {},
        {},
        {},
    )
    assert to_launch == {"m4.large": 25}

    # Test high upscaling_speed
    fast_scheduler = ResourceDemandScheduler(
        create_provider(),
        node_types,
        200,
        head_node_type="empty_node",
        upscaling_speed=9999,
    )

    to_launch = fast_scheduler._get_concurrent_resource_demand_to_launch(
        {"m4.large": 50},
        [],
        fast_scheduler.provider.non_terminated_nodes({}),
        {},
        {},
        {},
    )
    assert to_launch == {"m4.large": 50}


def test_get_nodes_to_launch_max_launch_concurrency_placement_groups():
    provider = MockProvider()
    new_types = copy.deepcopy(TYPES_A)
    new_types["p2.8xlarge"]["min_workers"] = 10
    new_types["p2.8xlarge"]["max_workers"] = 40

    scheduler = ResourceDemandScheduler(
        provider,
        new_types,
        50,
        head_node_type=None,
        upscaling_speed=1,
    )

    pending_placement_groups = [
        PlacementGroupTableData(
            state=PlacementGroupTableData.RESCHEDULING,
            strategy=PlacementStrategy.PACK,
            bundles=([Bundle(unit_resources={"GPU": 8})] * 25),
        )
    ]
    # placement groups should bypass max launch limit.
    # Note that 25 = max(placement group resources=25, min_workers=10).
    to_launch, rem = scheduler.get_nodes_to_launch(
        [],
        {},
        [],
        {},
        pending_placement_groups,
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch == {"p2.8xlarge": 25}
    assert not rem

    pending_placement_groups = [
        # Requires 25 p2.8xlarge nodes.
        PlacementGroupTableData(
            state=PlacementGroupTableData.RESCHEDULING,
            strategy=PlacementStrategy.STRICT_SPREAD,
            bundles=([Bundle(unit_resources={"GPU": 2})] * 25),
        ),
        # Requires 5 additional nodes (total 30).
        PlacementGroupTableData(
            state=PlacementGroupTableData.RESCHEDULING,
            strategy=PlacementStrategy.PACK,
            bundles=([Bundle(unit_resources={"GPU": 6})] * 30),
        ),
    ]

    to_launch, rem = scheduler.get_nodes_to_launch(
        [],
        {},
        [],
        {},
        pending_placement_groups,
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    # Test that combining spreads and normal placement group demands bypasses
    # launch limit.
    assert to_launch == {"p2.8xlarge": 30}
    assert not rem

    pending_placement_groups = [
        # Requires 25 p2.8xlarge nodes.
        PlacementGroupTableData(
            state=PlacementGroupTableData.RESCHEDULING,
            strategy=PlacementStrategy.STRICT_SPREAD,
            bundles=([Bundle(unit_resources={"GPU": 2})] * 25),
        ),
        # Requires 35 additional nodes (total 60).
        PlacementGroupTableData(
            state=PlacementGroupTableData.RESCHEDULING,
            strategy=PlacementStrategy.PACK,
            bundles=([Bundle(unit_resources={"GPU": 6})] * 60),
        ),
    ]

    to_launch, rem = scheduler.get_nodes_to_launch(
        [],
        {},
        [],
        {},
        pending_placement_groups,
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    # make sure it still respects max_workers of p2.8xlarge.
    assert to_launch == {"p2.8xlarge": 40}
    assert rem == [{"GPU": 6.0}] * 20

    scheduler.node_types["p2.8xlarge"]["max_workers"] = 60
    to_launch, rem = scheduler.get_nodes_to_launch(
        [],
        {},
        [],
        {},
        pending_placement_groups,
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    # make sure it still respects global max_workers constraint.
    # 50 + 1 is global max_workers + head node.ß
    assert to_launch == {"p2.8xlarge": 51}
    assert rem == [{"GPU": 6.0}] * 9


def test_get_nodes_to_launch_max_launch_concurrency():
    provider = MockProvider()
    new_types = copy.deepcopy(TYPES_A)
    new_types["p2.8xlarge"]["min_workers"] = 10
    new_types["p2.8xlarge"]["max_workers"] = 40

    scheduler = ResourceDemandScheduler(
        provider,
        new_types,
        30,
        head_node_type=None,
        upscaling_speed=1,
    )

    to_launch, rem = scheduler.get_nodes_to_launch(
        [],
        {},
        [],
        {},
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    # Respects min_workers despite max launch limit.
    assert to_launch == {"p2.8xlarge": 10}
    assert not rem
    scheduler.node_types["p2.8xlarge"]["min_workers"] = 4
    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
        },
        1,
    )
    nodes = provider.non_terminated_nodes({})
    # Trying to force here that the node shows in nodes but not connected yet
    # and hence does not show up in LoadMetrics (or utilizations).
    ips = provider.non_terminated_node_ips({TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
    utilizations = {ip: {"GPU": 8} for ip in ips}
    launching_nodes = {"p2.8xlarge": 1}
    # requires 41 p2.8xls (currently 1 pending, 1 launching, 0 running}
    demands = [{"GPU": 8}] * (len(utilizations) + 40)
    to_launch, rem = scheduler.get_nodes_to_launch(
        nodes,
        launching_nodes,
        demands,
        utilizations,
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    # Enforces max launch to 5 when < 5 running. 2 are pending/launching.
    assert to_launch == {"p2.8xlarge": 3}
    assert rem == [{"GPU": 8}] * 9

    provider.create_node(
        {},
        {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge", TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE},
        8,
    )
    nodes = provider.non_terminated_nodes({})
    ips = provider.non_terminated_node_ips({TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE})
    utilizations = {ip: {"GPU": 8} for ip in ips}
    launching_nodes = {"p2.8xlarge": 1}
    # Requires additional 17 p2.8xls (now 1 pending, 1 launching, 8 running}
    demands = [{"GPU": 8}] * (len(utilizations) + 15)
    to_launch, rem = scheduler.get_nodes_to_launch(
        nodes,
        launching_nodes,
        demands,
        utilizations,
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    # We are allowed to launch up to 8 more since 8 are running.
    # We already have 2 pending/launching, so only 6 remain.
    assert to_launch == {"p2.8xlarge": 6}
    assert not rem


def test_placement_group_match_string():
    assert (
        is_placement_group_resource("bundle_group_ffe7d420752c6e8658638d19ecf2b68c")
        is True
    )
    assert (
        is_placement_group_resource("CPU_group_0_625ace126f848864c46f50dced5e0ef7")
        is True
    )
    assert (
        is_placement_group_resource("CPU_group_625ace126f848864c46f50dced5e0ef7")
        is True
    )
    assert is_placement_group_resource("CPU") is False
    assert is_placement_group_resource("GPU") is False
    assert is_placement_group_resource("custom_resource") is False
    assert is_placement_group_resource("ip:192.168.1.1") is False

    provider = MockProvider()
    new_types = copy.deepcopy(TYPES_A)
    scheduler = ResourceDemandScheduler(
        provider,
        new_types,
        3,
        head_node_type="p2.8xlarge",
        upscaling_speed=1,
    )

    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
        },
        1,
    )

    nodes = provider.non_terminated_nodes({})
    ips = provider.non_terminated_node_ips({})

    utilizations = {ip: {"GPU": 8} for ip in ips}

    with mock.patch(
        "ray.autoscaler._private.resource_demand_scheduler.logger"
    ) as logger_mock:
        to_launch, rem = scheduler.get_nodes_to_launch(
            nodes,
            {},
            [{"CPU_group_0_625ace126f848864c46f50dced5e0ef7": 8}],
            utilizations,
            [],
            {},
            [],
            EMPTY_AVAILABILITY_SUMMARY,
        )
        logger_mock.warning.assert_not_called()

    assert to_launch == {}
    assert rem == [{"CPU_group_0_625ace126f848864c46f50dced5e0ef7": 8}]

    with mock.patch(
        "ray.autoscaler._private.resource_demand_scheduler.logger"
    ) as logger_mock:
        to_launch, rem = scheduler.get_nodes_to_launch(
            nodes,
            {},
            [{"non-existent-custom": 8}],
            utilizations,
            [],
            {},
            [],
            EMPTY_AVAILABILITY_SUMMARY,
        )
        logger_mock.warning.assert_called()

    assert to_launch == {}
    assert rem == [{"non-existent-custom": 8}]


def _launch_nothing_utilization_scorer_plugin(
    node_resources,  # noqa
    resources,  # noqa
    node_type,  # noqa
    *,
    node_availability_summary,  # noqa
):
    assert node_availability_summary is not None
    return None


@pytest.fixture
def launch_nothing_utilization_score_plugin():
    os.environ[AUTOSCALER_UTILIZATION_SCORER_KEY] = (
        "ray.tests.test_resource_demand_scheduler."
        "_launch_nothing_utilization_scorer_plugin"
    )
    try:
        yield None
    finally:
        del os.environ[AUTOSCALER_UTILIZATION_SCORER_KEY]


def test_utilization_score_plugin_1(launch_nothing_utilization_score_plugin):
    assert launch_nothing_utilization_score_plugin is None, "Keep mypy happy."

    provider = MockProvider()
    new_types = copy.deepcopy(TYPES_A)
    scheduler = ResourceDemandScheduler(
        provider,
        new_types,
        3,
        head_node_type="p2.8xlarge",
        upscaling_speed=1,
    )

    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
        },
        1,
    )

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    utilizations = {ip: {"GPU": 8} for ip in ips}

    to_launch, rem = scheduler.get_nodes_to_launch(
        nodes,
        {},
        [{"GPU": 8}] * 2,
        utilizations,
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch == {}


def _lexical_scorer_plugin(
    node_resources,  # noqa
    resources,  # noqa
    node_type,  # noqa
    *,
    node_availability_summary,  # noqa
):
    assert node_availability_summary is not None
    if (
        _resource_based_utilization_scorer(
            node_resources,
            resources,
            node_availability_summary=node_availability_summary,
        )
        is not None
    ):
        return node_type
    else:
        return None


@pytest.fixture
def lexical_score_plugin():
    os.environ[AUTOSCALER_UTILIZATION_SCORER_KEY] = (
        "ray.tests.test_resource_demand_scheduler." "_lexical_scorer_plugin"
    )
    try:
        yield None
    finally:
        del os.environ[AUTOSCALER_UTILIZATION_SCORER_KEY]


def test_utilization_score_plugin_2(lexical_score_plugin):
    assert lexical_score_plugin is None, "Keep mypy happy."

    provider = MockProvider()
    new_types = copy.deepcopy(TYPES_A)
    new_types["z2.8xlarge"] = new_types["p2.8xlarge"]
    scheduler = ResourceDemandScheduler(
        provider,
        new_types,
        3,
        head_node_type="p2.8xlarge",
        upscaling_speed=1,
    )

    provider.create_node(
        {},
        {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
        },
        1,
    )

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    utilizations = {ip: {"GPU": 8} for ip in ips}

    to_launch, rem = scheduler.get_nodes_to_launch(
        nodes,
        {},
        [{"GPU": 8}] * 2,
        utilizations,
        [],
        {},
        [],
        EMPTY_AVAILABILITY_SUMMARY,
    )
    assert to_launch == {"z2.8xlarge": 1}


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
