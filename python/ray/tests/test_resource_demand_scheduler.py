import pytest
import time
import yaml
import tempfile
import shutil
import unittest
import copy

import ray
from ray.autoscaler._private.util import \
    rewrite_legacy_yaml_to_available_node_types
from ray.tests.test_autoscaler import SMALL_CLUSTER, MockProvider, \
    MockProcessRunner
from ray.autoscaler._private.providers import (_NODE_PROVIDERS,
                                               _clear_provider_cache)
from ray.autoscaler._private.autoscaler import StandardAutoscaler
from ray.autoscaler._private.load_metrics import LoadMetrics
from ray.autoscaler._private.commands import get_or_create_head_node
from ray.autoscaler._private.resource_demand_scheduler import \
    _utilization_score, _add_min_workers_nodes, \
    get_bin_pack_residual, get_nodes_for, ResourceDemandScheduler
from ray.gcs_utils import PlacementGroupTableData
from ray.core.generated.common_pb2 import Bundle, PlacementStrategy
from ray.autoscaler.tags import TAG_RAY_USER_NODE_TYPE, TAG_RAY_NODE_KIND, \
                                NODE_KIND_WORKER, TAG_RAY_NODE_STATUS, \
                                STATUS_UP_TO_DATE, STATUS_UNINITIALIZED, \
                                NODE_KIND_HEAD, NODE_TYPE_LEGACY_WORKER, \
                                NODE_TYPE_LEGACY_HEAD
from ray.test_utils import same_elements
from ray.autoscaler._private.constants import \
    AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE

from time import sleep

TYPES_A = {
    "empty_node": {
        "node_config": {
            "FooProperty": 42,
        },
        "resources": {},
        "max_workers": 0,
    },
    "m4.large": {
        "node_config": {},
        "resources": {
            "CPU": 2
        },
        "max_workers": 10,
    },
    "m4.4xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 16
        },
        "max_workers": 8,
    },
    "m4.16xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 64
        },
        "max_workers": 4,
    },
    "p2.xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 16,
            "GPU": 1
        },
        "max_workers": 10,
    },
    "p2.8xlarge": {
        "node_config": {},
        "resources": {
            "CPU": 32,
            "GPU": 8
        },
        "max_workers": 4,
    },
}

MULTI_WORKER_CLUSTER = dict(
    SMALL_CLUSTER, **{
        "available_node_types": TYPES_A,
        "head_node_type": "empty_node",
        "worker_default_node_type": "m4.large",
    })


def test_util_score():
    assert _utilization_score({"CPU": 64}, [{"TPU": 16}]) is None
    assert _utilization_score({"GPU": 4}, [{"GPU": 2}]) == (0.5, 0.5)
    assert _utilization_score({"GPU": 4}, [{"GPU": 1}, {"GPU": 1}]) == \
        (0.5, 0.5)
    assert _utilization_score({"GPU": 2}, [{"GPU": 2}]) == (2, 2)
    assert _utilization_score({"GPU": 2}, [{"GPU": 1}, {"GPU": 1}]) == (2, 2)
    assert _utilization_score({"GPU": 2, "TPU": 1}, [{"GPU": 2}]) == (0, 1)
    assert _utilization_score({"CPU": 64}, [{"CPU": 64}]) == (64, 64)
    assert _utilization_score({"CPU": 64}, [{"CPU": 32}]) == (8, 8)
    assert _utilization_score({"CPU": 64}, [{"CPU": 16}, {"CPU": 16}]) == \
        (8, 8)


def test_bin_pack():
    assert get_bin_pack_residual([], [{"GPU": 2}, {"GPU": 2}])[0] == \
        [{"GPU": 2}, {"GPU": 2}]
    assert get_bin_pack_residual([{"GPU": 2}], [{"GPU": 2}, {"GPU": 2}])[0] \
        == [{"GPU": 2}]
    assert get_bin_pack_residual([{
        "GPU": 4
    }], [{
        "GPU": 2
    }, {
        "GPU": 2
    }])[0] == []
    arg = [{"GPU": 2}, {"GPU": 2, "CPU": 2}]
    assert get_bin_pack_residual(arg, [{"GPU": 2}, {"GPU": 2}])[0] == []
    arg = [{"CPU": 2}, {"GPU": 2}]
    assert get_bin_pack_residual(arg, [{
        "GPU": 2
    }, {
        "GPU": 2
    }])[0] == [{
        "GPU": 2
    }]
    arg = [{"GPU": 3}]
    assert get_bin_pack_residual(
        arg, [{
            "GPU": 1
        }, {
            "GPU": 1
        }], strict_spread=False)[0] == []
    assert get_bin_pack_residual(
        arg, [{
            "GPU": 1
        }, {
            "GPU": 1
        }], strict_spread=True) == ([{
            "GPU": 1
        }], [{
            "GPU": 2
        }])


def test_get_nodes_packing_heuristic():
    assert get_nodes_for(TYPES_A, {}, 9999, [{"GPU": 8}]) == \
        {"p2.8xlarge": 1}
    assert get_nodes_for(TYPES_A, {}, 9999, [{"GPU": 1}] * 6) == \
        {"p2.8xlarge": 1}
    assert get_nodes_for(TYPES_A, {}, 9999, [{"GPU": 1}] * 4) == \
        {"p2.xlarge": 4}
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 32, "GPU": 1}] * 3) \
        == {"p2.8xlarge": 3}
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 64, "GPU": 1}] * 3) \
        == {}
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 64}] * 3) == \
        {"m4.16xlarge": 3}
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 64}, {"CPU": 1}]) \
        == {"m4.16xlarge": 1, "m4.large": 1}
    assert get_nodes_for(
        TYPES_A, {}, 9999, [{"CPU": 64}, {"CPU": 9}, {"CPU": 9}]) == \
        {"m4.16xlarge": 1, "m4.4xlarge": 2}
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 16}] * 5) == \
        {"m4.16xlarge": 1, "m4.4xlarge": 1}
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 8}] * 10) == \
        {"m4.16xlarge": 1, "m4.4xlarge": 1}
    assert get_nodes_for(TYPES_A, {}, 9999, [{"CPU": 1}] * 100) == \
        {"m4.16xlarge": 1, "m4.4xlarge": 2, "m4.large": 2}
    assert get_nodes_for(
        TYPES_A, {}, 9999, [{"GPU": 1}] + ([{"CPU": 1}] * 64)) == \
        {"m4.16xlarge": 1, "p2.xlarge": 1}
    assert get_nodes_for(
        TYPES_A, {}, 9999, ([{"GPU": 1}] * 8) + ([{"CPU": 1}] * 64)) == \
        {"m4.16xlarge": 1, "p2.8xlarge": 1}
    assert get_nodes_for(
        TYPES_A, {}, 9999, [{
            "GPU": 1
        }] * 8, strict_spread=False) == {
            "p2.8xlarge": 1
        }
    assert get_nodes_for(
        TYPES_A, {}, 9999, [{
            "GPU": 1
        }] * 8, strict_spread=True) == {
            "p2.xlarge": 8
        }


def test_get_nodes_respects_max_limit():
    types = {
        "m4.large": {
            "resources": {
                "CPU": 2
            },
            "max_workers": 10,
        },
        "gpu": {
            "resources": {
                "GPU": 1
            },
            "max_workers": 99999,
        },
    }
    assert get_nodes_for(types, {}, 2, [{"CPU": 1}] * 10) == \
        {"m4.large": 2}
    assert get_nodes_for(types, {"m4.large": 9999}, 9999, [{
        "CPU": 1
    }] * 10) == {}
    assert get_nodes_for(types, {"m4.large": 0}, 9999, [{
        "CPU": 1
    }] * 10) == {
        "m4.large": 5
    }
    assert get_nodes_for(types, {"m4.large": 7}, 4, [{
        "CPU": 1
    }] * 10) == {
        "m4.large": 3
    }
    assert get_nodes_for(types, {"m4.large": 7}, 2, [{
        "CPU": 1
    }] * 10) == {
        "m4.large": 2
    }


def test_add_min_workers_nodes():
    types = {
        "m2.large": {
            "resources": {
                "CPU": 2
            },
            "min_workers": 50,
            "max_workers": 100,
        },
        "m4.large": {
            "resources": {
                "CPU": 2
            },
            "min_workers": 0,
            "max_workers": 10,
        },
        "gpu": {
            "resources": {
                "GPU": 1
            },
            "min_workers": 99999,
            "max_workers": 99999,
        },
        "gpubla": {
            "resources": {
                "GPU": 1
            },
            "min_workers": 10,
            "max_workers": 0,
        },
    }
    assert _add_min_workers_nodes([],
                                  {},
                                  types) == \
        ([{"CPU": 2}]*50+[{"GPU": 1}]*99999, {"m2.large": 50, "gpu": 99999},
            {"m2.large": 50, "gpu": 99999})

    assert _add_min_workers_nodes([{"CPU": 2}]*5,
                                  {"m2.large": 5},
                                  types) == \
        ([{"CPU": 2}]*50+[{"GPU": 1}]*99999, {"m2.large": 50, "gpu": 99999},
            {"m2.large": 45, "gpu": 99999})

    assert _add_min_workers_nodes([{"CPU": 2}]*60,
                                  {"m2.large": 60},
                                  types) == \
        ([{"CPU": 2}]*60+[{"GPU": 1}]*99999, {"m2.large": 60, "gpu": 99999},
            {"gpu": 99999})

    assert _add_min_workers_nodes([{
        "CPU": 2
    }] * 50 + [{
        "GPU": 1
    }] * 99999, {
        "m2.large": 50,
        "gpu": 99999
    }, types) == ([{
        "CPU": 2
    }] * 50 + [{
        "GPU": 1
    }] * 99999, {
        "m2.large": 50,
        "gpu": 99999
    }, {})

    assert _add_min_workers_nodes([], {},
                                  {"gpubla": types["gpubla"]}) == ([], {}, {})

    types["gpubla"]["max_workers"] = 10
    assert _add_min_workers_nodes([], {}, {"gpubla": types["gpubla"]}) == ([{
        "GPU": 1
    }] * 10, {
        "gpubla": 10
    }, {
        "gpubla": 10
    })


def test_get_nodes_to_launch_with_min_workers():
    provider = MockProvider()
    new_types = copy.deepcopy(TYPES_A)
    new_types["p2.8xlarge"]["min_workers"] = 2
    scheduler = ResourceDemandScheduler(provider, new_types, 3)

    provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 1)

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    utilizations = {ip: {"GPU": 8} for ip in ips}

    to_launch = scheduler.get_nodes_to_launch(nodes, {}, [{
        "GPU": 8
    }], utilizations, [], {})
    assert to_launch == {"p2.8xlarge": 1}


def test_get_nodes_to_launch_with_min_workers_and_bin_packing():
    provider = MockProvider()
    new_types = copy.deepcopy(TYPES_A)
    new_types["p2.8xlarge"]["min_workers"] = 2
    scheduler = ResourceDemandScheduler(provider, new_types, 10)

    provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 1)

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    # 1 free p2.8xls
    utilizations = {ip: {"GPU": 8} for ip in ips}
    # 1 more on the way
    pending_nodes = {"p2.8xlarge": 1}
    # requires 2 p2.8xls (only 2 are in cluster/pending) and 1 p2.xlarge
    demands = [{"GPU": 8}] * (len(utilizations) + 1) + [{"GPU": 1}]
    to_launch = scheduler.get_nodes_to_launch(nodes, pending_nodes, demands,
                                              utilizations, [], {})
    assert to_launch == {"p2.xlarge": 1}

    # 3 min_workers of p2.8xlarge covers the 2 p2.8xlarge + 1 p2.xlarge demand.
    # 2 p2.8xlarge are running/pending. So we need 1 more p2.8xlarge only to
    # meet the min_workers constraint and the demand.
    new_types["p2.8xlarge"]["min_workers"] = 3
    scheduler = ResourceDemandScheduler(provider, new_types, 10)
    to_launch = scheduler.get_nodes_to_launch(nodes, pending_nodes, demands,
                                              utilizations, [], {})
    # Make sure it does not return [("p2.8xlarge", 1), ("p2.xlarge", 1)]
    assert to_launch == {"p2.8xlarge": 1}


def test_get_nodes_to_launch_limits():
    provider = MockProvider()
    scheduler = ResourceDemandScheduler(provider, TYPES_A, 3)

    provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 2)

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    utilizations = {ip: {"GPU": 8} for ip in ips}

    to_launch = scheduler.get_nodes_to_launch(nodes, {"p2.8xlarge": 1}, [{
        "GPU": 8
    }] * 2, utilizations, [], {})
    assert to_launch == {}


def test_calculate_node_resources():
    provider = MockProvider()
    scheduler = ResourceDemandScheduler(provider, TYPES_A, 10)

    provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 2)

    nodes = provider.non_terminated_nodes({})

    ips = provider.non_terminated_node_ips({})
    # 2 free p2.8xls
    utilizations = {ip: {"GPU": 8} for ip in ips}
    # 1 more on the way
    pending_nodes = {"p2.8xlarge": 1}
    # requires 4 p2.8xls (only 3 are in cluster/pending)
    demands = [{"GPU": 8}] * (len(utilizations) + 2)
    to_launch = scheduler.get_nodes_to_launch(nodes, pending_nodes, demands,
                                              utilizations, [], {})

    assert to_launch == {"p2.8xlarge": 1}


def test_request_resources_existing_usage():
    provider = MockProvider()
    TYPES = {
        "p2.8xlarge": {
            "node_config": {},
            "resources": {
                "CPU": 32,
                "GPU": 8
            },
            "max_workers": 40,
        },
    }
    scheduler = ResourceDemandScheduler(provider, TYPES, max_workers=100)

    # 5 nodes with 32 CPU and 8 GPU each
    provider.create_node({}, {
        TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
        TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE
    }, 2)
    all_nodes = provider.non_terminated_nodes({})
    node_ips = provider.non_terminated_node_ips({})
    assert len(node_ips) == 2, node_ips

    # Fully utilized, no requests.
    avail_by_ip = {ip: {} for ip in node_ips}
    max_by_ip = {ip: {"GPU": 8, "CPU": 32} for ip in node_ips}
    demands = []
    to_launch = scheduler.get_nodes_to_launch(all_nodes, {}, [], avail_by_ip,
                                              [], max_by_ip, demands)
    assert len(to_launch) == 0, to_launch

    # Fully utilized, resource requests exactly equal.
    avail_by_ip = {ip: {} for ip in node_ips}
    demands = [{"GPU": 4}] * 4
    to_launch = scheduler.get_nodes_to_launch(all_nodes, {}, [], avail_by_ip,
                                              [], max_by_ip, demands)
    assert len(to_launch) == 0, to_launch

    # Fully utilized, resource requests in excess.
    avail_by_ip = {ip: {} for ip in node_ips}
    demands = [{"GPU": 4}] * 7
    to_launch = scheduler.get_nodes_to_launch(all_nodes, {}, [], avail_by_ip,
                                              [], max_by_ip, demands)
    assert to_launch.get("p2.8xlarge") == 2, to_launch

    # Not utilized, no requests.
    avail_by_ip = {ip: {"GPU": 4, "CPU": 32} for ip in node_ips}
    demands = []
    to_launch = scheduler.get_nodes_to_launch(all_nodes, {}, [], avail_by_ip,
                                              [], max_by_ip, demands)
    assert len(to_launch) == 0, to_launch

    # Not utilized, resource requests exactly equal.
    avail_by_ip = {ip: {"GPU": 4, "CPU": 32} for ip in node_ips}
    demands = [{"GPU": 4}] * 4
    to_launch = scheduler.get_nodes_to_launch(all_nodes, {}, [], avail_by_ip,
                                              [], max_by_ip, demands)
    assert len(to_launch) == 0, to_launch

    # Not utilized, resource requests in excess.
    avail_by_ip = {ip: {"GPU": 4, "CPU": 32} for ip in node_ips}
    demands = [{"GPU": 4}] * 7
    to_launch = scheduler.get_nodes_to_launch(all_nodes, {}, [], avail_by_ip,
                                              [], max_by_ip, demands)
    assert to_launch.get("p2.8xlarge") == 2, to_launch

    # Not utilized, resource requests hugely in excess.
    avail_by_ip = {ip: {"GPU": 4, "CPU": 32} for ip in node_ips}
    demands = [{"GPU": 4}] * 70
    to_launch = scheduler.get_nodes_to_launch(all_nodes, {}, [], avail_by_ip,
                                              [], max_by_ip, demands)
    # This bypasses the launch rate limit.
    assert to_launch.get("p2.8xlarge") == 33, to_launch


def test_backlog_queue_impact_on_binpacking_time():
    new_types = copy.deepcopy(TYPES_A)
    new_types["p2.8xlarge"]["max_workers"] = 1000
    new_types["m4.16xlarge"]["max_workers"] = 1000

    def test_backlog_queue_impact_on_binpacking_time_aux(
            num_available_nodes, time_to_assert, demand_request_shape):
        provider = MockProvider()
        scheduler = ResourceDemandScheduler(
            provider, new_types, max_workers=10000)

        provider.create_node({}, {
            TAG_RAY_USER_NODE_TYPE: "m4.16xlarge",
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE
        }, num_available_nodes)
        # <num_available_nodes> m4.16xlarge instances.
        cpu_ips = provider.non_terminated_node_ips({})
        provider.create_node({}, {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE
        }, num_available_nodes)
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
        demands = demand_request_shape * \
            AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE
        t1 = time.time()
        to_launch = scheduler.get_nodes_to_launch(all_nodes, {}, demands,
                                                  usage_by_ip, [], {})
        t2 = time.time()
        assert t2 - t1 < time_to_assert
        print("The time took to launch", to_launch,
              "with number of available nodes set to", num_available_nodes,
              "is:", t2 - t1)
        return to_launch

    # The assertions below use 10s but the actual time took when this test was
    # measured on 2.3 GHz 8-Core Intel (I9-9880H) Core i9 is commented inline.

    # Check the time it takes when there are 0 nodes available and the demand
    # is requires adding another ~100 nodes.
    to_launch = test_backlog_queue_impact_on_binpacking_time_aux(
        num_available_nodes=0,
        time_to_assert=10,  # real time 0.2s.
        demand_request_shape=[{
            "GPU": 1
        }, {
            "CPU": 1
        }])
    # If not for the max launch concurrency the next assert should be:
    # {'m4.large': 4, 'm4.4xlarge': 2, 'm4.16xlarge': 15, 'p2.8xlarge': 125}.
    assert to_launch == {
        "m4.large": 4,
        "m4.4xlarge": 2,
        "m4.16xlarge": 5,
        "p2.8xlarge": 5
    }

    # Check the time it takes when there are 100 nodes available and the demand
    # requires another 75 nodes.
    to_launch = test_backlog_queue_impact_on_binpacking_time_aux(
        num_available_nodes=50,
        time_to_assert=10,  # real time 0.075s.
        demand_request_shape=[{
            "GPU": 1
        }, {
            "CPU": 2
        }])
    # If not for the max launch concurrency the next assert should be:
    # {'p2.8xlarge': 75}.
    assert to_launch == {"p2.8xlarge": 50}

    # Check the time it takes when there are 250 nodes available and can
    # cover the demand.
    to_launch = test_backlog_queue_impact_on_binpacking_time_aux(
        num_available_nodes=125,
        time_to_assert=10,  # real time 0.06s.
        demand_request_shape=[{
            "GPU": 1
        }, {
            "CPU": 1
        }])
    assert to_launch == {}

    # Check the time it takes when there are 1000 nodes available and the
    # demand requires another 1000 nodes.
    to_launch = test_backlog_queue_impact_on_binpacking_time_aux(
        num_available_nodes=500,
        time_to_assert=10,  # real time 1.32s.
        demand_request_shape=[{
            "GPU": 8
        }, {
            "CPU": 64
        }])
    assert to_launch == {"m4.16xlarge": 500, "p2.8xlarge": 500}


class TestPlacementGroupScaling:
    def test_strategies(self):
        provider = MockProvider()
        scheduler = ResourceDemandScheduler(provider, TYPES_A, 10)

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
                    Bundle(unit_resources={"GPU": 2})
                ]),
            # Requires a new node (uses the whole node).
            PlacementGroupTableData(
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.STRICT_PACK,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 4)),
            # Fits across the machines that strict spread.
            PlacementGroupTableData(
                # runs on.
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.PACK,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 2)),
            # Fits across the machines that strict spread.
            PlacementGroupTableData(
                # runs on.
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.SPREAD,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 2)),
        ]
        to_launch = scheduler.get_nodes_to_launch(
            nodes, {}, resource_demands, {}, pending_placement_groups, {})
        assert to_launch == {"p2.8xlarge": 2}

    def test_many_strict_spreads(self):
        provider = MockProvider()
        scheduler = ResourceDemandScheduler(provider, TYPES_A, 10)

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
                bundles=[Bundle(unit_resources={"GPU": 2})] * 3),
        ]
        # Each placement group will take up 2 GPUs per node, but the distinct
        # placement groups should still reuse the same nodes.
        pending_placement_groups = pending_placement_groups * 3
        to_launch = scheduler.get_nodes_to_launch(
            nodes, {}, resource_demands, {}, pending_placement_groups, {})
        assert to_launch == {"p2.8xlarge": 1}

    def test_packing(self):
        provider = MockProvider()
        scheduler = ResourceDemandScheduler(provider, TYPES_A, 10)

        provider.create_node({}, {TAG_RAY_USER_NODE_TYPE: "p2.8xlarge"}, 1)
        # At this point our cluster has 1 p2.8xlarge instances (8 GPUs) and is
        # fully idle.
        nodes = provider.non_terminated_nodes({})

        resource_demands = [{"GPU": 1}] * 2
        pending_placement_groups = [
            PlacementGroupTableData(
                state=PlacementGroupTableData.PENDING,
                strategy=PlacementStrategy.STRICT_PACK,
                bundles=[Bundle(unit_resources={"GPU": 2})] * 3),
        ]
        # The 2 resource demand gpus should still be packed onto the same node
        # as the 6 GPU placement group.
        to_launch = scheduler.get_nodes_to_launch(
            nodes, {}, resource_demands, {}, pending_placement_groups, {})
        assert to_launch == {}


def test_get_concurrent_resource_demand_to_launch():
    node_types = copy.deepcopy(TYPES_A)
    node_types["p2.8xlarge"]["min_workers"] = 1
    node_types["p2.8xlarge"]["max_workers"] = 10
    node_types["m4.large"]["min_workers"] = 2
    node_types["m4.large"]["max_workers"] = 100
    provider = MockProvider()
    scheduler = ResourceDemandScheduler(provider, node_types, 200)
    # Sanity check.
    assert len(provider.non_terminated_nodes({})) == 0

    # Sanity check.
    updated_to_launch = \
        scheduler._get_concurrent_resource_demand_to_launch({}, [], [], {}, {})
    assert updated_to_launch == {}

    provider.create_node({}, {
        TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
        TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
    }, 1)
    provider.create_node({}, {
        TAG_RAY_USER_NODE_TYPE: "m4.large",
        TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
    }, 2)

    # All nodes so far are pending/launching here.
    to_launch = {"p2.8xlarge": 4, "m4.large": 40}
    non_terminated_nodes = provider.non_terminated_nodes({})
    pending_launches_nodes = {"p2.8xlarge": 1, "m4.large": 1}
    connected_nodes = []  # All the non_terminated_nodes are not connected yet.
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes,
        pending_launches_nodes, {})
    # Note: we have 2 pending/launching gpus, 3 pending/launching cpus,
    # 0 running gpu, and 0 running cpus.
    assert updated_to_launch == {"p2.8xlarge": 3, "m4.large": 2}

    # This starts the min workers only, so we have no more pending workers.
    # The workers here are either running (connected) or in
    # pending_launches_nodes (i.e., launching).
    connected_nodes = [
        provider.internal_ip(node_id) for node_id in non_terminated_nodes
    ]
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes,
        pending_launches_nodes, {})
    # Note that here we have 1 launching gpu, 1 launching cpu,
    # 1 running gpu, and 2 running cpus.
    assert updated_to_launch == {"p2.8xlarge": 4, "m4.large": 4}

    # Launch the nodes. Note, after create_node the node is pending.
    provider.create_node({}, {
        TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
        TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
    }, 5)
    provider.create_node({}, {
        TAG_RAY_USER_NODE_TYPE: "m4.large",
        TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
    }, 5)

    # Continue scaling.
    non_terminated_nodes = provider.non_terminated_nodes({})
    to_launch = {"m4.large": 36}  # No more gpus are necessary
    pending_launches_nodes = {}  # No pending launches
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes,
        pending_launches_nodes, {})
    # Note: we have 5 pending cpus. So we are not allowed to start any.
    # Still only 2 running cpus.
    assert updated_to_launch == {}

    # All the non_terminated_nodes are connected here.
    connected_nodes = [
        provider.internal_ip(node_id) for node_id in non_terminated_nodes
    ]
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes,
        pending_launches_nodes, {})
    # Note: that here we have 7 running cpus and nothing pending/launching.
    assert updated_to_launch == {"m4.large": 7}

    # Launch the nodes. Note, after create_node the node is pending.
    provider.create_node({}, {
        TAG_RAY_USER_NODE_TYPE: "m4.large",
        TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
    }, 7)

    # Continue scaling.
    non_terminated_nodes = provider.non_terminated_nodes({})
    to_launch = {"m4.large": 29}
    pending_launches_nodes = {"m4.large": 1}
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes,
        pending_launches_nodes, {})
    # Note: we have 8 pending/launching cpus and only 7 running.
    # So we should not launch anything (8 < 7).
    assert updated_to_launch == {}

    # All the non_terminated_nodes are connected here.
    connected_nodes = [
        provider.internal_ip(node_id) for node_id in non_terminated_nodes
    ]
    updated_to_launch = scheduler._get_concurrent_resource_demand_to_launch(
        to_launch, connected_nodes, non_terminated_nodes,
        pending_launches_nodes, {})
    # Note: that here we have 14 running cpus and 1 launching.
    assert updated_to_launch == {"m4.large": 13}


def test_get_nodes_to_launch_max_launch_concurrency():
    provider = MockProvider()
    new_types = copy.deepcopy(TYPES_A)
    new_types["p2.8xlarge"]["min_workers"] = 4
    new_types["p2.8xlarge"]["max_workers"] = 40

    scheduler = ResourceDemandScheduler(provider, new_types, 30)

    to_launch = scheduler.get_nodes_to_launch([], {}, [], {}, [], {})
    # Respects min_workers despite concurrency limitation.
    assert to_launch == {"p2.8xlarge": 4}

    provider.create_node({}, {
        TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
        TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED
    }, 1)
    nodes = provider.non_terminated_nodes({})
    # Trying to force here that the node shows in nodes but not connected yet
    # and hence does not show up in LoadMetrics (or utilizations).
    ips = provider.non_terminated_node_ips({
        TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE
    })
    utilizations = {ip: {"GPU": 8} for ip in ips}
    launching_nodes = {"p2.8xlarge": 1}
    # requires 41 p2.8xls (currently 1 pending, 1 launching, 0 running}
    demands = [{"GPU": 8}] * (len(utilizations) + 40)
    to_launch = scheduler.get_nodes_to_launch(nodes, launching_nodes, demands,
                                              utilizations, [], {})
    # Enforces max launch to 5 when < 5 running. 2 are pending/launching.
    assert to_launch == {"p2.8xlarge": 3}

    provider.create_node({}, {
        TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
        TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE
    }, 8)
    nodes = provider.non_terminated_nodes({})
    ips = provider.non_terminated_node_ips({
        TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE
    })
    utilizations = {ip: {"GPU": 8} for ip in ips}
    launching_nodes = {"p2.8xlarge": 1}
    # Requires additional 17 p2.8xls (now 1 pending, 1 launching, 8 running}
    demands = [{"GPU": 8}] * (len(utilizations) + 15)
    to_launch = scheduler.get_nodes_to_launch(nodes, launching_nodes, demands,
                                              utilizations, [], {})
    # We are allowed to launch up to 8 more since 8 are running.
    # We already have 2 pending/launching, so only 6 remain.
    assert to_launch == {"p2.8xlarge": 6}


def test_rewrite_legacy_yaml_to_available_node_types():
    cluster_config = copy.deepcopy(SMALL_CLUSTER)  # Legacy cluster_config.
    cluster_config = rewrite_legacy_yaml_to_available_node_types(
        cluster_config)
    assert cluster_config["available_node_types"][NODE_TYPE_LEGACY_HEAD][
        "max_workers"] == 0
    assert cluster_config["available_node_types"][NODE_TYPE_LEGACY_HEAD][
        "min_workers"] == 0
    assert cluster_config["available_node_types"][NODE_TYPE_LEGACY_HEAD][
        "node_config"] == SMALL_CLUSTER["head_node"]

    assert cluster_config["available_node_types"][NODE_TYPE_LEGACY_WORKER][
        "node_config"] == SMALL_CLUSTER["worker_nodes"]
    assert cluster_config["available_node_types"][NODE_TYPE_LEGACY_WORKER][
        "max_workers"] == SMALL_CLUSTER["max_workers"]
    assert cluster_config["available_node_types"][NODE_TYPE_LEGACY_WORKER][
        "min_workers"] == SMALL_CLUSTER["min_workers"]


def test_handle_legacy_cluster_config_yaml():
    provider = MockProvider()
    head_resources = {"CPU": 8, "GPU": 1}
    worker_resources = {"CPU": 32, "GPU": 8}
    cluster_config = copy.deepcopy(SMALL_CLUSTER)  # Legacy cluster_config.
    cluster_config = rewrite_legacy_yaml_to_available_node_types(
        cluster_config)
    scheduler = ResourceDemandScheduler(
        provider, cluster_config["available_node_types"], 0)
    provider.create_node({}, {
        TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
        TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_HEAD
    }, 1)
    head_ip = provider.non_terminated_node_ips({})[0]
    head_node_id = provider.non_terminated_nodes({})[0]
    to_launch = scheduler.get_nodes_to_launch([], {}, [], {}, [],
                                              {head_ip: head_resources})
    assert to_launch == {}  # Should always be empty with max_workers = 0.

    scheduler.max_workers = 30
    min_workers = scheduler.node_types[NODE_TYPE_LEGACY_WORKER]["min_workers"]
    scheduler.node_types[NODE_TYPE_LEGACY_WORKER]["min_workers"] = 0
    to_launch = scheduler.get_nodes_to_launch([head_node_id], {}, [], {}, [],
                                              {head_ip: head_resources})
    assert to_launch == {
    }  # Since the resource demand does not require adding nodes.
    to_launch = scheduler.get_nodes_to_launch([head_node_id], {},
                                              [head_resources], {}, [],
                                              {head_ip: head_resources})
    assert to_launch == {
    }  # Since the resource demand does not require adding nodes.

    scheduler.node_types[NODE_TYPE_LEGACY_WORKER]["min_workers"] = min_workers
    # Returns min_workers when min_workers>0.
    to_launch = scheduler.get_nodes_to_launch([head_node_id], {},
                                              [head_resources], {}, [],
                                              {head_ip: head_resources})
    assert to_launch == {NODE_TYPE_LEGACY_WORKER: min_workers}

    provider.create_node({}, {
        TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
        TAG_RAY_NODE_STATUS: STATUS_UNINITIALIZED,
        TAG_RAY_USER_NODE_TYPE: NODE_TYPE_LEGACY_WORKER
    }, min_workers)
    nodes = provider.non_terminated_nodes({})
    to_launch = scheduler.get_nodes_to_launch(nodes, {}, [head_resources], {},
                                              [], {head_ip: head_resources})
    assert to_launch == {}  # A node is running, at some point it'll connect.
    pending_launches = {NODE_TYPE_LEGACY_WORKER: 4}
    to_launch = scheduler.get_nodes_to_launch([], pending_launches,
                                              [head_resources], {}, [],
                                              {head_ip: head_resources})
    assert to_launch == {}  # A node is launching, at some point it'll connect.

    # Now assume that we already launched/connected the nodes.
    ips = provider.non_terminated_node_ips({})
    lm = LoadMetrics()
    worker_ips = []
    for ip in ips:
        if ip == head_ip:
            lm.update(ip, head_resources, head_resources, {})
        else:
            lm.update(ip, worker_resources, worker_resources, {})
            worker_ips.append(ip)

    assert not scheduler.node_types[NODE_TYPE_LEGACY_WORKER]["resources"]
    to_launch = scheduler.get_nodes_to_launch(
        nodes, {}, [], {}, [], lm.get_static_node_resources_by_ip())
    assert scheduler.node_types[NODE_TYPE_LEGACY_WORKER][
        "resources"] == worker_resources
    assert to_launch == {}
    utilizations = {ip: worker_resources for ip in worker_ips}
    utilizations[head_ip] = head_resources
    # Requires 4 nodes since worker resources is bigger than head reasources.
    demands = [worker_resources] * (len(utilizations) + 3)
    to_launch = scheduler.get_nodes_to_launch(
        nodes, {}, demands, utilizations, [],
        lm.get_static_node_resources_by_ip())
    # 4 nodes are necessary to meet resource demand, but we never exceed
    # max_workers.
    assert to_launch == {}
    scheduler.max_workers = 10
    to_launch = scheduler.get_nodes_to_launch(
        nodes, {}, demands, utilizations, [],
        lm.get_static_node_resources_by_ip())
    # 4 nodes are necessary to meet resource demand, but we never exceed
    # max_workers.
    assert to_launch == {}
    scheduler.node_types[NODE_TYPE_LEGACY_WORKER]["max_workers"] = 10
    to_launch = scheduler.get_nodes_to_launch(
        nodes, {}, demands, utilizations, [],
        lm.get_static_node_resources_by_ip())
    # 4 nodes are necessary to meet resource demand.
    assert to_launch == {NODE_TYPE_LEGACY_WORKER: 4}
    to_launch = scheduler.get_nodes_to_launch(nodes, pending_launches, demands,
                                              utilizations, [],
                                              lm.get_node_resources())
    # 0 because there are 4 pending launches and we only need 4.
    assert to_launch == {}
    to_launch = scheduler.get_nodes_to_launch(nodes, pending_launches,
                                              demands * 2, utilizations, [],
                                              lm.get_node_resources())
    # 1 because there are 4 pending launches and we only allow a max of 5.
    assert to_launch == {NODE_TYPE_LEGACY_WORKER: 1}


class LoadMetricsTest(unittest.TestCase):
    def testResourceDemandVector(self):
        lm = LoadMetrics()
        lm.update(
            "1.1.1.1", {"CPU": 2}, {"CPU": 1}, {},
            waiting_bundles=[{
                "GPU": 1
            }],
            infeasible_bundles=[{
                "CPU": 16
            }])
        assert same_elements(lm.get_resource_demand_vector(), [{
            "CPU": 16
        }, {
            "GPU": 1
        }])

    def testPlacementGroupLoad(self):
        lm = LoadMetrics()
        pending_placement_groups = [
            PlacementGroupTableData(
                state=PlacementGroupTableData.RESCHEDULING,
                strategy=PlacementStrategy.PACK,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 2)),
            PlacementGroupTableData(
                state=PlacementGroupTableData.RESCHEDULING,
                strategy=PlacementStrategy.SPREAD,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 2)),
        ]
        lm.update(
            "1.1.1.1", {}, {}, {},
            pending_placement_groups=pending_placement_groups)
        assert lm.get_pending_placement_groups() == pending_placement_groups


class AutoscalingTest(unittest.TestCase):
    def setUp(self):
        _NODE_PROVIDERS["mock"] = \
            lambda config: self.create_provider
        self.provider = None
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        self.provider = None
        del _NODE_PROVIDERS["mock"]
        _clear_provider_cache()
        shutil.rmtree(self.tmpdir)
        ray.shutdown()

    def waitForNodes(self, expected, comparison=None, tag_filters={}):
        MAX_ITER = 50
        for i in range(MAX_ITER):
            n = len(self.provider.non_terminated_nodes(tag_filters))
            if comparison is None:
                comparison = self.assertEqual
            try:
                comparison(n, expected)
                return
            except Exception:
                if i == MAX_ITER - 1:
                    raise
            time.sleep(.1)

    def create_provider(self, config, cluster_name):
        assert self.provider
        return self.provider

    def write_config(self, config):
        path = self.tmpdir + "/simple.yaml"
        with open(path, "w") as f:
            f.write(yaml.dump(config))
        return path

    def testGetOrCreateMultiNodeType(self):
        config_path = self.write_config(MULTI_WORKER_CLUSTER)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]"])
        get_or_create_head_node(
            MULTI_WORKER_CLUSTER,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner)
        self.waitForNodes(1)
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")
        self.assertEqual(self.provider.mock_nodes[0].node_type, "empty_node")
        self.assertEqual(
            self.provider.mock_nodes[0].node_config.get("FooProperty"), 42)
        self.assertEqual(
            self.provider.mock_nodes[0].node_config.get("TestProp"), 1)
        self.assertEqual(
            self.provider.mock_nodes[0].tags.get(TAG_RAY_USER_NODE_TYPE),
            "empty_node")

    def testGetOrCreateMultiNodeTypeCustomHeadResources(self):
        config = copy.deepcopy(MULTI_WORKER_CLUSTER)
        config["available_node_types"]["empty_node"]["resources"] = {
            "empty_resource_name": 1000
        }
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]"])
        get_or_create_head_node(
            config,
            printable_config_file=config_path,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=None,
            _provider=self.provider,
            _runner=runner)
        self.waitForNodes(1)
        runner.assert_has_call("1.2.3.4", "init_cmd")
        runner.assert_has_call("1.2.3.4", "setup_cmd")
        runner.assert_has_call("1.2.3.4", "start_ray_head")
        runner.assert_has_call("1.2.3.4", "empty_resource_name")
        self.assertEqual(self.provider.mock_nodes[0].node_type, "empty_node")
        self.assertEqual(
            self.provider.mock_nodes[0].node_config.get("FooProperty"), 42)
        self.assertEqual(
            self.provider.mock_nodes[0].node_config.get("TestProp"), 1)
        self.assertEqual(
            self.provider.mock_nodes[0].tags.get(TAG_RAY_USER_NODE_TYPE),
            "empty_node")

    def testScaleUpMinSanity(self):
        config = copy.deepcopy(MULTI_WORKER_CLUSTER)
        config["available_node_types"]["m4.large"]["min_workers"] = \
            config["min_workers"]
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(2)
        autoscaler.update()
        self.waitForNodes(2)

    def testPlacementGroup(self):
        # Note this is mostly an integration test. See
        # testPlacementGroupScaling for more comprehensive tests.
        config = copy.deepcopy(MULTI_WORKER_CLUSTER)
        config["min_workers"] = 0
        config["max_workers"] = 999
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_USER_NODE_TYPE: "m4.4xlarge"
        }, 1)
        head_ip = self.provider.non_terminated_node_ips({})[0]
        assert len(self.provider.non_terminated_nodes({})) == 1
        autoscaler.update()
        self.waitForNodes(1)

        pending_placement_groups = [
            PlacementGroupTableData(
                state=PlacementGroupTableData.RESCHEDULING,
                strategy=PlacementStrategy.STRICT_SPREAD,
                bundles=[Bundle(unit_resources={"GPU": 2})] * 3),
            PlacementGroupTableData(
                state=PlacementGroupTableData.RESCHEDULING,
                strategy=PlacementStrategy.PACK,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 5)),
        ]
        # Since placement groups are implemented with custom resources, this is
        # an example of the accompanying resource demands. Note the resource
        # demand autoscaler will be unable to fulfill these demands, but we
        # should still handle the other infeasible/waiting bundles.
        placement_group_resource_demands = [{
            "GPU_group_0_6c2506ac733bc37496295b02c4fad446": 0.0101,
            "GPU_group_6c2506ac733bc37496295b02c4fad446": 0.0101
        }]
        lm.update(
            head_ip, {"CPU": 16}, {"CPU": 16}, {},
            infeasible_bundles=placement_group_resource_demands,
            waiting_bundles=[{
                "GPU": 8
            }],
            pending_placement_groups=pending_placement_groups)
        autoscaler.update()
        self.waitForNodes(5)

        for i in range(1, 5):
            assert self.provider.mock_nodes[i].node_type == "p2.8xlarge"

        pending_placement_groups = [
            PlacementGroupTableData(
                state=PlacementGroupTableData.RESCHEDULING,
                strategy=PlacementStrategy.STRICT_PACK,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 4)),
            PlacementGroupTableData(
                state=PlacementGroupTableData.RESCHEDULING,
                strategy=PlacementStrategy.SPREAD,
                bundles=([Bundle(unit_resources={"GPU": 2})] * 2)),
        ]

    def testScaleUpMinWorkers(self):
        config = copy.deepcopy(MULTI_WORKER_CLUSTER)
        config["max_workers"] = 50
        config["idle_timeout_minutes"] = 1
        config["available_node_types"]["m4.large"]["min_workers"] = 1
        config["available_node_types"]["p2.8xlarge"]["min_workers"] = 1
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(2)
        assert len(self.provider.mock_nodes) == 2
        assert {
            self.provider.mock_nodes[0].node_type,
            self.provider.mock_nodes[1].node_type
        } == {"p2.8xlarge", "m4.large"}
        self.provider.create_node({}, {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER
        }, 2)
        self.provider.create_node({}, {
            TAG_RAY_USER_NODE_TYPE: "m4.16xlarge",
            TAG_RAY_NODE_KIND: NODE_KIND_WORKER
        }, 2)
        assert len(self.provider.non_terminated_nodes({})) == 6
        # Make sure that after idle_timeout_minutes we don't kill idle
        # min workers.
        for node_id in self.provider.non_terminated_nodes({}):
            lm.last_used_time_by_ip[self.provider.internal_ip(node_id)] = -60
        autoscaler.update()
        self.waitForNodes(2)

        cnt = 0
        for id in self.provider.mock_nodes:
            if self.provider.mock_nodes[id].state == "running" or \
                    self.provider.mock_nodes[id].state == "pending":
                assert self.provider.mock_nodes[id].node_type in {
                    "p2.8xlarge", "m4.large"
                }
                cnt += 1
        assert cnt == 2

    def testScaleUpIgnoreUsed(self):
        config = MULTI_WORKER_CLUSTER.copy()
        # Commenting out this line causes the test case to fail?!?!
        config["min_workers"] = 0
        config["target_utilization_fraction"] = 1.0
        config_path = self.write_config(config)
        self.provider = MockProvider()
        self.provider.create_node({}, {
            TAG_RAY_NODE_KIND: "head",
            TAG_RAY_USER_NODE_TYPE: "p2.xlarge"
        }, 1)
        head_ip = self.provider.non_terminated_node_ips({})[0]
        self.provider.finish_starting_nodes()
        runner = MockProcessRunner()
        lm = LoadMetrics(local_ip=head_ip)
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        autoscaler.update()
        self.waitForNodes(1)
        lm.update(head_ip, {"CPU": 4, "GPU": 1}, {}, {})
        self.waitForNodes(1)

        lm.update(
            head_ip, {
                "CPU": 4,
                "GPU": 1
            }, {"GPU": 0}, {},
            waiting_bundles=[{
                "GPU": 1
            }])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.xlarge"

    def testRequestBundlesAccountsForHeadNode(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["head_node_type"] = "p2.8xlarge"
        config["min_workers"] = 0
        config["max_workers"] = 50
        config_path = self.write_config(config)
        self.provider = MockProvider()
        self.provider.create_node({}, {
            TAG_RAY_USER_NODE_TYPE: "p2.8xlarge",
            TAG_RAY_NODE_KIND: "head"
        }, 1)
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 1

        # These requests fit on the head node.
        autoscaler.update()
        self.waitForNodes(1)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert len(self.provider.mock_nodes) == 1
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(1)

        # This request requires an additional worker node.
        autoscaler.request_resources([{"GPU": 8}] * 2)
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"

    def testRequestBundles(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 50
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"
        autoscaler.request_resources([{"CPU": 32}] * 4)
        autoscaler.update()
        self.waitForNodes(4)
        assert self.provider.mock_nodes[2].node_type == "m4.16xlarge"
        assert self.provider.mock_nodes[3].node_type == "m4.16xlarge"

    def testResourcePassing(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 50
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"

        # TODO (Alex): Autoscaler creates the node during one update then
        # starts the updater in the enxt update. The sleep is largely
        # unavoidable because the updater runs in its own thread and we have no
        # good way of ensuring that the commands are sent in time.
        autoscaler.update()
        sleep(0.1)

        # These checks are done separately because we have no guarantees on the
        # order the dict is serialized in.
        runner.assert_has_call("172.0.0.0", "RAY_OVERRIDE_RESOURCES=")
        runner.assert_has_call("172.0.0.0", "\"CPU\":2")
        runner.assert_has_call("172.0.0.1", "RAY_OVERRIDE_RESOURCES=")
        runner.assert_has_call("172.0.0.1", "\"CPU\":32")
        runner.assert_has_call("172.0.0.1", "\"GPU\":8")

    def testScaleUpLoadMetrics(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["min_workers"] = 0
        config["max_workers"] = 50
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        lm = LoadMetrics()
        autoscaler = StandardAutoscaler(
            config_path,
            lm,
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.update()
        lm.update(
            "1.2.3.4", {}, {}, {},
            waiting_bundles=[{
                "GPU": 1
            }],
            infeasible_bundles=[{
                "CPU": 16
            }])
        autoscaler.update()
        self.waitForNodes(2)
        nodes = {
            self.provider.mock_nodes[0].node_type,
            self.provider.mock_nodes[1].node_type
        }
        assert nodes == {"p2.xlarge", "m4.4xlarge"}

    def testCommandPassing(self):
        t = "custom"
        config = MULTI_WORKER_CLUSTER.copy()
        config["available_node_types"]["p2.8xlarge"][
            "worker_setup_commands"] = ["new_worker_setup_command"]
        config["available_node_types"]["p2.xlarge"][
            "initialization_commands"] = ["new_worker_initialization_cmd"]
        config["available_node_types"]["p2.xlarge"]["resources"][t] = 1
        # Commenting out this line causes the test case to fail?!?!
        config["min_workers"] = 0
        config["max_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(2)])
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"
        autoscaler.request_resources([{"GPU": 1}] * 9)
        autoscaler.update()
        self.waitForNodes(3)
        assert self.provider.mock_nodes[2].node_type == "p2.xlarge"
        autoscaler.update()
        sleep(0.1)
        runner.assert_has_call(self.provider.mock_nodes[1].internal_ip,
                               "new_worker_setup_command")
        runner.assert_not_has_call(self.provider.mock_nodes[1].internal_ip,
                                   "setup_cmd")
        runner.assert_not_has_call(self.provider.mock_nodes[1].internal_ip,
                                   "worker_setup_cmd")
        runner.assert_has_call(self.provider.mock_nodes[2].internal_ip,
                               "new_worker_initialization_cmd")
        runner.assert_not_has_call(self.provider.mock_nodes[2].internal_ip,
                                   "init_cmd")

    def testDockerWorkers(self):
        config = MULTI_WORKER_CLUSTER.copy()
        config["available_node_types"]["p2.8xlarge"]["docker"] = {
            "worker_image": "p2.8x_image:latest",
            "worker_run_options": ["p2.8x-run-options"]
        }
        config["available_node_types"]["p2.xlarge"]["docker"] = {
            "worker_image": "p2x_image:nightly"
        }
        config["docker"]["worker_run_options"] = ["standard-run-options"]
        config["docker"]["image"] = "default-image:nightly"
        config["docker"]["worker_image"] = "default-image:nightly"
        # Commenting out this line causes the test case to fail?!?!
        config["min_workers"] = 0
        config["max_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        runner.respond_to_call("json .Config.Env", ["[]" for i in range(4)])
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"
        autoscaler.request_resources([{"GPU": 1}] * 9)
        autoscaler.update()
        self.waitForNodes(3)
        assert self.provider.mock_nodes[2].node_type == "p2.xlarge"
        autoscaler.update()
        # Fill up m4, p2.8, p2 and request 2 more CPUs
        autoscaler.request_resources([{
            "CPU": 2
        }, {
            "CPU": 16
        }, {
            "CPU": 32
        }, {
            "CPU": 2
        }])
        autoscaler.update()
        self.waitForNodes(4)
        assert self.provider.mock_nodes[3].node_type == "m4.16xlarge"
        autoscaler.update()
        sleep(0.1)
        runner.assert_has_call(self.provider.mock_nodes[1].internal_ip,
                               "p2.8x-run-options")
        runner.assert_has_call(self.provider.mock_nodes[1].internal_ip,
                               "p2.8x_image:latest")
        runner.assert_not_has_call(self.provider.mock_nodes[1].internal_ip,
                                   "default-image:nightly")
        runner.assert_not_has_call(self.provider.mock_nodes[1].internal_ip,
                                   "standard-run-options")

        runner.assert_has_call(self.provider.mock_nodes[2].internal_ip,
                               "p2x_image:nightly")
        runner.assert_has_call(self.provider.mock_nodes[2].internal_ip,
                               "standard-run-options")
        runner.assert_not_has_call(self.provider.mock_nodes[2].internal_ip,
                                   "p2.8x-run-options")

        runner.assert_has_call(self.provider.mock_nodes[3].internal_ip,
                               "default-image:nightly")
        runner.assert_has_call(self.provider.mock_nodes[3].internal_ip,
                               "standard-run-options")
        runner.assert_not_has_call(self.provider.mock_nodes[3].internal_ip,
                                   "p2.8x-run-options")
        runner.assert_not_has_call(self.provider.mock_nodes[3].internal_ip,
                                   "p2x_image:nightly")

    def testUpdateConfig(self):
        config = copy.deepcopy(MULTI_WORKER_CLUSTER)
        config["available_node_types"]["m4.large"]["min_workers"] = \
            config["min_workers"]
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(2)
        config["available_node_types"]["m4.large"]["min_workers"] = 0
        config["available_node_types"]["m4.large"]["node_config"][
            "field_changed"] = 1
        config_path = self.write_config(config)
        autoscaler.update()
        self.waitForNodes(0)

    def testEmptyDocker(self):
        config = MULTI_WORKER_CLUSTER.copy()
        del config["docker"]
        config["min_workers"] = 0
        config["max_workers"] = 10
        config_path = self.write_config(config)
        self.provider = MockProvider()
        runner = MockProcessRunner()
        autoscaler = StandardAutoscaler(
            config_path,
            LoadMetrics(),
            max_failures=0,
            process_runner=runner,
            update_interval_s=0)
        assert len(self.provider.non_terminated_nodes({})) == 0
        autoscaler.update()
        self.waitForNodes(0)
        autoscaler.request_resources([{"CPU": 1}])
        autoscaler.update()
        self.waitForNodes(1)
        assert self.provider.mock_nodes[0].node_type == "m4.large"
        autoscaler.request_resources([{"GPU": 8}])
        autoscaler.update()
        self.waitForNodes(2)
        assert self.provider.mock_nodes[1].node_type == "p2.8xlarge"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
