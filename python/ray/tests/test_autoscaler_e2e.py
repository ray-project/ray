import copy
import os
import json
import shutil
import tempfile
import time
import unittest
from dataclasses import asdict
from datetime import datetime
from time import sleep
from unittest import mock
import subprocess
from ray.autoscaler._private.constants import AUTOSCALER_METRIC_PORT

import pytest
import yaml

import ray
import ray._private.ray_constants
from ray._private.gcs_utils import PlacementGroupTableData
from ray._private.test_utils import (
    same_elements,
    wait_for_condition,
    metric_check_condition,
)
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
from ray.cluster_utils import AutoscalingCluster
from functools import partial

import pytest
import platform

import ray
from ray.cluster_utils import AutoscalingCluster


def test_ray_status_e2e(shutdown_only):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-i": {
                "resources": {"CPU": 1, "fun": 1},
                "node_config": {},
                "min_workers": 1,
                "max_workers": 1,
            },
            "type-ii": {
                "resources": {"CPU": 1, "fun": 100},
                "node_config": {},
                "min_workers": 1,
                "max_workers": 1,
            },
        },
    )

    try:
        cluster.start()
        ray.init(address="auto")

        @ray.remote(num_cpus=0, resources={"fun": 2})
        class Actor:
            def ping(self):
                return None

        actor = Actor.remote()
        ray.get(actor.ping.remote())

        assert "Demands" in subprocess.check_output("ray status", shell=True).decode()
        assert (
            "Total Demands"
            not in subprocess.check_output("ray status", shell=True).decode()
        )
        assert (
            "Total Demands"
            in subprocess.check_output("ray status -v", shell=True).decode()
        )
        assert (
            "Total Demands"
            in subprocess.check_output("ray status --verbose", shell=True).decode()
        )
    finally:
        cluster.shutdown()


def test_metrics(shutdown_only):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-i": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 1,
                "max_workers": 1,
            },
            "type-ii": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 1,
                "max_workers": 1,
            },
        },
    )

    try:
        cluster.start()
        info = ray.init(address="auto")
        autoscaler_export_addr = "{}:{}".format(
            info.address_info["node_ip_address"], AUTOSCALER_METRIC_PORT
        )
        print("**************", autoscaler_export_addr, info.address_info)

        @ray.remote(num_cpus=1)
        class Foo:
            def ping(self):
                return True

        wait_for_condition(
            metric_check_condition(
                {"autoscaler_cluster_resources": 0, "autoscaler_pending_resources": 0},
                export_addr=autoscaler_export_addr,
            )
        )

        actors = [Foo.remote() for _ in range(2)]
        ray.get([actor.ping.remote() for actor in actors])

        wait_for_condition(
            metric_check_condition(
                {"autoscaler_cluster_resources": 2, "autoscaler_pending_resources": 0},
                export_addr=autoscaler_export_addr,
            )
        )
        # TODO (Alex): Ideally we'd also assert that pending_resources
        # eventually became 1 or 2, but it's difficult to do that in a
        # non-racey way. (Perhaps we would need to artificially delay the fake
        # autoscaler node launch?).

    finally:
        cluster.shutdown()
