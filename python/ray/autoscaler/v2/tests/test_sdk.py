# coding: utf-8
import os
import sys
from typing import List

import pytest  # noqa

import ray
import ray._private.ray_constants as ray_constants
from ray._private.test_utils import wait_for_condition
from ray.autoscaler.v2.sdk import request_cluster_resources
from ray.autoscaler.v2.tests.util import get_cluster_resource_state
from ray.core.generated.experimental import autoscaler_pb2_grpc
from ray.core.generated.experimental.autoscaler_pb2 import GetClusterResourceStateReply


def _autoscaler_state_service_stub():
    """Get the grpc stub for the autoscaler state service"""
    gcs_address = ray.get_runtime_context().gcs_address
    gcs_channel = ray._private.utils.init_grpc_channel(
        gcs_address, ray_constants.GLOBAL_GRPC_OPTIONS
    )
    return autoscaler_pb2_grpc.AutoscalerStateServiceStub(gcs_channel)


def assert_cluster_resource_constraints(
    reply: GetClusterResourceStateReply, expected: List[dict]
):
    """
    Assert a GetClusterResourceStateReply has cluster_resource_constraints that
    matches with the expected resources.
    """
    # We only have 1 constraint for now.
    assert len(reply.cluster_resource_constraints) == 1

    min_bundles = reply.cluster_resource_constraints[0].min_bundles
    assert len(min_bundles) == len(expected)

    # Sort all the bundles by bundle's resource names
    min_bundles = sorted(
        min_bundles, key=lambda bundle: "".join(bundle.resources_bundle.keys())
    )
    expected = sorted(expected, key=lambda bundle: "".join(bundle.keys()))

    for actual_bundle, expected_bundle in zip(min_bundles, expected):
        assert dict(actual_bundle.resources_bundle) == expected_bundle


def test_request_cluster_resources_basic(shutdown_only):
    ray.init(num_cpus=1)
    stub = _autoscaler_state_service_stub()

    # Request one
    request_cluster_resources([{"CPU": 1}])

    def verify():
        reply = get_cluster_resource_state(stub)
        assert_cluster_resource_constraints(reply, [{"CPU": 1}])
        return True

    wait_for_condition(verify)

    # Request another overrides the previous request
    request_cluster_resources([{"CPU": 2, "GPU": 1}, {"CPU": 1}])

    def verify():
        reply = get_cluster_resource_state(stub)
        assert_cluster_resource_constraints(reply, [{"CPU": 2, "GPU": 1}, {"CPU": 1}])
        return True

    wait_for_condition(verify)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
