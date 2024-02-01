# coding: utf-8
import os
import sys
import time

import pytest

from ray.autoscaler.v2.instance_manager.config import InstanceReconcileConfig  # noqa
from ray.autoscaler.v2.instance_manager.reconciler import Reconciler
from ray.autoscaler.v2.tests.util import create_instance
from ray.core.generated.autoscaler_pb2 import ClusterResourceState
from ray.core.generated.instance_manager_pb2 import Instance


class TestReconciler:
    def test_requested_instance_no_op(self):
        instances = [
            create_instance(
                "i-1",
                Instance.REQUESTED,
                launch_request_id="l1",
                instance_type="type-1",
                status_times=[(Instance.REQUESTED, time.time_ns())],
            ),
        ]

        results = Reconciler.reconcile(
            instances,
            ray_cluster_resource_state=ClusterResourceState(),
            non_terminated_cloud_instances={},
            cloud_provider_errors=[],
            config=InstanceReconcileConfig(),
        )
        # No ops
        assert len(results) == 0


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
