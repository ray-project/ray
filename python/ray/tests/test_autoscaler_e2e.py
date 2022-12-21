import subprocess
from ray.autoscaler._private.constants import AUTOSCALER_METRIC_PORT

import ray
import sys
from ray._private.test_utils import (
    wait_for_condition,
    get_metric_check_condition,
)
from ray.cluster_utils import AutoscalingCluster
from ray.autoscaler.node_launch_exception import NodeLaunchException


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

        @ray.remote(num_cpus=1)
        class Foo:
            def ping(self):
                return True

        zero_reported_condition = get_metric_check_condition(
            {"autoscaler_cluster_resources": 0, "autoscaler_pending_resources": 0},
            export_addr=autoscaler_export_addr,
        )
        wait_for_condition(zero_reported_condition)

        actors = [Foo.remote() for _ in range(2)]
        ray.get([actor.ping.remote() for actor in actors])

        two_cpu_no_pending_condition = get_metric_check_condition(
            {"autoscaler_cluster_resources": 2, "autoscaler_pending_resources": 0},
            export_addr=autoscaler_export_addr,
        )
        wait_for_condition(two_cpu_no_pending_condition)
        # TODO (Alex): Ideally we'd also assert that pending_resources
        # eventually became 1 or 2, but it's difficult to do that in a
        # non-racey way. (Perhaps we would need to artificially delay the fake
        # autoscaler node launch?).

    finally:
        cluster.shutdown()


def test_node_launch_exception_serialization(shutdown_only):
    ray.init(num_cpus=1)

    exc_info = None
    try:
        raise Exception("Test exception.")
    except Exception:
        exc_info = sys.exc_info()
    assert exc_info is not None

    exc = NodeLaunchException("cat", "desc", exc_info)

    after_serialization = ray.get(ray.put(exc))

    assert after_serialization.category == exc.category
    assert after_serialization.description == exc.description
    assert after_serialization.src_exc_info is None


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
