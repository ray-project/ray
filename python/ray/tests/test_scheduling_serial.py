import platform
import pytest
import sys
import os
import ray
import ray._private.gcs_utils as gcs_utils
import time
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray._private.test_utils import wait_for_condition, make_global_state_accessor


@pytest.mark.skipif(
    platform.system() == "Windows", reason="FakeAutoscaler doesn't work on Windows"
)
def test_demand_report_for_node_affinity_scheduling_strategy(
    monkeypatch, shutdown_only
):
    monkeypatch.setenv("RAY_num_heartbeats_timeout", "4")
    from ray.cluster_utils import AutoscalingCluster

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 1,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 1,
                "max_workers": 1,
            },
        },
    )

    cluster.start()
    info = ray.init("auto")

    @ray.remote(num_cpus=1)
    def f(sleep_s):
        time.sleep(sleep_s)
        return ray.get_runtime_context().node_id

    worker_node_id = ray.get(f.remote(0))

    tasks = []
    tasks.append(f.remote(10000))
    # This is not reported since there is feasible node.
    tasks.append(
        f.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                worker_node_id, soft=False
            )
        ).remote(0)
    )
    # This is reported since there is no feasible node and soft is True.
    tasks.append(
        f.options(
            num_gpus=1,
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                ray.NodeID.from_random().hex(), soft=True
            ),
        ).remote(0)
    )

    global_state_accessor = make_global_state_accessor(info)

    def check_resource_demand():
        message = global_state_accessor.get_all_resource_usage()
        if message is None:
            return False

        resource_usage = gcs_utils.ResourceUsageBatchData.FromString(message)
        aggregate_resource_load = resource_usage.resource_load_by_shape.resource_demands

        if len(aggregate_resource_load) != 1:
            return False

        if aggregate_resource_load[0].num_infeasible_requests_queued != 1:
            return False

        if aggregate_resource_load[0].shape != {"CPU": 1.0, "GPU": 1.0}:
            return False

        return True

    wait_for_condition(check_resource_demand, 20)
    cluster.shutdown()


@pytest.mark.skipif(
    platform.system() == "Windows", reason="FakeAutoscaler doesn't work on Windows"
)
@pytest.mark.skipif(os.environ.get("ASAN_OPTIONS") is not None, reason="ASAN is slow")
def test_demand_report_when_scale_up(shutdown_only):
    # https://github.com/ray-project/ray/issues/22122
    from ray.cluster_utils import AutoscalingCluster

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "cpu_node": {
                "resources": {
                    "CPU": 1,
                    "object_store_memory": 1024 * 1024 * 1024,
                },
                "node_config": {},
                "min_workers": 10,
                "max_workers": 10,
            },
        },
    )

    cluster.start()

    info = ray.init("auto")

    @ray.remote
    def f():
        time.sleep(10000)

    @ray.remote
    def g():
        ray.get(h.remote())

    @ray.remote
    def h():
        time.sleep(10000)

    tasks = [f.remote() for _ in range(5000)].extend(  # noqa: F841
        [g.remote() for _ in range(5000)]
    )

    global_state_accessor = make_global_state_accessor(info)

    def check_backlog_info():
        message = global_state_accessor.get_all_resource_usage()
        if message is None:
            return 0

        resource_usage = gcs_utils.ResourceUsageBatchData.FromString(message)
        aggregate_resource_load = resource_usage.resource_load_by_shape.resource_demands

        if len(aggregate_resource_load) != 1:
            return False

        (backlog_size, num_ready_requests_queued, shape) = (
            aggregate_resource_load[0].backlog_size,
            aggregate_resource_load[0].num_ready_requests_queued,
            aggregate_resource_load[0].shape,
        )
        if backlog_size + num_ready_requests_queued != 9990:
            return False

        if shape != {"CPU": 1.0}:
            return False
        return True

    # In ASAN test it's slow.
    # Wait for 20s for the cluster to be up
    wait_for_condition(check_backlog_info, 20)
    cluster.shutdown()


if __name__ == "__main__":

    # TODO: Make it run parallel
    # sys.exit(pytest.main(["-n", "auto", "--boxed", "-vx", __file__]))
    sys.exit(pytest.main(["-v", __file__]))
