import subprocess
from ray.autoscaler._private.constants import AUTOSCALER_METRIC_PORT

import pytest
import ray
import sys
from ray._private.test_utils import (
    wait_for_condition,
    get_metric_check_condition,
    MetricSamplePattern,
    SignalActor,
)
from ray.autoscaler.node_launch_exception import NodeLaunchException


@pytest.mark.parametrize(
    "local_autoscaling_cluster",
    [
        (
            {"CPU": 0},
            {
                "type-i": {
                    "resources": {"CPU": 4, "fun": 1},
                    "node_config": {},
                    "min_workers": 1,
                    "max_workers": 1,
                },
                "type-ii": {
                    "resources": {"CPU": 3, "fun": 100},
                    "node_config": {},
                    "min_workers": 1,
                    "max_workers": 1,
                },
            },
            None,
        )
    ],
    indirect=["local_autoscaling_cluster"],
)
@pytest.mark.parametrize("enable_v2", [True, False], ids=["v2", "v1"])
def test_ray_status_activity(local_autoscaling_cluster, shutdown_only, enable_v2):

    ray.init(address="auto")
    if enable_v2:
        assert (
            subprocess.check_output("ray status --verbose", shell=True)
            .decode()
            .count("Idle: ")
            > 0
        )

    @ray.remote(num_cpus=2, resources={"fun": 2})
    class Actor:
        def ping(self):
            return None

    actor = Actor.remote()
    ray.get(actor.ping.remote())

    occurrences = 1 if enable_v2 else 0
    assert (
        subprocess.check_output("ray status --verbose", shell=True)
        .decode()
        .count("Resource: CPU currently in use.")
        == occurrences
    )

    from ray.util.placement_group import placement_group

    pg = placement_group([{"CPU": 2}], strategy="STRICT_SPREAD")
    ray.get(pg.ready())

    occurrences = 2 if enable_v2 else 0
    assert (
        subprocess.check_output("ray status --verbose", shell=True)
        .decode()
        .count("Resource: CPU currently in use.")
        == occurrences
    )

    assert (
        subprocess.check_output("ray status --verbose", shell=True)
        .decode()
        .count("Resource: bundle_group_")
        == 0
    )


@pytest.mark.parametrize(
    "local_autoscaling_cluster",
    [
        (
            {"CPU": 0},
            {
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
            None,
        )
    ],
    indirect=["local_autoscaling_cluster"],
)
@pytest.mark.parametrize("enable_v2", [True, False], ids=["v2", "v1"])
def test_ray_status_e2e(local_autoscaling_cluster, shutdown_only):

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
        "Total Demands" in subprocess.check_output("ray status -v", shell=True).decode()
    )
    assert (
        "Total Demands"
        in subprocess.check_output("ray status --verbose", shell=True).decode()
    )


@pytest.mark.parametrize(
    "local_autoscaling_cluster",
    [
        (
            {"CPU": 0},
            {
                "type-i": {
                    "resources": {"CPU": 1},
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 1,
                },
                "type-ii": {
                    "resources": {"CPU": 1},
                    "node_config": {},
                    "min_workers": 0,
                    "max_workers": 1,
                },
            },
            None,
        )
    ],
    indirect=["local_autoscaling_cluster"],
)
@pytest.mark.parametrize("enable_v2", [False, True], ids=["v2", "v1"])
def test_metrics(local_autoscaling_cluster, shutdown_only):

    info = ray.init(address="auto")
    autoscaler_export_addr = "{}:{}".format(
        info.address_info["node_ip_address"], AUTOSCALER_METRIC_PORT
    )

    @ray.remote(num_cpus=1)
    class Foo:
        def ping(self):
            return True

    zero_reported_condition = get_metric_check_condition(
        [
            MetricSamplePattern(
                name="autoscaler_cluster_resources",
                value=0,
                partial_label_match={"resource": "CPU"},
            ),
            MetricSamplePattern(name="autoscaler_pending_resources", value=0),
            MetricSamplePattern(name="autoscaler_pending_nodes", value=0),
            MetricSamplePattern(
                name="autoscaler_active_nodes",
                value=0,
                partial_label_match={"NodeType": "type-i"},
            ),
            MetricSamplePattern(
                name="autoscaler_active_nodes",
                value=0,
                partial_label_match={"NodeType": "type-ii"},
            ),
            MetricSamplePattern(
                name="autoscaler_active_nodes",
                value=1,
                partial_label_match={"NodeType": "ray.head.default"},
            ),
        ],
        export_addr=autoscaler_export_addr,
    )
    wait_for_condition(zero_reported_condition)

    actors = [Foo.remote() for _ in range(2)]
    ray.get([actor.ping.remote() for actor in actors])

    two_cpu_no_pending_condition = get_metric_check_condition(
        [
            MetricSamplePattern(
                name="autoscaler_cluster_resources",
                value=2,
                partial_label_match={"resource": "CPU"},
            ),
            MetricSamplePattern(
                name="autoscaler_pending_nodes",
                value=0,
                partial_label_match={"NodeType": "type-i"},
            ),
            MetricSamplePattern(
                name="autoscaler_pending_nodes",
                value=0,
                partial_label_match={"NodeType": "type-ii"},
            ),
            MetricSamplePattern(
                name="autoscaler_active_nodes",
                value=1,
                partial_label_match={"NodeType": "type-i"},
            ),
            MetricSamplePattern(
                name="autoscaler_active_nodes",
                value=1,
                partial_label_match={"NodeType": "type-ii"},
            ),
            MetricSamplePattern(
                name="autoscaler_active_nodes",
                value=1,
                partial_label_match={"NodeType": "ray.head.default"},
            ),
        ],
        export_addr=autoscaler_export_addr,
    )
    wait_for_condition(two_cpu_no_pending_condition)
    # TODO (Alex): Ideally we'd also assert that pending increases
    # eventually became 1 or 2, but it's difficult to do that in a
    # non-racey way. (Perhaps we would need to artificially delay the fake
    # autoscaler node launch?).


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


@pytest.mark.parametrize(
    "local_autoscaling_cluster",
    [
        (
            {"CPU": 0},
            {
                "type-i": {
                    "resources": {"CPU": 1},
                    "node_config": {},
                    "min_workers": 1,
                    "max_workers": 1,
                },
            },
            {"enable_infeasible_task_early_exit": True},
        )
    ],
    indirect=["local_autoscaling_cluster"],
)
@pytest.mark.parametrize("enable_v2", [True], ids=["v2"])
def test_infeasible_task_early_cancellation_normal_tasks(
    local_autoscaling_cluster, shutdown_only
):

    ray.init(address="auto")

    signal = SignalActor.remote()

    @ray.remote(num_cpus=1)
    def feasible_task():
        signal.wait.remote()
        return 1

    @ray.remote(num_cpus=10)
    def infeasible_task():
        return 2

    obj_feasible = feasible_task.remote()
    obj_infeasible = infeasible_task.remote()

    # The infeasible task should be cancelled with TaskUnschedulableError
    with pytest.raises(
        ray.exceptions.TaskUnschedulableError,
        match=r"The task is not schedulable: Tasks or actors with resource shapes \[{CPU: 10}] failed to schedule because there are not enough resources for the tasks or actors on the whole cluster.",
    ):
        ray.get(obj_infeasible, timeout=10)

    # The feasible task should continue to run successfully
    signal.send.remote()
    assert ray.get(obj_feasible, timeout=5) == 1


@pytest.mark.parametrize(
    "local_autoscaling_cluster",
    [
        (
            {"CPU": 0},
            {
                "type-i": {
                    "resources": {"CPU": 1},
                    "node_config": {},
                    "min_workers": 1,
                    "max_workers": 1,
                },
            },
            {"enable_infeasible_task_early_exit": True},
        )
    ],
    indirect=["local_autoscaling_cluster"],
)
@pytest.mark.parametrize("enable_v2", [True], ids=["v2"])
def test_infeasible_task_early_cancellation_actor_creation(
    local_autoscaling_cluster, shutdown_only
):

    ray.init(address="auto")

    signal = SignalActor.remote()

    @ray.remote(num_cpus=1)
    class FeasibleActor:
        def f(self):
            signal.wait.remote()
            return 1

    @ray.remote(num_cpus=10)
    class InfeasibleActor:
        def f(self):
            return 2

    feasible_actor = FeasibleActor.remote()
    infeasible_actor = InfeasibleActor.remote()

    # The infeasible actor should be cancelled with ActorUnschedulableError
    with pytest.raises(
        ray.exceptions.ActorUnschedulableError,
        match=r"The actor is not schedulable: Tasks or actors with resource shapes \[{CPU: 10}] failed to schedule because there are not enough resources for the tasks or actors on the whole cluster.",
    ):
        ray.get(infeasible_actor.f.remote(), timeout=5)

    # The feasible actor should continue to run successfully
    signal.send.remote()
    assert ray.get(feasible_actor.f.remote(), timeout=5) == 1


if __name__ == "__main__":
    import os
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
