from collections import defaultdict
from typing import Dict

import os
import pytest
import sys
import threading
import time
from ray._private.state_api_test_utils import (
    verify_failed_task,
)
from ray.exceptions import RuntimeEnvSetupError
from ray.runtime_env import RuntimeEnv

import ray
from ray._private.test_utils import (
    raw_metrics,
    run_string_as_driver_nonblocking,
    wait_for_condition,
)
from ray.util.state import list_tasks

from ray._private.worker import RayContext

_SYSTEM_CONFIG = {
    "task_events_report_interval_ms": 100,
    "metrics_report_interval_ms": 200,
    "enable_timeline": False,
    "gcs_mark_task_failed_on_job_done_delay_ms": 1000,
}


def aggregate_task_event_metric(info: RayContext) -> Dict:
    """
    Aggregate metrics of task events into:
        {
            "REPORTED": ray_gcs_task_manager_task_events_reported,
            "STORED": ray_gcs_task_manager_task_events_stored,
            "DROPPED_PROFILE_EVENT":
                ray_gcs_task_manager_task_events_dropped PROFILE_EVENT,
            "DROPPED_STATUS_EVENT":
                ray_gcs_task_manager_task_events_dropped STATUS_EVENT,
        }
    """
    res = raw_metrics(info)
    task_events_info = defaultdict(int)
    if "ray_gcs_task_manager_task_events_dropped" in res:
        for sample in res["ray_gcs_task_manager_task_events_dropped"]:
            if "Type" in sample.labels and sample.labels["Type"] != "":
                task_events_info["DROPPED_" + sample.labels["Type"]] += sample.value

    if "ray_gcs_task_manager_task_events_stored" in res:
        for sample in res["ray_gcs_task_manager_task_events_stored"]:
            task_events_info["STORED"] += sample.value

    if "ray_gcs_task_manager_task_events_reported" in res:
        for sample in res["ray_gcs_task_manager_task_events_reported"]:
            task_events_info["REPORTED"] += sample.value

    return task_events_info


def test_status_task_events_metrics(shutdown_only):
    info = ray.init(num_cpus=1, _system_config=_SYSTEM_CONFIG)

    # Start a task
    @ray.remote
    def f():
        pass

    for _ in range(10):
        ray.get(f.remote())

    def verify():
        metric = aggregate_task_event_metric(info)
        assert metric["REPORTED"] >= 10, (
            "At least 10 tasks events should be reported. "
            "Could be more than 10 with multiple flush."
        )
        assert metric["STORED"] == 11, "10 task + 1 driver's events should be stored."

        return True

    wait_for_condition(
        verify,
        timeout=20,
        retry_interval_ms=100,
    )


def test_failed_task_error(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)

    # Test failed task with TASK_EXECUTION_EXCEPTION
    error_msg_str = "fail is expected to fail"

    @ray.remote
    def fail(x=None):
        if x is not None:
            time.sleep(x)
        raise ValueError(error_msg_str)

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(fail.options(name="fail").remote())

    wait_for_condition(
        verify_failed_task,
        name="fail",
        error_type="TASK_EXECUTION_EXCEPTION",
        error_message=error_msg_str,
    )

    # Test canceled tasks with TASK_CANCELLED
    @ray.remote
    def not_running():
        raise ValueError("should not be run")

    with pytest.raises(ray.exceptions.TaskCancelledError):
        t = not_running.options(name="cancel-before-running").remote()
        ray.cancel(t)
        ray.get(t)

    # Cancel task doesn't have additional error message
    wait_for_condition(
        verify_failed_task,
        name="cancel-before-running",
        error_type="TASK_CANCELLED",
        error_message="",
    )

    # Test task failed when worker killed :WORKER_DIED
    @ray.remote(max_retries=0)
    def die():
        exit(27)

    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(die.options(name="die-worker").remote())

    wait_for_condition(
        verify_failed_task,
        name="die-worker",
        error_type="WORKER_DIED",
        error_message="Worker exits with an exit code 27",
    )

    # Test actor task failed with actor dead: ACTOR_DIED
    @ray.remote
    class Actor:
        def f(self):
            time.sleep(999)

        def ready(self):
            pass

    a = Actor.remote()
    ray.get(a.ready.remote())

    with pytest.raises(ray.exceptions.RayActorError):
        ray.kill(a)
        ray.get(a.f.options(name="actor-killed").remote())

    wait_for_condition(
        verify_failed_task,
        name="actor-killed",
        error_type="ACTOR_DIED",
        error_message="The actor is dead because it was killed by `ray.kill`",
    )


def test_failed_task_failed_due_to_node_failure(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)
    ray.init(address=cluster.address)
    node = cluster.add_node(num_cpus=2)

    driver_script = """
import ray
ray.init("auto")

@ray.remote(num_cpus=2, max_retries=0)
def sleep():
    import time
    time.sleep(999)

x = sleep.options(name="node-killed").remote()
ray.get(x)
    """

    run_string_as_driver_nonblocking(driver_script)

    def driver_running():
        t = list_tasks(filters=[("name", "=", "node-killed")])
        return len(t) > 0

    wait_for_condition(driver_running)

    # Kill the node
    cluster.remove_node(node)

    wait_for_condition(
        verify_failed_task,
        name="node-killed",
        error_type="NODE_DIED",
        error_message="Task failed due to the node (where this task was running) "
        " was dead or unavailable",
    )


def test_failed_task_unschedulable(shutdown_only):
    ray.init(num_cpus=1, _system_config=_SYSTEM_CONFIG)

    node_id = ray.get_runtime_context().get_node_id()
    policy = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=node_id,
        soft=False,
    )

    @ray.remote
    def task():
        pass

    task.options(
        scheduling_strategy=policy,
        name="task-unschedulable",
        num_cpus=2,
    ).remote()

    wait_for_condition(
        verify_failed_task,
        name="task-unschedulable",
        error_type="TASK_UNSCHEDULABLE_ERROR",
        error_message=(
            "The node specified via NodeAffinitySchedulingStrategy"
            " doesn't exist any more or is infeasible"
        ),
    )


# TODO(rickyx): Make this work.
# def test_failed_task_removed_placement_group(shutdown_only, monkeypatch):
#     ray.init(num_cpus=2, _system_config=_SYSTEM_CONFIG)
#     from ray.util.placement_group import placement_group, remove_placement_group
#     from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
#
#     pg = placement_group([{"CPU": 2}])
#     ray.get(pg.ready())
#
#     @ray.remote(num_cpus=2)
#     def sleep():
#         time.sleep(999)
#
#     with monkeypatch.context() as m:
#         m.setenv(
#             "RAY_testing_asio_delay_us",
#             "NodeManagerService.grpc_server.RequestWorkerLease=3000000:3000000",
#         )
#
#         sleep.options(
#             scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg),
#             name="task-pg-removed",
#             max_retries=0,
#         ).remote()
#
#     remove_placement_group(pg)
#
#     wait_for_condition(
#         verify_failed_task,
#         name="task-pg-removed",
#         error_type="TASK_PLACEMENT_GROUP_REMOVED",
#     )


def test_failed_task_runtime_env_setup(shutdown_only):
    import conda

    @ray.remote
    def f():
        pass

    bad_env = RuntimeEnv(conda={"dependencies": ["_this_does_not_exist"]})
    with pytest.raises(
        RuntimeEnvSetupError,
    ):
        ray.get(f.options(runtime_env=bad_env, name="task-runtime-env-failed").remote())

    conda_major_version = int(conda.__version__.split(".")[0])
    error_message = (
        "PackagesNotFoundError"
        if conda_major_version >= 24
        else "ResolvePackageNotFound"
    )
    wait_for_condition(
        verify_failed_task,
        name="task-runtime-env-failed",
        error_type="RUNTIME_ENV_SETUP_FAILED",
        error_message=error_message,
    )


def test_parent_task_id_threaded_task(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)

    # Task starts a thread
    @ray.remote
    def main_task():
        def thd_task():
            @ray.remote
            def thd_task():
                pass

            ray.get(thd_task.remote())

        thd = threading.Thread(target=thd_task)
        thd.start()
        thd.join()

    ray.get(main_task.remote())

    def verify():
        tasks = list_tasks()
        assert len(tasks) == 2
        expect_parent_task_id = None
        actual_parent_task_id = None
        for task in tasks:
            if task["name"] == "main_task":
                expect_parent_task_id = task["task_id"]
            elif task["name"] == "thd_task":
                actual_parent_task_id = task["parent_task_id"]
        assert actual_parent_task_id is not None
        assert expect_parent_task_id == actual_parent_task_id

        return True

    wait_for_condition(verify)


def test_parent_task_id_non_concurrent_actor(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)

    def run_task_in_thread():
        def thd_task():
            @ray.remote
            def thd_task():
                pass

            ray.get(thd_task.remote())

        thd = threading.Thread(target=thd_task)
        thd.start()
        thd.join()

    @ray.remote
    class Actor:
        def main_task(self):
            run_task_in_thread()

    a = Actor.remote()
    ray.get(a.main_task.remote())

    def verify():
        tasks = list_tasks()
        expect_parent_task_id = None
        actual_parent_task_id = None
        for task in tasks:
            if "main_task" in task["name"]:
                expect_parent_task_id = task["task_id"]
            elif "thd_task" in task["name"]:
                actual_parent_task_id = task["parent_task_id"]
        print(tasks)
        assert actual_parent_task_id is not None
        assert expect_parent_task_id == actual_parent_task_id

        return True

    wait_for_condition(verify)


@pytest.mark.parametrize("actor_concurrency", [3, 10])
def test_parent_task_id_concurrent_actor(shutdown_only, actor_concurrency):
    # Test tasks runs in user started thread from actors have a parent_task_id
    # as the actor's creation task.
    ray.init(_system_config=_SYSTEM_CONFIG)

    def run_task_in_thread(name, i):
        def thd_task():
            @ray.remote
            def thd_task():
                pass

            ray.get(thd_task.options(name=f"{name}_{i}").remote())

        thd = threading.Thread(target=thd_task)
        thd.start()
        thd.join()

    @ray.remote
    class AsyncActor:
        async def main_task(self, i):
            run_task_in_thread("async_thd_task", i)

    @ray.remote
    class ThreadedActor:
        def main_task(self, i):
            run_task_in_thread("threaded_thd_task", i)

    def verify(actor_method_name, actor_class_name):
        tasks = list_tasks()
        print(tasks)
        expect_parent_task_id = None
        actual_parent_task_id = None
        for task in tasks:
            if f"{actor_class_name}.__init__" in task["name"]:
                expect_parent_task_id = task["task_id"]

        assert expect_parent_task_id is not None
        for task in tasks:
            if f"{actor_method_name}" in task["name"]:
                actual_parent_task_id = task["parent_task_id"]
                assert expect_parent_task_id == actual_parent_task_id, task

        return True

    async_actor = AsyncActor.options(max_concurrency=actor_concurrency).remote()
    ray.get([async_actor.main_task.remote(i) for i in range(20)])
    wait_for_condition(
        verify, actor_class_name="AsyncActor", actor_method_name="async_thd_task"
    )

    thd_actor = ThreadedActor.options(max_concurrency=actor_concurrency).remote()
    ray.get([thd_actor.main_task.remote(i) for i in range(20)])
    wait_for_condition(
        verify, actor_class_name="ThreadedActor", actor_method_name="threaded_thd_task"
    )


def test_is_debugger_paused(shutdown_only):
    ray.init(num_cpus=1, _system_config=_SYSTEM_CONFIG)

    @ray.remote(max_retries=0)
    def f():
        import time

        # Pause 5 seconds inside debugger
        with ray._private.worker.global_worker.task_paused_by_debugger():
            time.sleep(5)

    def verify(num_paused):
        tasks = list_tasks(filters=[("is_debugger_paused", "=", "True")])
        return len(tasks) == num_paused

    f_task = f.remote()  # noqa

    wait_for_condition(
        verify,
        timeout=20,
        retry_interval_ms=100,
        num_paused=1,
    )

    wait_for_condition(
        verify,
        timeout=20,
        retry_interval_ms=100,
        num_paused=0,
    )


@pytest.mark.parametrize("actor_concurrency", [1, 3])
def test_is_debugger_paused_actor(shutdown_only, actor_concurrency):
    ray.init(_system_config=_SYSTEM_CONFIG)

    @ray.remote
    class TestActor:
        def main_task(self, i):
            if i == 0:
                import time

                # Pause 5 seconds inside debugger
                with ray._private.worker.global_worker.task_paused_by_debugger():
                    time.sleep(5)

    def verify(expected_task_name):
        tasks = list_tasks(filters=[("is_debugger_paused", "=", "True")])
        return len(tasks) == 1 and f"{expected_task_name}_0" in tasks[0]["name"]

    test_actor = TestActor.options(max_concurrency=actor_concurrency).remote()
    refs = [  # noqa
        test_actor.main_task.options(name=f"TestActor.main_task_{i}").remote(i)
        for i in range(20)
    ]

    wait_for_condition(verify, expected_task_name="TestActor.main_task")


@pytest.mark.parametrize("actor_concurrency", [1, 3])
def test_is_debugger_paused_threaded_actor(shutdown_only, actor_concurrency):
    ray.init(_system_config=_SYSTEM_CONFIG)

    @ray.remote
    class ThreadedActor:
        def main_task(self, i):
            def thd_task():
                @ray.remote
                def thd_task():
                    if i == 0:
                        import time

                        # Pause 5 seconds inside debugger
                        with ray._private.worker.global_worker.task_paused_by_debugger():  # noqa: E501
                            time.sleep(5)

                ray.get(thd_task.options(name=f"ThreadedActor.main_task_{i}").remote())

            thd = threading.Thread(target=thd_task)
            thd.start()
            thd.join()

    def verify(expected_task_name):
        tasks = list_tasks(filters=[("is_debugger_paused", "=", "True")])
        return len(tasks) == 1 and f"{expected_task_name}_0" in tasks[0]["name"]

    threaded_actor = ThreadedActor.options(max_concurrency=actor_concurrency).remote()
    refs = [  # noqa
        threaded_actor.main_task.options(name=f"ThreadedActor.main_task_{i}").remote(i)
        for i in range(20)
    ]

    wait_for_condition(verify, expected_task_name="ThreadedActor.main_task")


@pytest.mark.parametrize("actor_concurrency", [1, 3])
def test_is_debugger_paused_async_actor(shutdown_only, actor_concurrency):
    ray.init(_system_config=_SYSTEM_CONFIG)

    @ray.remote
    class AsyncActor:
        async def main_task(self, i):
            if i == 0:
                import time

                # Pause 5 seconds inside debugger
                print()
                with ray._private.worker.global_worker.task_paused_by_debugger():
                    time.sleep(5)

    def verify(expected_task_name):
        tasks = list_tasks(filters=[("is_debugger_paused", "=", "True")])
        print(tasks)
        return len(tasks) == 1 and f"{expected_task_name}_0" in tasks[0]["name"]

    async_actor = AsyncActor.options(max_concurrency=actor_concurrency).remote()
    refs = [  # noqa
        async_actor.main_task.options(name=f"AsyncActor.main_task_{i}").remote(i)
        for i in range(20)
    ]

    wait_for_condition(verify, expected_task_name="AsyncActor.main_task")


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
