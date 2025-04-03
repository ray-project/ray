import asyncio
from collections import defaultdict
import os
from typing import Dict
import pytest
import sys
import time
from ray._private import ray_constants
from functools import reduce

import ray
from ray._private.state_api_test_utils import (
    PidActor,
    get_state_api_manager,
    verify_tasks_running_or_terminated,
    verify_failed_task,
    _is_actor_task_running,
)
from ray.util.state.common import ListApiOptions, StateResource
from ray._private.test_utils import (
    async_wait_for_condition,
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_condition,
)
from ray.util.state import (
    StateApiClient,
    list_actors,
    list_jobs,
    list_tasks,
)
import psutil

_SYSTEM_CONFIG = {
    "task_events_report_interval_ms": 100,
    "metrics_report_interval_ms": 200,
    "enable_timeline": False,
    "gcs_mark_task_failed_on_job_done_delay_ms": 1000,
    "gcs_mark_task_failed_on_worker_dead_delay_ms": 1000,
}


@ray.remote
class ActorOk:
    def ready(self):
        pass


@ray.remote
class ActorInitFailed:
    def __init__(self):
        raise ValueError("Actor init is expected to fail")

    def ready(self):
        pass


def test_actor_creation_task_ok(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)
    a = ActorOk.remote()
    ray.get(a.ready.remote())

    def verify():
        tasks = list_tasks(filters=[("name", "=", "ActorOk.__init__")])
        actors = list_actors(filters=[("class_name", "=", "ActorOk")])

        assert len(tasks) == 1
        assert len(actors) == 1
        actor = actors[0]
        task = tasks[0]
        assert task["state"] == "FINISHED"
        assert task["actor_id"] == actor["actor_id"]
        return True

    wait_for_condition(verify)


def test_actor_creation_task_failed(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)
    a = ActorInitFailed.remote()

    with pytest.raises(ray.exceptions.RayActorError):
        ray.get(a.ready.remote())

    def verify():
        tasks = list_tasks(filters=[("name", "=", "ActorInitFailed.__init__")])
        actors = list_actors(filters=[("class_name", "=", "ActorInitFailed")])

        assert len(tasks) == 1
        assert len(actors) == 1
        actor = actors[0]
        task = tasks[0]
        assert task["state"] == "FAILED"
        assert task["actor_id"] == actor["actor_id"]
        assert actor["state"] == "DEAD"
        return True

    wait_for_condition(verify)


def test_actor_creation_nested_failure_from_actor(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)

    @ray.remote
    class NestedActor:
        def ready(self):
            a = ActorInitFailed.remote()
            ray.get(a.ready.remote())

    a = NestedActor.remote()

    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(a.ready.remote())

    def verify():
        creation_tasks = list_tasks(filters=[("type", "=", "ACTOR_CREATION_TASK")])
        actors = list_actors()

        assert len(creation_tasks) == 2
        assert len(actors) == 2
        for actor in actors:
            if "NestedActor" in actor["class_name"]:
                assert actor["state"] == "ALIVE"
            else:
                assert "ActorInitFailed" in actor["class_name"]
                assert actor["state"] == "DEAD"

        for task in creation_tasks:
            if "ActorInitFailed" in task["name"]:
                assert task["state"] == "FAILED"
            else:
                assert task["name"] == "NestedActor.__init__"
                assert task["state"] == "FINISHED"
        return True

    wait_for_condition(verify)


def test_actor_creation_canceled(shutdown_only):
    ray.init(num_cpus=2, _system_config=_SYSTEM_CONFIG)

    # An actor not gonna be scheduled
    a = ActorOk.options(num_cpus=10).remote()

    # Kill it before it could be scheduled.
    ray.kill(a)

    def verify():
        tasks = list_tasks(filters=[("name", "=", "ActorOk.__init__")])
        actors = list_actors(filters=[("class_name", "=", "ActorOk")])

        assert len(tasks) == 1
        assert len(actors) == 1
        actor = actors[0]
        task = tasks[0]
        assert task["state"] == "FAILED"
        assert task["actor_id"] == actor["actor_id"]
        assert actor["state"] == "DEAD"
        return True

    wait_for_condition(verify)


def test_handle_driver_tasks(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)

    job_id = ray.get_runtime_context().get_job_id()
    script = """
import ray
import time
ray.init("auto")

@ray.remote
def f():
    time.sleep(3)


ray.get(f.remote())
"""
    run_string_as_driver_nonblocking(script)

    client = StateApiClient()

    def list_tasks(exclude_driver):
        return client.list(
            StateResource.TASKS,
            # Filter out this driver
            options=ListApiOptions(
                exclude_driver=exclude_driver, filters=[("job_id", "!=", job_id)]
            ),
            raise_on_missing_output=True,
        )

    # Check driver running
    def verify():
        tasks_with_driver = list_tasks(exclude_driver=False)
        assert len(tasks_with_driver) == 2, tasks_with_driver
        task_types = {task["type"] for task in tasks_with_driver}
        assert task_types == {"NORMAL_TASK", "DRIVER_TASK"}

        for task in tasks_with_driver:
            if task["type"] == "DRIVER_TASK":
                assert task["state"] == "RUNNING", task

        return True

    wait_for_condition(verify, timeout=15, retry_interval_ms=1000)

    # Check driver finishes
    def verify():
        tasks_with_driver = list_tasks(exclude_driver=False)
        assert len(tasks_with_driver) == 2, tasks_with_driver
        for task in tasks_with_driver:
            if task["type"] == "DRIVER_TASK":
                assert task["state"] == "FINISHED", task

        tasks_no_driver = list_tasks(exclude_driver=True)
        assert len(tasks_no_driver) == 1, tasks_no_driver
        return True

    wait_for_condition(verify)


def test_fault_tolerance_detached_actor(shutdown_only):
    """
    Tests that tasks from a detached actor **shouldn't** be marked as failed
    """
    ray.init(_system_config=_SYSTEM_CONFIG)

    pid_actor = PidActor.remote()

    # Check a detached actor's parent task's failure do not
    # affect the actor's task subtree.
    @ray.remote(max_retries=0)
    def parent_starts_detached_actor(pid_actor):
        @ray.remote
        class DetachedActor:
            def __init__(self):
                pass

            async def running(self):
                while not self.running:
                    await asyncio.sleep(0.1)
                pass

            async def run(self, pid_actor):
                ray.get(
                    pid_actor.report_pid.remote(
                        "detached-actor-run", os.getpid(), "RUNNING"
                    )
                )
                self.running = True
                await asyncio.sleep(999)

        # start a detached actor
        a = DetachedActor.options(
            name="detached-actor", lifetime="detached", namespace="test"
        ).remote()
        a.run.options(name="detached-actor-run").remote(pid_actor)
        ray.get(a.running.remote())

        # Enough time for events to be reported to GCS.
        time.sleep(1)
        # fail this parent task
        os._exit(1)

    with pytest.raises(ray.exceptions.WorkerCrashedError):
        ray.get(parent_starts_detached_actor.remote(pid_actor))

    a = ray.get_actor("detached-actor", namespace="test")
    task_pids = ray.get(pid_actor.get_pids.remote())
    wait_for_condition(
        verify_tasks_running_or_terminated,
        task_pids=task_pids,
        expect_num_tasks=1,
    )

    a = ray.get_actor("detached-actor", namespace="test")
    ray.kill(a)

    # Verify the actual process no longer running.
    task_pids["detached-actor-run"] = (task_pids["detached-actor-run"][0], "FAILED")
    wait_for_condition(
        verify_tasks_running_or_terminated,
        task_pids=task_pids,
        expect_num_tasks=1,
    )

    # Verify failed task marked with expected info.
    wait_for_condition(
        verify_failed_task,
        name="detached-actor-run",
        error_type="WORKER_DIED",
        error_message="The actor is dead because it was killed by `ray.kill`",
    )


def test_fault_tolerance_job_failed(shutdown_only):
    sys_config = _SYSTEM_CONFIG.copy()
    config = {
        "gcs_mark_task_failed_on_job_done_delay_ms": 1000,
        # make worker failure not trigger task failure
        "gcs_mark_task_failed_on_worker_dead_delay_ms": 30000,
    }
    sys_config.update(config)
    ray.init(num_cpus=8, _system_config=sys_config)
    script = """
import ray
import time

ray.init("auto")
NUM_CHILD = 2

@ray.remote
def grandchild():
    time.sleep(999)

@ray.remote
def child():
    ray.get(grandchild.remote())

@ray.remote
def finished_child():
    ray.put(1)
    return

@ray.remote
def parent():
    children = [child.remote() for _ in range(NUM_CHILD)]
    finished_children = ray.get([finished_child.remote() for _ in range(NUM_CHILD)])
    ray.get(children)

ray.get(parent.remote())

"""
    proc = run_string_as_driver_nonblocking(script)

    def all_tasks_running():
        tasks = list_tasks()
        assert len(tasks) == 7, (
            "Incorrect number of tasks are reported. "
            "Expected length: 1 parent + 2 finished child +  2 failed child + "
            "2 failed grandchild tasks"
        )
        return True

    wait_for_condition(
        all_tasks_running,
        timeout=10,
        retry_interval_ms=500,
    )
    time_sleep_s = 3
    # Sleep for a while to allow driver job runs async.
    time.sleep(time_sleep_s)

    proc.kill()

    def verify():
        tasks = list_tasks(detail=True)
        assert len(tasks) == 7, (
            "Incorrect number of tasks are reported. "
            "Expected length: 1 parent + 2 finished child +  2 failed child + "
            "2 failed grandchild tasks"
        )
        for task in tasks:
            if "finished" in task["func_or_class_name"]:
                assert (
                    task["state"] == "FINISHED"
                ), f"task {task['func_or_class_name']} has wrong state"
            else:
                assert (
                    task["state"] == "FAILED"
                ), f"task {task['func_or_class_name']} has wrong state"

                assert task["error_type"] == "WORKER_DIED"
                assert "Job finishes" in task["error_message"]

                duration_ms = task["end_time_ms"] - task["start_time_ms"]
                assert (
                    # It takes time for the job to run
                    duration_ms > time_sleep_s / 2 * 1000
                    and duration_ms < 2 * time_sleep_s * 1000
                )

        return True

    wait_for_condition(
        verify,
        timeout=10,
        retry_interval_ms=500,
    )


@ray.remote
def task_finish_child(pid_actor):
    ray.get(pid_actor.report_pid.remote("task_finish_child", os.getpid(), "FINISHED"))
    pass


@ray.remote
def task_sleep_child(pid_actor):
    ray.get(pid_actor.report_pid.remote("task_sleep_child", os.getpid()))
    time.sleep(999)


@ray.remote
class ChildActor:
    def children(self, pid_actor):
        ray.get(pid_actor.report_pid.remote("children", os.getpid()))
        ray.get(task_finish_child.options(name="task_finish_child").remote(pid_actor))
        ray.get(task_sleep_child.options(name="task_sleep_child").remote(pid_actor))


@ray.remote
class Actor:
    def fail_parent(self, pid_actor):
        ray.get(pid_actor.report_pid.remote("fail_parent", os.getpid(), "FAILED"))
        ray.get(task_finish_child.options(name="task_finish_child").remote(pid_actor))
        task_sleep_child.options(name="task_sleep_child").remote(pid_actor)

        # Wait til child tasks run.
        def wait_fn():
            assert (
                ray.get(pid_actor.get_pids.remote()).get("task_sleep_child") is not None
            )
            assert (
                list_tasks(filters=[("name", "=", "task_finish_child")])[0]["state"]
                == "FINISHED"
            )
            return True

        wait_for_condition(wait_fn)
        raise ValueError("expected to fail.")

    def child_actor(self, pid_actor):
        ray.get(pid_actor.report_pid.remote("child_actor", os.getpid(), "FAILED"))
        a = ChildActor.remote()
        a.children.options(name="children").remote(pid_actor)
        # Wait til child tasks run.
        wait_for_condition(
            lambda: ray.get(pid_actor.get_pids.remote()).get("task_sleep_child")
            is not None
        )
        raise ValueError("expected to fail.")

    def ready(self):
        pass


def test_fault_tolerance_actor_tasks_failed(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)
    # Test actor tasks
    pid_actor = PidActor.remote()
    with pytest.raises(ray.exceptions.RayTaskError):
        a = Actor.remote()
        ray.get(a.ready.remote())
        ray.get(a.fail_parent.options(name="fail_parent").remote(pid_actor))

    # Wait for all tasks to finish:
    # 3 = fail_parent + task_finish_child + task_sleep_child
    wait_for_condition(
        verify_tasks_running_or_terminated,
        task_pids=ray.get(pid_actor.get_pids.remote()),
        expect_num_tasks=3,
    )


def test_fault_tolerance_nested_actors_failed(shutdown_only):
    ray.init(_system_config=_SYSTEM_CONFIG)
    pid_actor = PidActor.remote()
    # Test nested actor tasks
    with pytest.raises(ray.exceptions.RayTaskError):
        a = Actor.remote()
        ray.get(a.ready.remote())
        ray.get(a.child_actor.options(name="child_actor").remote(pid_actor))

    # Wait for all tasks to finish:
    # 4 = child_actor + children + task_finish_child + task_sleep_child
    wait_for_condition(
        verify_tasks_running_or_terminated,
        task_pids=ray.get(pid_actor.get_pids.remote()),
        expect_num_tasks=4,
    )


def test_ray_intentional_errors(shutdown_only):
    """
    Test in the below cases, ray task should not be marked as failure:
    1. ray.actor_exit_actor()
    2. __ray_terminate__.remote()
    3. max calls reached.
    4. task that exit with exit(0)
    """

    # Test `exit_actor`
    @ray.remote
    class Actor:
        def ready(self):
            pass

        def exit(self):
            ray.actor.exit_actor()

        def exit_normal(self):
            exit(0)

    ray.init(num_cpus=1)

    a = Actor.remote()
    ray.get(a.ready.remote())

    a.exit.remote()

    def verify():
        ts = list_tasks(filters=[("name", "=", "Actor.exit")])
        assert len(ts) == 1
        t = ts[0]

        assert t["state"] == "FINISHED"
        return True

    wait_for_condition(verify)

    # Test `__ray_terminate__`
    b = Actor.remote()

    ray.get(b.ready.remote())

    b.__ray_terminate__.remote()

    def verify():
        ts = list_tasks(filters=[("name", "=", "Actor.__ray_terminate__")])
        assert len(ts) == 1
        t = ts[0]

        assert t["state"] == "FINISHED"
        return True

    wait_for_condition(verify)

    # Test max calls reached exiting workers should not fail the task.
    @ray.remote(max_calls=1)
    def f():
        pass

    for _ in range(3):
        ray.get(f.remote())

    def verify():
        ts = list_tasks(filters=[("name", "=", "f")])
        assert len(ts) == 3
        workers = set()
        for t in ts:
            assert t["state"] == "FINISHED"
            workers.add(t["worker_id"])

        assert len(workers) == 3
        return True

    wait_for_condition(verify)

    # Test tasks that fail with exit(0)
    @ray.remote
    def g():
        exit(0)

    def verify():
        ts = list_tasks(filters=[("name", "=", "g")])
        assert len(ts) == 1
        t = ts[0]

        assert t["state"] == "FINISHED"
        return True

    c = Actor.remote()
    ray.get(c.ready.remote())

    c.exit_normal.remote()

    def verify():
        ts = list_tasks(filters=[("name", "=", "Actor.exit_normal")])
        assert len(ts) == 1
        t = ts[0]

        assert t["state"] == "FINISHED"
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize(
    "exit_type",
    ["exit_kill", "exit_exception"],
)
@pytest.mark.parametrize(
    "actor_or_normal_tasks",
    ["normal_task", "actor_task"],
)
def test_fault_tolerance_chained_task_fail(
    shutdown_only, exit_type, actor_or_normal_tasks
):
    ray.init(_system_config=_SYSTEM_CONFIG)

    def sleep_or_fail(pid_actor=None, exit_type=None):
        if exit_type is None:
            time.sleep(999)
        # Wait until the children run
        if pid_actor:
            wait_for_condition(
                lambda: len(ray.get(pid_actor.get_pids.remote())) == 3,
            )

        if exit_type == "exit_kill":
            os._exit(1)
        else:
            raise ValueError("Expected to fail")

    # Test a chain of tasks
    @ray.remote(max_retries=0)
    def A(exit_type, pid_actor):
        x = B.remote(pid_actor)
        ray.get(pid_actor.report_pid.remote("A", os.getpid()))
        sleep_or_fail(pid_actor, exit_type)
        ray.get(x)

    @ray.remote(max_retries=0)
    def B(pid_actor):
        x = C.remote(pid_actor)
        ray.get(pid_actor.report_pid.remote("B", os.getpid()))
        sleep_or_fail()
        ray.get(x)

    @ray.remote(max_retries=0)
    def C(pid_actor):
        ray.get(pid_actor.report_pid.remote("C", os.getpid()))
        sleep_or_fail()

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Actor:
        def run(self, pid_actor):
            with pytest.raises(
                (ray.exceptions.RayTaskError, ray.exceptions.WorkerCrashedError)
            ):
                ray.get(A.remote(exit_type=exit_type, pid_actor=pid_actor))

    pid_actor = PidActor.remote()

    if actor_or_normal_tasks == "normal_task":
        with pytest.raises(
            (ray.exceptions.RayTaskError, ray.exceptions.WorkerCrashedError)
        ):
            ray.get(A.remote(exit_type=exit_type, pid_actor=pid_actor))
    else:
        a = Actor.remote()
        ray.get(a.run.remote(pid_actor=pid_actor))

    wait_for_condition(
        verify_tasks_running_or_terminated,
        task_pids=ray.get(pid_actor.get_pids.remote()),
        expect_num_tasks=3,
    )


NORMAL_TASK = "normal_task"
ACTOR_TASK = "actor_task"


@pytest.mark.parametrize(
    "death_list",
    [
        [("A", "exit_kill")],
        [("Abb", "exit_kill"), ("C", "exit_exception")],
        [("D", "exit_kill"), ("Ca", "exit_kill"), ("A", "exit_exception")],
    ],
)
def test_fault_tolerance_advanced_tree(shutdown_only, death_list):
    """
    Test fault tolerance for a more complicated execution graph.
    """
    # Root should always be finish
    execution_graph = {
        "root": [
            (NORMAL_TASK, "A"),
            (ACTOR_TASK, "B"),
            (NORMAL_TASK, "C"),
            (ACTOR_TASK, "D"),
        ],
        "A": [(ACTOR_TASK, "Aa"), (NORMAL_TASK, "Ab")],
        "C": [(ACTOR_TASK, "Ca"), (NORMAL_TASK, "Cb")],
        "D": [
            (NORMAL_TASK, "Da"),
            (NORMAL_TASK, "Db"),
            (ACTOR_TASK, "Dc"),
            (ACTOR_TASK, "Dd"),
        ],
        "Aa": [],
        "Ab": [(ACTOR_TASK, "Aba"), (NORMAL_TASK, "Abb"), (NORMAL_TASK, "Abc")],
        "Ca": [(ACTOR_TASK, "Caa"), (NORMAL_TASK, "Cab")],
        "Abb": [(NORMAL_TASK, "Abba")],
        "Abc": [],
        "Abba": [(NORMAL_TASK, "Abbaa"), (ACTOR_TASK, "Abbab")],
        "Abbaa": [(NORMAL_TASK, "Abbaaa"), (ACTOR_TASK, "Abbaab")],
    }

    ray.init(_system_config=_SYSTEM_CONFIG)

    @ray.remote
    class Killer:
        def __init__(self, death_list):
            self.idx_ = 0
            self.death_list_ = death_list
            self.kill_started = False
            self.name_to_pids = {}

        async def start_killing(self):
            self.kill_started = True

        async def next_to_kill(self):
            while not self.kill_started:
                await asyncio.sleep(0.5)

            # if no more tasks to kill - simply sleep to keep all running tasks blocked.
            while self.idx_ >= len(self.death_list_):
                await asyncio.sleep(999)

            to_kill = self.death_list_[self.idx_]
            return to_kill

        async def report_pid(self, name, pid):
            self.name_to_pids[name] = (pid, None)

        async def get_pids(self):
            return self.name_to_pids

        async def all_killed(self):
            while self.idx_ < len(self.death_list_):
                await asyncio.sleep(0.5)

        async def advance_next(self):
            self.idx_ += 1

    def run_children(my_name, killer, execution_graph):
        children = execution_graph.get(my_name, [])
        for task_type, child_name in children:
            if task_type == NORMAL_TASK:
                task.options(name=child_name).remote(
                    child_name, killer, execution_graph
                )
            else:
                a = Actor.remote()
                a.actor_task.options(name=child_name).remote(
                    child_name, killer, execution_graph
                )

        # Block until killed
        while True:
            to_fail, fail_kind = ray.get(killer.next_to_kill.remote())
            if to_fail == my_name:
                ray.get(killer.advance_next.remote())
                if fail_kind == "exit_kill":
                    os._exit(1)
                elif fail_kind == "exit_exception":
                    raise ValueError("Killed by test")
                else:
                    assert False, f"Test invalid kill options: {fail_kind}"
            else:
                # Sleep a bit to wait for death.
                time.sleep(0.1)

    @ray.remote(max_task_retries=0, max_restarts=0)
    class Actor:
        def actor_task(self, my_name, killer, execution_graph):
            ray.get(killer.report_pid.remote(my_name, os.getpid()))
            run_children(my_name, killer, execution_graph)

    @ray.remote(max_retries=0)
    def task(my_name, killer, execution_graph):
        ray.get(killer.report_pid.remote(my_name, os.getpid()))
        run_children(my_name, killer, execution_graph)

    killer = Killer.remote(death_list)
    # Kick off the workload
    task.options(name="root").remote("root", killer, execution_graph)

    # Calculate all expected tasks
    tasks = []

    def add_task_recur(task):
        # Not all tasks are keyed in the execution graph.
        tasks.append(task)
        children = execution_graph.get(task, [])
        for _, child in children:
            add_task_recur(child)

    add_task_recur("root")

    def tasks_in_execution_graph_all_running():
        running_tasks = [
            task["name"]
            for task in list_tasks(filters=[("state", "=", "RUNNING")], limit=10000)
        ]
        for task in tasks:
            assert task in running_tasks, f"Task {task} not running"
        print("All tasks in execution graph are running")
        return True

    wait_for_condition(
        tasks_in_execution_graph_all_running, timeout=20, retry_interval_ms=2000
    )

    # Starts killing :O
    print("start killing")
    ray.get(killer.start_killing.remote())
    print("waiting for all killed")
    ray.get(killer.all_killed.remote())
    print("all killed")

    wait_for_condition(
        verify_tasks_running_or_terminated,
        task_pids=ray.get(killer.get_pids.remote()),
        expect_num_tasks=len(tasks),
        timeout=30,
        retry_interval_ms=500,
    )


def check_file(type, task_name, expected_log, expect_no_end=False):
    """Check file of type = 'out'/'err'"""

    def _read_file(filepath, start, end):
        with open(filepath, "r") as f:
            f.seek(start, 0)
            if end is None:
                return f.read()
            return f.read(end - start)

    tasks = list_tasks(filters=[("name", "=", f"{task_name}")], detail=True)
    assert len(tasks) == 1
    task = tasks[0]
    assert task["task_log_info"] is not None
    log_info = task["task_log_info"]

    file = log_info.get(f"std{type}_file", None)
    start_offset = log_info.get(f"std{type}_start", None)
    end_offset = log_info.get(f"std{type}_end", None)
    if not expect_no_end:
        assert end_offset >= start_offset
    else:
        assert end_offset is None
    assert start_offset > 0, "offsets should be > 0 with magical log prefix"
    actual_log = _read_file(file, start_offset, end_offset)
    assert actual_log == expected_log


@pytest.mark.skipif(
    not ray_constants.RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING or sys.platform == "win32",
    reason=(
        "Skipping if not recording task logs offsets, "
        "and windows has logging race issues."
    ),
)
def test_task_logs_info_basic(shutdown_only):
    """Test tasks (normal tasks/actor tasks) execution logging
    to files have the correct task log info
    """
    ray.init(num_cpus=1)

    def do_print(x):
        out_msg = ""
        err_msg = ""
        for j in range(3):
            out_msg += f"this is log line {j} to stdout from {x}\n"
        print(out_msg, end="", file=sys.stdout)

        for j in range(3):
            err_msg += f"this is log line {j} to stderr from {x}\n"
        print(err_msg, end="", file=sys.stderr)
        return out_msg, err_msg

    @ray.remote
    class Actor:
        def print(self, x):
            return do_print(x)

    @ray.remote
    def task_print(x):
        return do_print(x)

    a = Actor.remote()
    expected_logs = {}
    for j in range(3):
        exp_actor_out, exp_actor_err = ray.get(
            a.print.options(name=f"actor-task-{j}").remote(f"actor-task-{j}")
        )
        expected_logs[f"actor-task-{j}-out"] = exp_actor_out
        expected_logs[f"actor-task-{j}-err"] = exp_actor_err

    for j in range(3):
        exp_task_out, exp_task_err = ray.get(
            task_print.options(name=f"normal-task-{j}").remote(f"normal-task-{j}")
        )
        expected_logs[f"normal-task-{j}-out"] = exp_task_out
        expected_logs[f"normal-task-{j}-err"] = exp_task_err

    def verify():
        # verify logs
        for j in range(3):
            check_file("out", f"normal-task-{j}", expected_logs[f"normal-task-{j}-out"])
            check_file("err", f"normal-task-{j}", expected_logs[f"normal-task-{j}-err"])
            check_file("out", f"actor-task-{j}", expected_logs[f"actor-task-{j}-out"])
            check_file("err", f"actor-task-{j}", expected_logs[f"actor-task-{j}-err"])
            return True

    wait_for_condition(verify)


@pytest.mark.skipif(
    not ray_constants.RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING,
    reason="Skipping if not recording task logs offsets.",
)
def test_task_logs_info_disabled(shutdown_only, monkeypatch):
    """Test when redirect disabled, no task log info is available
    due to missing log file
    """
    with monkeypatch.context() as m:
        m.setenv(ray_constants.LOGGING_REDIRECT_STDERR_ENVIRONMENT_VARIABLE, "1")

        ray.init(num_cpus=1)

        @ray.remote
        def f():
            print("hi")

        ray.get(f.remote())

        def verify():
            tasks = list_tasks()

            assert len(tasks) == 1
            assert tasks[0].get("task_log_info") is None
            return True

        wait_for_condition(verify)


@pytest.mark.skipif(
    not ray_constants.RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING,
    reason="Skipping if not recording task logs offsets.",
)
def test_task_logs_info_running_task(shutdown_only):
    ray.init(num_cpus=1)

    @ray.remote
    def do_print_sleep(out_msg, err_msg):
        print(out_msg, end="", file=sys.stdout)
        print(err_msg, end="", file=sys.stderr)
        time.sleep(999)

    err_msg = "this is log line to stderr before sleeping\n"
    out_msg = "this is log line to stdout before sleeping\n"
    task_name = "log-running-task"
    do_print_sleep.options(name=task_name).remote(out_msg, err_msg)

    def verify():
        check_file("err", task_name, err_msg, expect_no_end=True)
        check_file("out", task_name, out_msg, expect_no_end=True)
        return True

    wait_for_condition(verify)


@pytest.mark.asyncio
async def test_task_events_gc_jobs(shutdown_only):
    """
    Test that later jobs should override previous jobs' task events.
    """
    ctx = ray.init(
        num_cpus=8,
        _system_config={
            "task_events_max_num_task_in_gcs": 3,
            "task_events_skip_driver_for_test": True,
            "task_events_report_interval_ms": 100,
        },
    )

    script = """
import ray

ray.init("auto")
@ray.remote
def f():
    pass

ray.get([f.options(name="f.{task_name}").remote() for _ in range(10)])
"""

    gcs_address = ctx.address_info["gcs_address"]
    manager = get_state_api_manager(gcs_address)

    def get_last_job() -> str:
        jobs = list_jobs()
        sorted(jobs, key=lambda x: x["job_id"])
        return jobs[-1].job_id

    async def verify_tasks(task_name: str):
        # Query with job directly.
        resp = await manager.list_tasks(
            option=ListApiOptions(
                filters=[("name", "=", task_name), ("job_id", "=", get_last_job())]
            )
        )
        assert len(resp.result) == 3
        assert resp.total == 10
        assert resp.num_after_truncation == 3

        return True

    for i in range(10):
        # Run the script
        run_string_as_driver(script.format(task_name=i))

        await async_wait_for_condition(
            verify_tasks, task_name=f"f.{i}", retry_interval_ms=500
        )


def test_task_events_gc_default_policy(shutdown_only):
    @ray.remote
    def finish_task():
        pass

    @ray.remote
    def running_task():
        time.sleep(999)

    @ray.remote
    class Actor:
        def actor_finish_task(self):
            pass

        def actor_running_task(self):
            time.sleep(999)

        def ready(self):
            pass

    @ray.remote(max_retries=0)
    def error_task():
        raise ValueError("Expected to fail")

    ray.init(
        num_cpus=8,
        _system_config={
            "task_events_max_num_task_in_gcs": 5,
            "task_events_skip_driver_for_test": True,
            "task_events_report_interval_ms": 100,
        },
    )
    a = Actor.remote()
    ray.get(a.ready.remote())
    # Run 10 and 5 should be evicted
    ray.get([finish_task.remote() for _ in range(10)])

    def verify_tasks(expected_tasks_cnt: Dict[str, int]):
        tasks = list_tasks(raise_on_missing_output=False)
        total_cnt = reduce(lambda x, y: x + y, expected_tasks_cnt.values())
        assert len(tasks) == total_cnt
        actual_cnt = defaultdict(int)
        for task in tasks:
            actual_cnt[task.name] += 1
        assert actual_cnt == expected_tasks_cnt
        return True

    wait_for_condition(verify_tasks, expected_tasks_cnt={"finish_task": 5})

    # Run a few other tasks to occupy the buffer
    running_task.remote()
    error_task.remote()

    wait_for_condition(
        verify_tasks,
        expected_tasks_cnt={"finish_task": 3, "running_task": 1, "error_task": 1},
    )

    # Run more finished tasks should not evict those running/error tasks
    for _ in range(3):
        ray.get(a.actor_finish_task.remote())

    wait_for_condition(
        verify_tasks,
        expected_tasks_cnt={
            "Actor.actor_finish_task": 3,
            "running_task": 1,
            "error_task": 1,
        },
    )

    # Run actor non-finished tasks should not evict those running/error tasks
    [a.actor_running_task.remote() for _ in range(3)]

    wait_for_condition(
        verify_tasks,
        expected_tasks_cnt={
            "Actor.actor_running_task": 3,
            "running_task": 1,
            "error_task": 1,
        },
    )

    # Run more error tasks now should evict the older "running_task"
    [error_task.remote() for _ in range(5)]

    wait_for_condition(verify_tasks, expected_tasks_cnt={"error_task": 5})


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="setproctitle has different definitions of `title` on different OSes",
)
class TestIsActorTaskRunning:
    def test_main_thread_short_comm(self, ray_start_regular):
        """
        Test that the main thread's comm is not truncated.
        """

        @ray.remote
        class A:
            def check(self):
                pid = os.getpid()
                assert _is_actor_task_running(pid, "check")
                assert psutil.Process(pid).name() == "ray::A.check"
                assert psutil.Process(pid).cmdline()[0] == "ray::A.check"
                return pid

        a = A.remote()
        pid = ray.get(a.check.remote())
        wait_for_condition(lambda: not _is_actor_task_running(pid, "check"))

    def test_main_thread_long_comm(self, ray_start_regular):
        """
        In this case, the process comm should be truncated because of the
        name is more than 15 characters ("ray::Actor.check_long_comm"). Hence,
        `psutil.Process.name()` will return `cmdline()[0]` instead.
        """

        @ray.remote
        class Actor:
            def check_long_comm(self):
                pid = os.getpid()
                assert _is_actor_task_running(pid, "check_long_comm")
                assert psutil.Process(pid).name() == "ray::Actor.check_long_comm"
                assert psutil.Process(pid).cmdline()[0] == "ray::Actor.check_long_comm"
                return pid

        a = Actor.remote()
        pid = ray.get(a.check_long_comm.remote())
        wait_for_condition(lambda: not _is_actor_task_running(pid, "check_long_comm"))

    def test_main_thread_options_name_short_comm(self, ray_start_regular):
        """
        The task name is passed in as `options.name`.
        """
        task_name = "hello"

        @ray.remote
        class A:
            def check(self):
                pid = os.getpid()
                assert _is_actor_task_running(pid, task_name)
                assert psutil.Process(pid).name() == f"ray::{task_name}"
                assert psutil.Process(pid).cmdline()[0] == f"ray::{task_name}"
                return pid

        a = A.remote()
        pid = ray.get(a.check.options(name=task_name).remote())
        wait_for_condition(lambda: not _is_actor_task_running(pid, task_name))

    def test_main_thread_options_name_long_comm(self, ray_start_regular):
        """
        The task name is passed in as `options.name`, and it's longer than 15
        characters. `psutil.Process.name()` will return `cmdline()[0]` instead.
        """
        task_name = "very_long_task_name_1234567890"

        @ray.remote
        class A:
            def check(self):
                pid = os.getpid()
                assert _is_actor_task_running(pid, task_name)
                assert psutil.Process(pid).name() == f"ray::{task_name}"
                assert psutil.Process(pid).cmdline()[0] == f"ray::{task_name}"
                return pid

        a = A.remote()
        pid = ray.get(a.check.options(name=task_name).remote())
        wait_for_condition(lambda: not _is_actor_task_running(pid, task_name))

    def test_default_thread_short_comm(self, ray_start_regular):
        """
        `check` is not running in the main thread, so `/proc/pid/comm` will
        not be updated but `/proc/pid/cmdline` will still be updated.
        """

        @ray.remote(concurrency_groups={"io": 1})
        class A:
            def check(self):
                pid = os.getpid()
                assert _is_actor_task_running(pid, "check")
                assert psutil.Process(pid).name() == "ray::A"
                assert psutil.Process(pid).cmdline()[0] == "ray::A.check"
                return pid

        a = A.remote()
        pid = ray.get(a.check.remote())
        wait_for_condition(lambda: not _is_actor_task_running(pid, "check"))

    def test_default_thread_long_comm(self, ray_start_regular):
        """
        `check` is not running in the main thread, so `/proc/pid/comm` will
        not be updated but `/proc/pid/cmdline` will still be updated.

        In this example, because `ray::VeryLongCommActor` is longer than 15
        characters, `psutil.Process.name()` will return `cmdline()[0]` instead.
        """

        @ray.remote(concurrency_groups={"io": 1})
        class VeryLongCommActor:
            def check_long_comm(self):
                pid = os.getpid()
                assert _is_actor_task_running(pid, "check_long_comm")
                assert (
                    psutil.Process(pid).name()
                    == "ray::VeryLongCommActor.check_long_comm"
                )
                assert (
                    psutil.Process(pid).cmdline()[0]
                    == "ray::VeryLongCommActor.check_long_comm"
                )
                return pid

        a = VeryLongCommActor.remote()
        pid = ray.get(a.check_long_comm.remote())
        wait_for_condition(lambda: not _is_actor_task_running(pid, "check_long_comm"))

    def test_default_thread_options_name_short_comm(self, ray_start_regular):
        """
        `check` is not running in the main thread, so `/proc/pid/comm` will
        not be updated but `/proc/pid/cmdline` will still be updated.
        """
        task_name = "hello"

        @ray.remote(concurrency_groups={"io": 1})
        class A:
            def check(self):
                pid = os.getpid()
                assert _is_actor_task_running(pid, task_name)
                assert psutil.Process(pid).name() == "ray::A"
                assert psutil.Process(pid).cmdline()[0] == f"ray::{task_name}"
                return pid

        a = A.remote()
        pid = ray.get(a.check.options(name=task_name).remote())
        wait_for_condition(lambda: not _is_actor_task_running(pid, task_name))

    def test_default_thread_options_name_long_comm(self, ray_start_regular):
        task_name = "hello"

        @ray.remote(concurrency_groups={"io": 1})
        class Actor:
            def check_long_comm(self):
                pid = os.getpid()
                assert _is_actor_task_running(pid, task_name)
                assert psutil.Process(pid).name() == "ray::Actor"
                assert psutil.Process(pid).cmdline()[0] == f"ray::{task_name}"
                return pid

        a = Actor.remote()
        pid = ray.get(a.check_long_comm.options(name=task_name).remote())
        wait_for_condition(lambda: not _is_actor_task_running(pid, task_name))

    def test_default_thread_options_name_long_comm_2(self, ray_start_regular):
        """
        `/proc/PID/comm` is truncated to 15 characters, so the process title
        is "ray::VeryLongCo". `psutil.Process.name()` doesn't return `cmdline()[0]`
        because `cmdline()[0]` doesn't start with "ray::VeryLongCo". This is the
        implementation detail of `psutil`.
        """
        task_name = "hello"

        @ray.remote(concurrency_groups={"io": 1})
        class VeryLongCommActor:
            def check_long_comm(self):
                pid = os.getpid()
                assert _is_actor_task_running(pid, task_name)
                # The first 15 characters of "ray::VeryLongCommActor"
                assert psutil.Process(pid).name() == "ray::VeryLongCo"
                assert psutil.Process(pid).cmdline()[0] == f"ray::{task_name}"
                return pid

        a = VeryLongCommActor.remote()
        pid = ray.get(a.check_long_comm.options(name=task_name).remote())
        wait_for_condition(lambda: not _is_actor_task_running(pid, task_name))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
