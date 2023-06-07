import asyncio
import sys
from copy import deepcopy
from collections import defaultdict
import concurrent.futures
from dataclasses import dataclass, field
import logging
import numpy as np
import pprint
import time
import traceback
from typing import Callable, Dict, List, Optional, Tuple, Union
from ray.util.state import list_tasks
import ray
from ray.actor import ActorHandle
from ray.util.state import list_workers


@dataclass
class StateAPIMetric:
    latency_sec: float
    result_size: int


@dataclass
class StateAPICallSpec:
    api: Callable
    verify_cb: Callable
    kwargs: Dict = field(default_factory=dict)


@dataclass
class StateAPIStats:
    pending_calls: int = 0
    total_calls: int = 0
    calls: Dict = field(default_factory=lambda: defaultdict(list))


GLOBAL_STATE_STATS = StateAPIStats()

STATE_LIST_LIMIT = int(1e6)  # 1m
STATE_LIST_TIMEOUT = 600  # 10min


def invoke_state_api(
    verify_cb: Callable,
    state_api_fn: Callable,
    state_stats: StateAPIStats = GLOBAL_STATE_STATS,
    key_suffix: Optional[str] = None,
    print_result: Optional[bool] = False,
    err_msg: Optional[str] = None,
    **kwargs,
):
    """Invoke a State API

    Args:
        - verify_cb: Callback that takes in the response from `state_api_fn` and
            returns a boolean, indicating the correctness of the results.
        - state_api_fn: Function of the state API
        - state_stats: Stats
        - kwargs: Keyword arguments to be forwarded to the `state_api_fn`
    """
    if "timeout" not in kwargs:
        kwargs["timeout"] = STATE_LIST_TIMEOUT

    # Suppress missing output warning
    kwargs["raise_on_missing_output"] = False

    res = None
    try:
        state_stats.total_calls += 1
        state_stats.pending_calls += 1

        t_start = time.perf_counter()
        res = state_api_fn(**kwargs)
        t_end = time.perf_counter()

        if print_result:
            pprint.pprint(res)

        metric = StateAPIMetric(t_end - t_start, len(res))
        if key_suffix:
            key = f"{state_api_fn.__name__}_{key_suffix}"
        else:
            key = state_api_fn.__name__
        state_stats.calls[key].append(metric)
        assert verify_cb(
            res
        ), f"Calling State API failed. len(res)=({len(res)}): {err_msg}"
    except Exception as e:
        traceback.print_exc()
        assert (
            False
        ), f"Calling {state_api_fn.__name__}({kwargs}) failed with {repr(e)}."
    finally:
        state_stats.pending_calls -= 1

    return res


def aggregate_perf_results(state_stats: StateAPIStats = GLOBAL_STATE_STATS):
    """Aggregate stats of state API calls

    Return:
        This returns a dict of below fields:
            - max_{api_key_name}_latency_sec:
                Max latency of call to {api_key_name}
            - {api_key_name}_result_size_with_max_latency:
                The size of the result (or the number of bytes for get_log API)
                for the max latency invocation
            - avg/p99/p95/p50_{api_key_name}_latency_sec:
                The percentile latency stats
            - avg_state_api_latency_sec:
                The average latency of all the state apis tracked
    """
    # Prevent iteration when modifying error
    state_stats = deepcopy(state_stats)
    perf_result = {}
    for api_key_name, metrics in state_stats.calls.items():
        # Per api aggregation
        # Max latency
        latency_key = f"max_{api_key_name}_latency_sec"
        size_key = f"{api_key_name}_result_size_with_max_latency"
        metric = max(metrics, key=lambda metric: metric.latency_sec)

        perf_result[latency_key] = metric.latency_sec
        perf_result[size_key] = metric.result_size

        latency_list = np.array([metric.latency_sec for metric in metrics])
        # avg latency
        key = f"avg_{api_key_name}_latency_sec"
        perf_result[key] = np.average(latency_list)

        # p99 latency
        key = f"p99_{api_key_name}_latency_sec"
        perf_result[key] = np.percentile(latency_list, 99)

        # p95 latency
        key = f"p95_{api_key_name}_latency_sec"
        perf_result[key] = np.percentile(latency_list, 95)

        # p50 latency
        key = f"p50_{api_key_name}_latency_sec"
        perf_result[key] = np.percentile(latency_list, 50)

    all_state_api_latency = sum(
        metric.latency_sec
        for metric_samples in state_stats.calls.values()
        for metric in metric_samples
    )

    perf_result["avg_state_api_latency_sec"] = (
        (all_state_api_latency / state_stats.total_calls)
        if state_stats.total_calls != 0
        else -1
    )

    return perf_result


@ray.remote(num_cpus=0)
class StateAPIGeneratorActor:
    def __init__(
        self,
        apis: List[StateAPICallSpec],
        call_interval_s: float = 5.0,
        print_interval_s: float = 20.0,
        wait_after_stop: bool = True,
        print_result: bool = False,
    ) -> None:
        """An actor that periodically issues state API

        Args:
            - apis: List of StateAPICallSpec
            - call_interval_s: State apis in the `apis` will be issued
                every `call_interval_s` seconds.
            - print_interval_s: How frequent state api stats will be dumped.
            - wait_after_stop: When true, call to `ray.get(actor.stop.remote())`
                will wait for all pending state APIs to return.
                Setting it to `False` might miss some long-running state apis calls.
            - print_result: True if result of each API call is printed. Default False.
        """
        # Configs
        self._apis = apis
        self._call_interval_s = call_interval_s
        self._print_interval_s = print_interval_s
        self._wait_after_cancel = wait_after_stop
        self._logger = logging.getLogger(self.__class__.__name__)
        self._print_result = print_result

        # States
        self._tasks = None
        self._fut_queue = None
        self._executor = None
        self._loop = None
        self._stopping = False
        self._stopped = False
        self._stats = StateAPIStats()

    async def start(self):
        # Run the periodic api generator
        self._fut_queue = asyncio.Queue()
        self._executor = concurrent.futures.ThreadPoolExecutor()

        self._tasks = [
            asyncio.ensure_future(awt)
            for awt in [
                self._run_generator(),
                self._run_result_waiter(),
                self._run_stats_reporter(),
            ]
        ]
        await asyncio.gather(*self._tasks)

    def call(self, fn, verify_cb, **kwargs):
        def run_fn():
            try:
                self._logger.debug(f"calling {fn.__name__}({kwargs})")
                return invoke_state_api(
                    verify_cb,
                    fn,
                    state_stats=self._stats,
                    print_result=self._print_result,
                    **kwargs,
                )
            except Exception as e:
                self._logger.warning(f"{fn.__name__}({kwargs}) failed with: {repr(e)}")
                return None

        fut = asyncio.get_running_loop().run_in_executor(self._executor, run_fn)
        return fut

    async def _run_stats_reporter(self):
        while not self._stopped:
            # Keep the reporter running until all pending apis finish and the bool
            # `self._stopped` is then True
            self._logger.info(pprint.pprint(aggregate_perf_results(self._stats)))
            try:
                await asyncio.sleep(self._print_interval_s)
            except asyncio.CancelledError:
                self._logger.info(
                    "_run_stats_reporter cancelled, "
                    f"waiting for all api {self._stats.pending_calls}calls to return..."
                )

    async def _run_generator(self):
        try:
            while not self._stopping:
                # Run the state API in another thread
                for api_spec in self._apis:
                    fut = self.call(api_spec.api, api_spec.verify_cb, **api_spec.kwargs)
                    self._fut_queue.put_nowait(fut)

                await asyncio.sleep(self._call_interval_s)
        except asyncio.CancelledError:
            # Stop running
            self._logger.info("_run_generator cancelled, now stopping...")
            return

    async def _run_result_waiter(self):
        try:
            while not self._stopping:
                fut = await self._fut_queue.get()
                await fut
        except asyncio.CancelledError:
            self._logger.info(
                f"_run_result_waiter cancelled, cancelling {self._fut_queue.qsize()} "
                "pending futures..."
            )
            while not self._fut_queue.empty():
                fut = self._fut_queue.get_nowait()
                if self._wait_after_cancel:
                    await fut
                else:
                    # Ignore the queue futures if we are not
                    # waiting on them after stop() called
                    fut.cancel()
            return

    def get_stats(self):
        # deep copy to prevent race between reporting and modifying stats
        return aggregate_perf_results(self._stats)

    def ready(self):
        pass

    def stop(self):
        self._stopping = True
        self._logger.debug(f"calling stop, canceling {len(self._tasks)} tasks")
        for task in self._tasks:
            task.cancel()

        # This will block the stop() function until all futures are cancelled
        # if _wait_after_cancel=True. When _wait_after_cancel=False, it will still
        # wait for any in-progress futures.
        # See: https://docs.python.org/3.8/library/concurrent.futures.html
        self._executor.shutdown(wait=self._wait_after_cancel)
        self._stopped = True


def periodic_invoke_state_apis_with_actor(*args, **kwargs) -> ActorHandle:
    current_node_ip = ray._private.worker.global_worker.node_ip_address
    # Schedule the actor on the current node.
    actor = StateAPIGeneratorActor.options(
        resources={f"node:{current_node_ip}": 0.001}
    ).remote(*args, **kwargs)
    print("Waiting for state api actor to be ready...")
    ray.get(actor.ready.remote())
    print("State api actor is ready now.")
    actor.start.remote()
    return actor


def summarize_worker_startup_time():
    workers = list_workers(
        detail=True,
        filters=[("worker_type", "=", "WORKER")],
        limit=10000,
        raise_on_missing_output=False,
    )
    time_to_launch = []
    time_to_initialize = []
    for worker in workers:
        launch_time = worker.get("worker_launch_time_ms")
        launched_time = worker.get("worker_launched_time_ms")
        start_time = worker.get("start_time_ms")

        if launched_time > 0:
            time_to_launch.append(launched_time - launch_time)
        if start_time:
            time_to_initialize.append(start_time - launched_time)
    time_to_launch.sort()
    time_to_initialize.sort()

    def print_latencies(latencies):
        print(f"Avg: {round(sum(latencies) / len(latencies), 2)} ms")
        print(f"P25: {round(latencies[int(len(latencies) * 0.25)], 2)} ms")
        print(f"P50: {round(latencies[int(len(latencies) * 0.5)], 2)} ms")
        print(f"P95: {round(latencies[int(len(latencies) * 0.95)], 2)} ms")
        print(f"P99: {round(latencies[int(len(latencies) * 0.99)], 2)} ms")

    print("Time to launch workers")
    print_latencies(time_to_launch)
    print("=======================")
    print("Time to initialize workers")
    print_latencies(time_to_initialize)


def verify_failed_task(
    name: str, error_type: str, error_message: Union[str, List[str]]
) -> bool:
    """
    Check if a task with 'name' has failed with the exact error type 'error_type'
    and 'error_message' in the error message.
    """
    tasks = list_tasks(filters=[("name", "=", name)], detail=True)
    assert len(tasks) == 1, tasks
    t = tasks[0]
    assert t["state"] == "FAILED", t
    assert t["error_type"] == error_type, t
    if isinstance(error_message, str):
        error_message = [error_message]
    for msg in error_message:
        assert msg in t.get("error_message", None), t
    return True


@ray.remote
class PidActor:
    def __init__(self):
        self.name_to_pid = {}

    def get_pids(self):
        return self.name_to_pid

    def report_pid(self, name, pid, state=None):
        self.name_to_pid[name] = (pid, state)


def verify_tasks_running_or_terminated(
    task_pids: Dict[str, Tuple[int, Optional[str]]], expect_num_tasks: int
):
    """
    Check if the tasks in task_pids are in RUNNING state if pid exists
    and running the task.
    If the pid is missing or the task is not running the task, check if the task
    is marked FAILED or FINISHED.

    Args:
        task_pids: A dict of task name to (pid, expected terminal state).

    """
    import psutil

    assert len(task_pids) == expect_num_tasks, task_pids
    for task_name, pid_and_state in task_pids.items():
        tasks = list_tasks(detail=True, filters=[("name", "=", task_name)])
        assert len(tasks) == 1, (
            f"One unique task with {task_name} should be found. "
            "Use `options(name=<task_name>)` when creating the task."
        )
        task = tasks[0]
        pid, expected_state = pid_and_state

        # If it's windows/macos, we don't have a way to check if the process
        # is actually running the task since the process name is just python,
        # rather than the actual task name.
        if sys.platform in ["win32", "darwin"]:
            if expected_state is not None:
                assert task["state"] == expected_state, task
            continue
        if psutil.pid_exists(pid) and task_name in psutil.Process(pid).name():
            assert (
                "ray::IDLE" not in task["name"]
            ), "One should not name it 'IDLE' since it's reserved in Ray"
            assert task["state"] == "RUNNING", task
            if expected_state is not None:
                assert task["state"] == expected_state, task
        else:
            # Tasks no longer running.
            if expected_state is None:
                assert task["state"] in [
                    "FAILED",
                    "FINISHED",
                ], f"{task_name}: {task['task_id']} = {task['state']}"
            else:
                assert (
                    task["state"] == expected_state
                ), f"expect {expected_state} but {task['state']} for {task}"

    return True
