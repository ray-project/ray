import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
import logging
import time
import concurrent.futures
import traceback
from typing import Callable, Dict, List
import ray
from ray.actor import ActorHandle
import pprint
import numpy as np


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

TEST_MAX_ACTORS = int(1e4)  # 10k
TEST_MAX_TASKS = int(1e4)  # 10k
TEST_MAX_OBJECTS = int(1e5)  # 100k
TEST_LOG_FILE_SIZE = 4 * 1024 * 1024  # 4GB
STATE_LIST_LIMIT = int(1e6)  # 1m
STATE_LIST_TIMEOUT = 600  # 10min


def invoke_state_api(verify_cb, state_api_fn, state_stats=GLOBAL_STATE_STATS, **kwargs):
    if "timeout" not in kwargs:
        kwargs["timeout"] = STATE_LIST_TIMEOUT

    if "limit" not in kwargs:
        kwargs["limit"] = STATE_LIST_LIMIT

    res = None
    try:
        state_stats.total_calls += 1
        state_stats.pending_calls += 1

        t_start = time.perf_counter()
        res = state_api_fn(**kwargs)
        t_end = time.perf_counter()

        metric = StateAPIMetric(t_end - t_start, len(res))
        state_stats.calls[state_api_fn.__name__] += [metric]
        assert verify_cb(res), f"Calling State API failed. len(res)=({len(res)})"
    except Exception as e:
        traceback.print_exc()
        assert (
            False
        ), f"Calling {state_api_fn.__name__}({kwargs}) failed with {repr(e)}."
    finally:
        state_stats.pending_calls -= 1

    return res


def aggregate_perf_results(state_stats=GLOBAL_STATE_STATS):
    perf_result = {}
    for api_name, metrics in state_stats.calls.items():
        # Per api aggregation
        # Max latency
        latency_key = f"max_{api_name}_latency_sec"
        size_key = f"max_{api_name}_result_size"
        metric = max(metrics, key=lambda metric: metric.latency_sec)

        perf_result[latency_key] = metric.latency_sec
        perf_result[size_key] = metric.result_size

        latency_list = np.array([metric.latency_sec for metric in metrics])
        # p99 latency
        key = f"p99_{api_name}_latency_sec"
        perf_result[key] = np.percentile(latency_list, 99)

        # p95 latency
        key = f"p95_{api_name}_latency_sec"
        perf_result[key] = np.percentile(latency_list, 95)

        # p50 latency
        key = f"p50_{api_name}_latency_sec"
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


@ray.remote
class StateAPIGeneratorActor:
    def __init__(
        self,
        apis: List[StateAPICallSpec],
        call_interval_s: float = 5.0,
        print_interval_s: float = 20.0,
        wait_after_stop: bool = True,
    ) -> None:
        # Configs
        self._apis = apis
        self._call_interval_s = call_interval_s
        self._print_interval_s = print_interval_s
        self._wait_after_cancel = wait_after_stop
        self._logger = logging.getLogger(self.__class__.__name__)

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
                    verify_cb, fn, state_stats=self._stats, **kwargs
                )
            except Exception as e:
                self._logger.warning(f"{fn.__name__}({kwargs}) failed with: {repr(e)}")
                return None

        fut = asyncio.get_running_loop().run_in_executor(self._executor, run_fn)
        return fut

    async def _run_stats_reporter(self):
        while not self._stopped:
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
                # Ignore the queue futures if it is no longer running
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
                    fut.cancel()
            return

    def get_stats(self):
        return aggregate_perf_results(self._stats)

    def ready(self):
        pass

    def stop(self):
        self._stop()

    def _stop(self):
        self._stopping = True
        self._logger.debug(f"calling stop, canceling {len(self._tasks)} tasks")
        for task in self._tasks:
            task.cancel()

        # This will block the _stop() function until all futures are cancelled
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
