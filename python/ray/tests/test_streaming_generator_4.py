import pytest
import numpy as np
import sys
import time
import gc
import random
import asyncio
from typing import Optional
from pydantic import BaseModel

import ray
from ray._private.test_utils import SignalActor

RECONSTRUCTION_CONFIG = {
    "health_check_failure_threshold": 10,
    "health_check_period_ms": 100,
    "health_check_timeout_ms": 100,
    "health_check_initial_delay_ms": 0,
    "max_direct_call_object_size": 100,
    "task_retry_delay_ms": 100,
    "object_timeout_milliseconds": 200,
    "fetch_warn_timeout_milliseconds": 1000,
}


def assert_no_leak():
    gc.collect()
    core_worker = ray._private.worker.global_worker.core_worker
    ref_counts = core_worker.get_all_reference_counts()
    print(ref_counts)
    for rc in ref_counts.values():
        assert rc["local"] == 0
        assert rc["submitted"] == 0
    assert core_worker.get_memory_store_size() == 0


@pytest.mark.parametrize("backpressure", [False, True])
@pytest.mark.parametrize("delay_latency", [0.1, 1])
@pytest.mark.parametrize("threshold", [1, 3])
def test_ray_datasetlike_mini_stress_test(
    monkeypatch, ray_start_cluster, backpressure, delay_latency, threshold
):
    """
    Test a workload that's like ray dataset + lineage reconstruction.
    """
    if not backpressure:
        if delay_latency == 0.1 and threshold == 1:
            return
        elif delay_latency == 1:
            return

    with monkeypatch.context() as m:
        m.setenv(
            "RAY_testing_asio_delay_us",
            "CoreWorkerService.grpc_server.ReportGeneratorItemReturns=10000:1000000",
        )
        cluster = ray_start_cluster
        cluster.add_node(
            num_cpus=1,
            resources={"head": 1},
            _system_config=RECONSTRUCTION_CONFIG,
            enable_object_reconstruction=True,
        )
        ray.init(address=cluster.address)

        if backpressure:
            threshold = 1
        else:
            threshold = -1

        @ray.remote(
            max_retries=-1,
            _generator_backpressure_num_objects=threshold,
        )
        def dynamic_generator(num_returns):
            for i in range(num_returns):
                time.sleep(0.1)
                yield np.ones(1_000_000, dtype=np.int8) * i

        @ray.remote(num_cpus=0, resources={"head": 1})
        def driver():
            unready = [dynamic_generator.remote(10) for _ in range(5)]
            ready = []
            while unready:
                for a in unready:
                    print(a._generator_ref)
                ready, unready = ray.wait(
                    unready, num_returns=len(unready), timeout=0.1
                )
                for r in ready:
                    try:
                        ref = next(r)
                        print(ref)
                        ray.get(ref)
                    except StopIteration:
                        pass
                    else:
                        unready.append(r)
            return None

        ref = driver.remote()

        nodes = []
        for _ in range(4):
            nodes.append(cluster.add_node(num_cpus=1, object_store_memory=10**8))
        cluster.wait_for_nodes()

        for _ in range(10):
            time.sleep(0.1)
            node_to_kill = random.choices(nodes)[0]
            nodes.remove(node_to_kill)
            cluster.remove_node(node_to_kill, allow_graceful=False)
            nodes.append(cluster.add_node(num_cpus=1, object_store_memory=10**8))

        ray.get(ref)
        del ref

        assert_no_leak()


def test_local_gc_not_hang(shutdown_only, monkeypatch):
    """Verify the generator doesn't deadlock when a local GC is triggered."""
    with monkeypatch.context() as m:
        m.setenv("RAY_local_gc_interval_s", 1)

        ray.init()

        @ray.remote(_generator_backpressure_num_objects=1)
        def f():
            for _ in range(5):
                yield 1

        gen = f.remote()
        time.sleep(5)

        # It should not hang.
        for ref in gen:
            ray.get(gen)


def test_sync_async_mix_regression_test(shutdown_only):
    """Verify when sync and async tasks are mixed up
    it doesn't raise a segfault

    https://github.com/ray-project/ray/issues/41346
    """

    class PayloadPydantic(BaseModel):
        class Error(BaseModel):
            msg: str
            code: int
            type: str

        text: Optional[str] = None
        ts: Optional[float] = None
        reason: Optional[str] = None
        error: Optional[Error] = None

    ray.init()

    @ray.remote
    class B:
        def __init__(self, a):
            self.a = a

        async def stream(self):
            async for ref in self.a.stream.remote(1):
                print("stream")
                await ref

        async def start(self):
            await asyncio.gather(*[self.stream() for _ in range(2)])

    @ray.remote
    class A:
        def stream(self, i):
            payload = PayloadPydantic(
                text="Test output",
                ts=time.time(),
                reason="Success!",
            )

            for _ in range(10):
                yield payload

        async def aio_stream(self):
            for _ in range(10):
                yield 1

    a = A.remote()
    b = B.remote(a)
    ray.get(b.start.remote())


@pytest.mark.parametrize("use_asyncio", [False, True])
def test_cancel(shutdown_only, use_asyncio):
    """Test concurrent task cancellation with generator task.

    Once the caller receives an ack that the executor has cancelled the task
    execution, the caller should receive a TaskCancelledError for the next
    ObjectRef that it tries to read from the generator. This should happen even
    if the caller has already received values for the next object indices in
    the stream. Also, we should not apply the usual logic that reorders
    out-of-order reports if the task was cancelled; waiting for the
    intermediate indices to appear would hang the caller."""

    @ray.remote
    class Actor:
        def ready(self):
            return

        def stream(self, signal):
            cancelled_ref = signal.wait.remote()

            i = 0
            done_at = time.time() + 1
            while time.time() < done_at:
                yield i
                i += 1

                ready, _ = ray.wait([cancelled_ref], timeout=0)
                if not ready:
                    # Continue executing for one second after the driver
                    # cancels. This is to make sure that we receive the cancel
                    # signal while the task is still running.
                    done_at = time.time() + 1

        async def async_stream(self, signal):
            cancelled_ref = signal.wait.remote()

            i = 0
            done_at = time.time() + 1
            while time.time() < done_at:
                yield i
                i += 1

                ready, _ = ray.wait([cancelled_ref], timeout=0)
                if not ready:
                    # Continue executing for one second after the driver
                    # cancels. This is to make sure that we receive the cancel
                    # signal while the task is still running.
                    done_at = time.time() + 1

    signal = SignalActor.remote()
    a = Actor.remote()
    ray.get(a.ready.remote())
    if use_asyncio:
        gen = a.async_stream.remote(signal)
    else:
        gen = a.stream.remote(signal)

    try:
        for i, ref in enumerate(gen):
            assert i == ray.get(ref)
            print(i)
            if i == 0:
                ray.cancel(gen)
                signal.send.remote()
    except ray.exceptions.TaskCancelledError:
        pass


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
