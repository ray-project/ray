import asyncio
import gc
import os
import random
import signal
import sys
import time
from typing import Optional

import numpy as np
import pytest
from pydantic import BaseModel

import ray
from ray._common.test_utils import SignalActor

RECONSTRUCTION_CONFIG = {
    "health_check_failure_threshold": 10,
    "health_check_period_ms": 100,
    "health_check_timeout_ms": 100,
    "health_check_initial_delay_ms": 0,
    "max_direct_call_object_size": 100,
    "task_retry_delay_ms": 100,
    "object_timeout_milliseconds": 200,
    "fetch_warn_timeout_milliseconds": 1000,
    # Required for reducing the retry time of RequestWorkerLease
    "raylet_rpc_server_reconnect_timeout_s": 0,
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


@pytest.mark.skipif(
    sys.platform == "win32", reason="SIGKILL is not available on Windows"
)
def test_caller_death(monkeypatch, shutdown_only):
    """
    Test the case where caller of a streaming generator actor task dies
    while the streaming generator task is executing. The streaming
    generator task should still finish and won't block other actor tasks.
    This means that `ReportGeneratorItemReturns` RPC should fail and it shouldn't
    be retried indefinitely.
    """
    monkeypatch.setenv("RAY_core_worker_rpc_server_reconnect_timeout_s", "1")
    ray.init()

    @ray.remote
    class Callee:
        def gen(self, caller_pid):
            os.kill(caller_pid, signal.SIGKILL)
            yield [1] * 1024 * 1024

        def ping(self):
            pass

    @ray.remote
    def caller(callee):
        ray.get(callee.gen.remote(os.getpid()))

    callee = Callee.remote()
    o = caller.remote(callee)
    ray.wait([o])
    # Make sure gen will finish and ping can run.
    ray.get(callee.ping.remote())


def test_intermediate_generator_object_recovery_while_generator_running(
    ray_start_cluster,
):
    """
    1. Streaming producer starts on worker1.
    2. consumer consumes value 1 from producer on worker2 and finishes.
    3. Run an extra consumer on worker2 to track when reconstruction is triggered.
    4. Add worker3.
    5. worker2 dies.
    6. Try to get consumer output.
    7. Therefore Ray tries to reconstruct value 1 from producer.
    8. Get the reconstructed extra_consumer_ref (assures 7 happened).
    9. Streaming producer should be cancelled and resubmitted.
    10. Retry for consumer should complete.
    """

    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)  # head
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"producer": 1})  # worker1
    worker2 = cluster.add_node(num_cpus=1, resources={"consumer": 1})

    @ray.remote(num_cpus=1, resources={"producer": 1})
    def producer():
        for _ in range(3):
            yield np.zeros(10 * 1024 * 1024, dtype=np.uint8)

    @ray.remote(num_cpus=1, resources={"consumer": 1})
    def consumer(np_arr):
        return np_arr

    streaming_ref = producer.options(_generator_backpressure_num_objects=1).remote()
    consumer_ref = consumer.remote(next(streaming_ref))
    extra_consumer_ref = consumer.remote(np.zeros(10 * 1024 * 1024, dtype=np.uint8))

    ray.wait([consumer_ref, extra_consumer_ref], num_returns=2, fetch_local=False)

    cluster.add_node(num_cpus=1, resources={"consumer": 1})  # worker3
    cluster.remove_node(worker2, allow_graceful=True)

    # Make sure reconstruction was triggered.
    assert ray.get(extra_consumer_ref).size == (10 * 1024 * 1024)
    # Allow first streaming generator attempt to finish
    ray.get([next(streaming_ref), next(streaming_ref)])

    assert ray.get(consumer_ref).size == (10 * 1024 * 1024)


def test_actor_intermediate_generator_object_recovery_while_generator_running(
    ray_start_cluster,
):
    """
    1. Producer actor and its generator producer task start on worker1.
    2. consumer consumes value 1 from producer on worker2 and finishes.
    3. Run an extra consumer on worker2 to track when reconstruction is triggered.
    4. Add worker3.
    5. worker2 dies.
    6. Ray tries to reconstruct value 1 from producer.
    7. Get the reconstructed extra_consumer_ref (assures 6 happened).
    8. Ray tries and fails to cancel the producer task.
    9. Get the next two values to relieve backpressure and allow producer to finish.
    10. Ray resubmits the producer generator task.
    11. Retry for consumer should complete.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0)  # head
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=1, resources={"producer": 1})  # worker 1
    worker2 = cluster.add_node(num_cpus=1, resources={"consumer": 1})

    @ray.remote(num_cpus=1, resources={"producer": 1}, max_task_retries=-1)
    class Producer:
        def producer(self):
            for _ in range(3):
                yield np.zeros(10 * 1024 * 1024, dtype=np.uint8)

    @ray.remote(num_cpus=1, resources={"consumer": 1})
    def consumer(np_arr):
        return np_arr

    producer_actor = Producer.remote()
    streaming_ref = producer_actor.producer.options(
        _generator_backpressure_num_objects=1
    ).remote()
    consumer_ref = consumer.remote(next(streaming_ref))
    extra_consumer_ref = consumer.remote(np.zeros(10 * 1024 * 1024, dtype=np.uint8))

    ray.wait([consumer_ref, extra_consumer_ref], num_returns=2, fetch_local=False)

    cluster.add_node(num_cpus=1, resources={"consumer": 1})  # worker 3
    cluster.remove_node(worker2, allow_graceful=True)

    # Make sure reconstruction was triggered.
    ray.get(extra_consumer_ref)
    # Allow first streaming generator attempt to finish
    ray.get([next(streaming_ref), next(streaming_ref)])

    assert ray.get(consumer_ref).size == (10 * 1024 * 1024)


@pytest.mark.parametrize("backpressure", [False, True])
@pytest.mark.parametrize("delay_latency", [0.1, 1])
@pytest.mark.parametrize("threshold", [1, 3])
def test_many_tasks_lineage_reconstruction_mini_stress_test(
    monkeypatch, ray_start_cluster, backpressure, delay_latency, threshold
):
    """Test a workload that spawns many tasks and relies on lineage reconstruction."""
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
        m.setenv(
            "RAY_testing_rpc_failure",
            "CoreWorkerService.grpc_client.ReportGeneratorItemReturns=5:25:25",
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
    sys.exit(pytest.main(["-sv", __file__]))
