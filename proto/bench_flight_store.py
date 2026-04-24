"""Benchmark: latency of Arrow table transfer through Ray.

One actor produces tables, one actor consumes (takes table as input,
returns num_rows). Measures single-hop transfer latency without pulling
the table back to the driver.

FLIGHT_MODE selects the transfer path:
  - "plasma": Ray's normal object store.
  - "native": Flight store via the RAY_USE_FLIGHT_NATIVE=1 intercept in
              store_task_outputs. Fastest (producer->consumer, no owner hop).
  - "rdt":    Flight store via @ray.method(tensor_transport="ARROW_FLIGHT").
              Goes through Ray's RDT owner-orchestrated path.
"""

import time

import numpy as np
import pyarrow as pa

import ray

FLIGHT_MODE = "native"  # "plasma" | "native" | "rdt"

SIZES_MB = [1, 10, 100]
N_ITERS = 10
CONCURRENCY = 4
N_ACTORS = 4


def _producer_cls():
    if FLIGHT_MODE == "rdt":

        @ray.remote(num_cpus=1, max_concurrency=CONCURRENCY)
        class Producer:
            @ray.method(tensor_transport="ARROW_FLIGHT")
            def make_table(self, size_mb):
                n_rows = max(1, size_mb * 1024 * 1024 // 8)
                return pa.table({"data": np.random.randn(n_rows)})

    else:

        @ray.remote(num_cpus=1, max_concurrency=CONCURRENCY)
        class Producer:
            def make_table(self, size_mb):
                n_rows = max(1, size_mb * 1024 * 1024 // 8)
                return pa.table({"data": np.random.randn(n_rows)})

    return Producer


@ray.remote(num_cpus=1, max_concurrency=CONCURRENCY)
class Consumer:
    def process(self, table):
        assert isinstance(table, pa.Table), f"Expected pa.Table, got {type(table)}"
        return table


TASKS_PER_ITER = N_ACTORS * CONCURRENCY


def _submit_round(producers, consumers, size_mb):
    """Submit CONCURRENCY tasks on each of N_ACTORS producer/consumer pairs."""
    consume_refs = []
    for producer, consumer in zip(producers, consumers):
        for _ in range(CONCURRENCY):
            ref = producer.make_table.remote(size_mb)
            consume_refs.append(consumer.process.remote(ref))
    return consume_refs


def bench(producers, consumers, size_mb, n_iters):
    # Warmup.
    ray.wait(
        _submit_round(producers, consumers, size_mb),
        num_returns=TASKS_PER_ITER,
        fetch_local=False,
    )

    latencies = []
    for _ in range(n_iters):
        t0 = time.perf_counter()
        ray.wait(
            _submit_round(producers, consumers, size_mb),
            num_returns=TASKS_PER_ITER,
            fetch_local=False,
        )
        latencies.append(time.perf_counter() - t0)

    avg_ms = sum(latencies) / len(latencies) * 1000
    p50_ms = sorted(latencies)[len(latencies) // 2] * 1000
    p99_ms = sorted(latencies)[int(len(latencies) * 0.99)] * 1000
    throughput_mbs = size_mb * TASKS_PER_ITER / (avg_ms / 1000)
    print(
        f"  {size_mb:4d} MB  "
        f"avg={avg_ms:7.1f}ms  p50={p50_ms:7.1f}ms  p99={p99_ms:7.1f}ms  "
        f"throughput={throughput_mbs:7.1f} MB/s"
    )


_MODE_LABELS = {
    "plasma": "Ray object store (plasma)",
    "native": "Flight store (native, RAY_USE_FLIGHT_NATIVE=1)",
    "rdt": "Flight store (RDT, ARROW_FLIGHT transport)",
}


def main():
    assert FLIGHT_MODE in _MODE_LABELS, f"invalid FLIGHT_MODE: {FLIGHT_MODE}"

    runtime_env = None
    if FLIGHT_MODE == "native":
        runtime_env = {"env_vars": {"RAY_USE_FLIGHT_NATIVE": "1"}}

    ray.init(runtime_env=runtime_env)
    print(f"Mode: {_MODE_LABELS[FLIGHT_MODE]}")
    print(
        f"Sizes: {SIZES_MB} MB, {N_ITERS} iterations each, "
        f"{N_ACTORS} actors x {CONCURRENCY} concurrent tasks "
        f"= {TASKS_PER_ITER} in flight"
    )
    print()

    Producer = _producer_cls()
    producers = [Producer.remote() for _ in range(N_ACTORS)]
    consumers = [Consumer.remote() for _ in range(N_ACTORS)]

    for size_mb in SIZES_MB:
        bench(producers, consumers, size_mb, N_ITERS)

    ray.shutdown()


if __name__ == "__main__":
    main()
