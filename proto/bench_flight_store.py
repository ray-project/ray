"""Benchmark: latency of Arrow table transfer through Ray.

One actor produces tables, one actor consumes (takes table as input,
returns it as output). Measures per-iteration latency.

If RAY_USE_FLIGHT_STORE=1 is set, tables go through the Flight store.
Otherwise they go through Ray's normal object store (plasma).
"""

import os
import time

import numpy as np
import pyarrow as pa

import ray

SIZES_MB = [1, 10, 50, 200]
N_ITERS = 20
NUM_CPUS = 4


@ray.remote(num_cpus=1)
class Producer:
    def make_table(self, size_mb):
        n_rows = max(1, size_mb * 1024 * 1024 // 8)
        return pa.table({"data": np.random.randn(n_rows)})


@ray.remote(num_cpus=1)
class Consumer:
    def process(self, table):
        assert isinstance(table, pa.Table), f"Expected pa.Table, got {type(table)}"
        return table


def bench(producer, consumer, size_mb, n_iters):
    # Warmup.
    ref = producer.make_table.remote(size_mb)
    ray.get(consumer.process.remote(ref))

    latencies = []
    for _ in range(n_iters):
        t0 = time.perf_counter()
        ref = producer.make_table.remote(size_mb)
        result = ray.get(consumer.process.remote(ref))
        assert isinstance(result, pa.Table)
        latencies.append(time.perf_counter() - t0)

    avg_ms = sum(latencies) / len(latencies) * 1000
    p50_ms = sorted(latencies)[len(latencies) // 2] * 1000
    p99_ms = sorted(latencies)[int(len(latencies) * 0.99)] * 1000
    throughput_mbs = size_mb / (avg_ms / 1000)
    print(
        f"  {size_mb:4d} MB  "
        f"avg={avg_ms:7.1f}ms  p50={p50_ms:7.1f}ms  p99={p99_ms:7.1f}ms  "
        f"throughput={throughput_mbs:7.1f} MB/s"
    )


def main():
    flight = os.environ.get("RAY_USE_FLIGHT_STORE", "0") == "1"
    mode = "Flight store" if flight else "Ray object store (plasma)"

    ray.init(num_cpus=NUM_CPUS)
    print(f"Mode: {mode}")
    print(f"Sizes: {SIZES_MB} MB, {N_ITERS} iterations each")
    print()

    producer = Producer.remote()
    consumer = Consumer.remote()

    for size_mb in SIZES_MB:
        bench(producer, consumer, size_mb, N_ITERS)

    ray.shutdown()


if __name__ == "__main__":
    main()
