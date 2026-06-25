import argparse
import gc
import time

import ray

from benchmark import Benchmark
from ray.data._internal.execution.block_ref_counter import BlockRefCounter

_SETTLE_TIMEOUT_S = 30.0


@ray.remote(num_cpus=1)
def _hold_block(block_ref, sleep_s: float) -> None:
    """Hold block_ref as a task argument for sleep_s seconds, then return."""
    time.sleep(sleep_s)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BlockRefCounter convergence latency benchmark"
    )
    parser.add_argument(
        "--num-tasks",
        type=int,
        default=100,
        help="Number of concurrent tasks / blocks in the burst. "
        "Latency scales with this value (ReferenceCounter mutex serializes callbacks); "
        "increase together with cluster size for stress testing.",
    )
    parser.add_argument(
        "--task-sleep-s",
        type=float,
        default=0.1,
        help="How long each task holds its block before completing.",
    )
    return parser.parse_args()


def run_burst_object_free(args: argparse.Namespace) -> dict:
    n = args.num_tasks
    producer_id = "burst_producer"
    counter = BlockRefCounter()

    # Object size is irrelevant: we measure callback timing, not memory accuracy.
    refs = [ray.put(b"block") for _ in range(n)]
    for ref in refs:
        counter.on_block_produced(ref, 1, producer_id)

    # Submit tasks that each hold one block, then drop Python refs.
    # SPREAD distributes tasks across nodes to simulate a multi-node burst.
    task_futures = [
        _hold_block.options(scheduling_strategy="SPREAD").remote(ref, args.task_sleep_s)
        for ref in refs
    ]
    # Drop Python refs so each block is kept alive solely by its task argument.
    # The callback can only fire once the task completes AND this ref is gone.
    del refs
    gc.collect()

    # T0 is stamped after all tasks finish so settle_time_s measures only the
    # callback firing lag, not task execution time.
    ray.get(task_futures)
    t0 = time.perf_counter()
    gc.collect()

    negative_events = 0
    converged = False
    while time.perf_counter() - t0 < _SETTLE_TIMEOUT_S:
        gc.collect()
        usage = counter.get_object_store_memory_usage(producer_id)
        if usage < 0:
            negative_events += 1
        if usage <= 0:
            converged = True
            break
        time.sleep(0.05)

    settle_time_s = time.perf_counter() - t0

    assert negative_events == 0, f"Counter went negative {negative_events} times"
    assert converged, (
        f"Counter did not reach 0 within {_SETTLE_TIMEOUT_S}s; "
        f"remaining: {counter.get_object_store_memory_usage(producer_id)} bytes"
    )

    return {
        "callback_settle_time_s": round(settle_time_s, 4),
        "num_tasks": n,
        "counter_negative_events": negative_events,
    }


def main(args: argparse.Namespace):
    benchmark = Benchmark()
    benchmark.run_fn("burst-object-free", run_burst_object_free, args)
    benchmark.write_result()


if __name__ == "__main__":
    main(parse_args())
