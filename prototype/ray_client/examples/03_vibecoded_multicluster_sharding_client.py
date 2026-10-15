"""Example 3: Multi-cluster job sharding from a single lightweight client.

Demonstrates connecting to multiple Ray clusters from a single out-of-cluster
process to shard a batch workload concurrently.
"""

import sys
from typing import Callable, List, Optional, Sequence

from ray_client import RayClient


class MultiClusterSharder:
    """Submits shards concurrently across multiple Ray clusters (`wait=False`)."""

    def __init__(self, addresses: Sequence[Optional[str]]):
        self.clients = [RayClient(address=addr) for addr in addresses]

    def map_shards(self, driver_fn: Callable, shards: List[list]) -> List[dict]:
        jobs = [
            self.clients[idx % len(self.clients)].run_function_as_driver(
                driver_fn, shard, idx, wait=False
            )
            for idx, shard in enumerate(shards)
        ]
        return [job.result() for job in jobs]


def shard_worker(numbers: list, shard_id: int) -> dict:
    import ray

    @ray.remote
    def square(x: int) -> int:
        return x * x

    squares = ray.get([square.remote(n) for n in numbers])
    return {"shard_id": shard_id, "sum_of_squares": sum(squares), "count": len(squares)}


def run_demo(addresses: Sequence[Optional[str]]) -> dict:
    sharder = MultiClusterSharder(addresses)
    shards = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    results = sharder.map_shards(shard_worker, shards)
    return {
        "shards": results,
        "total_sum_of_squares": sum(r["sum_of_squares"] for r in results),
    }


if __name__ == "__main__":
    addrs: Sequence[Optional[str]] = sys.argv[1:] if len(sys.argv) > 1 else [None, None]
    print("Multi-cluster output:", run_demo(addrs))
