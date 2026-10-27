"""Example 3: Multi-cluster sharding from a single lightweight client.

Connects to multiple Ray clusters from one external Python process and fans out
shards concurrently across clusters.
"""

import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Optional, Sequence

from ray_client import RayCluster


class MultiClusterSharder:
    def __init__(self, addresses: Sequence[Optional[str]]):
        self.clusters = [RayCluster(address=addr) for addr in addresses]

    def map_shards(self, task_fn: Callable, shards: List[list]) -> List[dict]:
        with ThreadPoolExecutor(max_workers=max(1, len(shards))) as pool:
            futures = [
                pool.submit(
                    self.clusters[idx % len(self.clusters)].run,
                    task_fn,
                    shard,
                    idx,
                )
                for idx, shard in enumerate(shards)
            ]
            return [f.result() for f in futures]

    def close(self) -> None:
        for cluster in self.clusters:
            cluster.close()


def shard_worker(numbers: list, shard_id: int) -> dict:
    import ray

    @ray.remote
    def square(x: int) -> int:
        return x * x

    squares = ray.get([square.remote(n) for n in numbers])
    return {"shard_id": shard_id, "sum_of_squares": sum(squares), "count": len(squares)}


def run_demo(addresses: Sequence[Optional[str]]) -> dict:
    sharder = MultiClusterSharder(addresses)
    try:
        shards = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        results = sharder.map_shards(shard_worker, shards)
        return {
            "shards": results,
            "total_sum_of_squares": sum(r["sum_of_squares"] for r in results),
        }
    finally:
        sharder.close()


if __name__ == "__main__":
    addrs: Sequence[Optional[str]] = sys.argv[1:] if len(sys.argv) > 1 else [None, None]
    print("Multi-cluster output:", run_demo(addrs))
