"""Example 1: Sequential driver function workflow (`main_1` -> `main_2`).

Runs self-contained functions as non-detached root actors via
`cluster.run` and passes JSON outputs between steps.
"""

import sys
from typing import Optional

from ray_client import RayCluster


def main_1(records: list) -> dict:
    import ray

    @ray.remote
    def score_record(item: dict) -> dict:
        return {"id": item["id"], "score": round(item["x"] * 2.5, 3)}

    scored = ray.get([score_record.remote(r) for r in records])
    return {"scored": scored, "mean": sum(r["score"] for r in scored) / len(scored)}


def main_2(step1_output: dict, threshold: float) -> dict:
    import ray

    @ray.remote
    class Aggregator:
        def filter_above(self, items: list, min_score: float) -> list:
            return [i for i in items if i["score"] >= min_score]

    agg = Aggregator.remote()
    kept = ray.get(agg.filter_above.remote(step1_output["scored"], threshold))
    return {"kept_ids": [r["id"] for r in kept], "count": len(kept)}


def run_demo(address: Optional[str] = None) -> dict:
    sample = [{"id": f"item-{i}", "x": float(i + 1)} for i in range(6)]
    with RayCluster(address=address) as cluster:
        step1 = cluster.run(main_1, sample)
        return cluster.run(main_2, step1, 7.5)


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    output = run_demo(cluster_addr)
    print("Workflow output:", output)
