"""Example 1: Sequential driver function workflow (`main_1` -> `main_2`).

Runs self-contained functions as non-detached root actors via
`cluster.run`, captures referenced module-level JSON constants (`SCORE_WEIGHT`),
and passes JSON outputs and keyword arguments between steps.
"""

import sys
from typing import Optional

from ray_client import RayCluster

SCORE_WEIGHT = 2.5


def main_1(records: list) -> dict:
    import ray

    @ray.remote
    class Scorer:
        def score(self, item: dict) -> dict:
            return {"id": item["id"], "score": round(item["x"] * SCORE_WEIGHT, 3)}

    scorer = Scorer.remote()
    scored = ray.get([scorer.score.remote(r) for r in records])
    return {"scored": scored, "mean": sum(r["score"] for r in scored) / len(scored)}


def main_2(step1_output: dict, threshold: float = 5.0) -> dict:
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
        return cluster.run(main_2, step1, kwargs={"threshold": 7.5})


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    output = run_demo(cluster_addr)
    print("Workflow output:", output)
