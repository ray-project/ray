"""Example 1: Sequential interactive workflow (`main_1` -> `main_2`).

Demonstrates running self-contained functions as cluster drivers and passing
JSON state between steps through the client.
"""

import sys
from typing import Optional

import ray_client


def main_1(records: list) -> dict:
    import ray

    @ray.remote
    def score_record(item: dict) -> dict:
        return {"id": item["id"], "score": round(item["x"] * 2.5, 3)}

    scored = ray.get([score_record.remote(r) for r in records])
    print(f"[main_1] Scored {len(scored)} records on cluster")
    return {"scored": scored, "mean": sum(r["score"] for r in scored) / len(scored)}


def main_2(step1_output: dict, threshold: float) -> dict:
    import ray

    @ray.remote
    class Aggregator:
        def filter_above(self, items: list, min_score: float) -> list:
            return [i for i in items if i["score"] >= min_score]

    agg = Aggregator.remote()
    kept = ray.get(agg.filter_above.remote(step1_output["scored"], threshold))
    print(f"[main_2] Kept {len(kept)} records >= {threshold}")
    return {"kept_ids": [r["id"] for r in kept], "count": len(kept)}


def run_demo(address: Optional[str] = None) -> dict:
    sample = [{"id": f"item-{i}", "x": float(i + 1)} for i in range(6)]
    job_result_1 = ray_client.run_function_as_driver(main_1, sample, address=address)
    assert job_result_1.status() == "SUCCEEDED"
    step1 = job_result_1.result()

    job_result_2 = ray_client.run_function_as_driver(
        main_2, step1, 7.5, address=address
    )
    assert job_result_2.status() == "SUCCEEDED"
    return job_result_2.result()


if __name__ == "__main__":
    cluster_addr = sys.argv[1] if len(sys.argv) > 1 else None
    output = run_demo(cluster_addr)
    print("Workflow output:", output)
