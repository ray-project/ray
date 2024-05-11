import json
import os

import argparse


def to_dict_key(key: str):
    for r in [" ", ":", "-"]:
        key = key.replace(r, "_")
    for r in ["(", ")"]:
        key = key.replace(r, "")
    return key


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    from ray._private.ray_experimental_multinode_perf import main
    results = main() or []

    result_dict = {
        f"{to_dict_key(v[0])}": (v[1], v[2]) for v in results if v is not None
    }

    perf_metrics = [
        {
            "perf_metric_name": to_dict_key(v[0]),
            "perf_metric_value": v[1],
            "perf_metric_type": "THROUGHPUT",
        }
        for v in results
        if v is not None
    ]
    result_dict["perf_metrics"] = perf_metrics

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/microbenchmark_multinode.json")

    with open(test_output_json, "wt") as f:
        json.dump(result_dict, f)
