import json
import os


def to_dict_key(key: str):
    for r in [" ", ":", "-"]:
        key = key.replace(r, "_")
    for r in ["(", ")"]:
        key = key.replace(r, "")
    return key


if __name__ == "__main__":
    from ray._private.ray_perf import main

    results = main() or []

    result_dict = {
        f"{to_dict_key(v[0])}": (v[1], v[2]) for v in results if v is not None
    }

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/microbenchmark.json")

    with open(test_output_json, "wt") as f:
        json.dump(result_dict, f)
