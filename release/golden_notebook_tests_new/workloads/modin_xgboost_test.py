from pathlib import Path
import importlib.util
import ray
import os
import time
import json

NOTEBOOK_PATH_RELATIVE_TO_RAY = "doc/examples/modin_xgboost/modin_xgboost.py"

def main():
    # get the ray folder
    ray_path = next(
        x for x in Path(__file__).resolve().parents if str(x).endswith("/ray"))
    notebook_path = ray_path.joinpath(NOTEBOOK_PATH_RELATIVE_TO_RAY)
    assert notebook_path.exists()

    spec = importlib.util.spec_from_file_location("notebook_test",
                                                    notebook_path)
    notebook_test_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(notebook_test_module)

if __name__ == "__main__":
    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "modin_xgboost_test")
    if addr is not None and addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    main()

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/modin_xgboost_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")