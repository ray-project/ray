import ray
import os
import time
import json
from util import import_and_execute_test_script

NOTEBOOK_PATH_RELATIVE_TO_RAY_REPO = (
    "doc/source/ray-core/examples/dask_xgboost/dask_xgboost.py"
)


def main():
    import_and_execute_test_script(NOTEBOOK_PATH_RELATIVE_TO_RAY_REPO)


if __name__ == "__main__":
    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "dask_xgboost_test")
    if addr is not None and addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    main()

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/dask_xgboost_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
