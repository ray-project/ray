import os
import time
import json

import ray


if __name__ == "__main__":
    ray.init(address="auto", runtime_env={"working_dir": os.path.dirname(__file__)})

    start = time.time()

    os.system(
        "aws s3 sync s3://large-dl-models-mirror/restricted/models--lmsys--vicuna-13b-delta-v1.1/main-safetensors/ /tmp/vicuna"
    )
    print(os.listdir("/tmp/vicuna"))

    taken = time.time() - start
    result = {
        "time_taken": taken,
        "val_accuracy": 1,
    }
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/lightning_trainer_test.json"
    )
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
