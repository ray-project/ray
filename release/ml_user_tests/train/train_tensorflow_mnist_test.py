import json
import os
import time

import ray
from ray.train.examples.tf.tensorflow_mnist_example import train_tensorflow_mnist

if __name__ == "__main__":
    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "train_tensorflow_mnist_test")

    if addr is not None and addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    train_tensorflow_mnist(num_workers=6, use_gpu=True, epochs=20)

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get(
        "TEST_OUTPUT_JSON", "/tmp/train_tensorflow_mnist_test.json"
    )

    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
