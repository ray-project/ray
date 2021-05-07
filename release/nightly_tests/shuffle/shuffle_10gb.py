import subprocess
import time
import json
import os


if __name__ == "__main__":
    start = time.time()
    # 10GB shuffle
    subprocess.check_call([
        "python", "-m", "ray.experimental.shuffle", "--ray-address=auto",
        "--num-partitions=50", "--partition-size=200e6"
    ])
    delta = time.time() - start

    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/shuffle_10gb.json")

    with open(test_output_json, "wt") as f:
        f.write(json.dumps({"shuffle_time": delta}))
