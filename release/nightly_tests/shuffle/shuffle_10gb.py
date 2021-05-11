import subprocess
import time
import os
import json

if __name__ == "__main__":
    start = time.time()
    # 10GB shuffle
    subprocess.check_call([
        "python", "-m", "ray.experimental.shuffle", "--ray-address={}".format(
            os.environ["RAY_ADDRESS"]), "--num-partitions=50",
        "--partition-size=200e6"
    ])
    delta = time.time() - start
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"shuffle_time": delta}))
