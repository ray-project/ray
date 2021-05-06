import subprocess
import time
import json

if __name__ == "__main__":
    start = time.time()
    # 10GB shuffle
    subprocess.check_call([
        "python", "-m", "ray.experimental.shuffle", "--ray-address=auto",
        "--num-partitions=50", "--partition-size=200e6"
    ])
    delta = time.time() - start
    with open("/tmp/shuffle_10gb.json", "w") as f:
        f.write(json.dumps({"shuffle_time": delta}))
