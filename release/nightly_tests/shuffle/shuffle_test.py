import argparse
import time
import os
import json
import subprocess

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-partitions", help="number of partitions", default=50, type=str
    )
    parser.add_argument(
        "--partition-size",
        help="number of reducer actors used",
        default="200e6",
        type=str,
    )
    parser.add_argument(
        "--no-streaming", help="Non streaming shuffle", action="store_true"
    )
    args = parser.parse_args()

    start = time.time()

    commands = [
        "python",
        "-m",
        "ray.experimental.shuffle",
        "--ray-address={}".format(os.environ["RAY_ADDRESS"]),
        f"--num-partitions={args.num_partitions}",
        f"--partition-size={args.partition_size}",
    ]
    if args.no_streaming:
        commands.append("--no-streaming")

    subprocess.check_call(commands)
    delta = time.time() - start

    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"shuffle_time": delta, "success": 1}))
