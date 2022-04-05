import time
import os
import json
import argparse

parser = argparse.ArgumentParser(description="Parquet Metadata Read")
parser.add_argument("--num-files", type=int, default=30)


if __name__ == "__main__":
    args = parser.parse_args()
    import ray

    print("Connecting to Ray cluster...")
    ray.init(address="auto")

    num = args.num_files

    files = [
        f"s3://shuffling-data-loader-benchmarks/data/r10_000_000_000-f1000"
        f"/input_data_{i}.parquet.snappy"
        for i in range(args.num_files)
    ]

    start = time.time()
    ray.data.read_parquet(files).count()  # This should only read Parquet metadata.
    delta = time.time() - start

    print(f"success! total time {delta}")
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"metadata_load_time": delta, "success": 1}))
