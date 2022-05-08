from collections import OrderedDict
import argparse
import os
import json
import ray
import time

# Training settings
parser = argparse.ArgumentParser(description="Dataset ingestion Example")
parser.add_argument(
    "--batch-size",
    type=int,
    default=250000,
)
parser.add_argument(
    "--epochs",
    type=int,
    default=10,
)
parser.add_argument("--num-workers", type=int, default=1)
parser.add_argument("--num-files", type=int, default=48) # 80GB of data.
parser.add_argument("--parallelism", type=int, default=400)

@ray.remote
def consume(split, rank=None, batch_size=None):
    for i, _ in enumerate(split.iter_batches(batch_size=batch_size, prefetch_blocks=1)):
        time.sleep(0.01)
        if i % 100 == 0:
            # print(f"rank {rank}, batch {i}")
            pass
    # print(split.stats())
    return

def create_dataset(
    ds,
    num_workers=1,
    epochs=1,
    parallelism=400,
):
    return [ds.repeat(1)]

if __name__ == "__main__":
    args = parser.parse_args()
    import ray

    ray.init(address="auto")
    num = args.num_files

    files = [
        f"s3://shuffling-data-loader-benchmarks/data/r10_000_000_000-f1000"
        f"/input_data_{i}.parquet.snappy"
        for i in range(args.num_files)
    ]

    start = time.time()
    
    # load data on memory nodes.
    ds = ray.data.read_parquet(files).map_batches(lambda x:x, num_gpus=0.00, num_cpus=0.01)

    print(f"data load time {time.time() - start}")
    start = time.time()

    splits = create_dataset(
        ds,
        num_workers=args.num_workers,
        epochs=args.epochs,
        parallelism=args.parallelism,
    )

    # ingest on cpu nodes.
    tasks = [
        consume.options(num_gpus=1, num_cpus=0).remote(
            split, rank=idx, batch_size=args.batch_size
        )
        for idx, split in enumerate(splits)
    ]
    ray.get(tasks)

    delta = time.time() - start
    print(f"validation time {delta}")
