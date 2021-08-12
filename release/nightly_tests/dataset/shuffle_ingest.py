import json
import os
import time
import ray
import argparse

from datetime import datetime

DUMMY_ROW = {
    "key": 0,
    "embeddings_name0": 1527,
    "embeddings_name1": 175,
    "embeddings_name2": 8,
    "embeddings_name3": 5,
    "embeddings_name4": 5,
    "embeddings_name5": 687,
    "embeddings_name6": 165,
    "embeddings_name7": 10,
    "embeddings_name8": 137,
    "embeddings_name9": 597,
    "embeddings_name10": 1574,
    "embeddings_name11": 78522,
    "embeddings_name12": 283941,
    "embeddings_name13": 3171,
    "embeddings_name14": 39560,
    "embeddings_name15": 718571,
    "embeddings_name16": 73699,
    "one_hot0": 1,
    "one_hot1": 30,
    "labels": 0.3801173847541949,
    "__index_level_0__": 0
}


def create_parser():
    parser = argparse.ArgumentParser(description="Test script to evaluate distributed shuffling with consumers")
    parser.add_argument("--address")
    parser.add_argument("--num-rows", type=int, default=8 * (10**8))
    parser.add_argument("--num-files", type=int, default=50)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250000,
        metavar="N",
        help="input batch size for training (default: 64)")
    parser.add_argument("--num-workers", type=int, default=4)
    return parser


def run_consume(args, data_dir=None):

    ds = ray.data.range(args.num_rows).map(lambda i: DUMMY_ROW)
    pipeline = ds.repeat().random_shuffle()
    splits = pipeline.split(args.num_workers, equal=True)

    @ray.remote(num_gpus=1)
    def consume(split, rank=None, batch_size=None):
        start = datetime.now()
        for i, x in enumerate(split.iter_rows()):
            time.sleep(1)
            if i % 10 == 0:
                print(i)
            now = datetime.now()
            # After 3000 seconds, we consider the test succeeds.
            if (now - start).total_seconds() > 3000:
                break
        return

    tasks = [
        consume.remote(split, rank=idx, batch_size=args.batch_size)
        for idx, split in enumerate(splits)
    ]
    ray.get(tasks)


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    print("Connecting to Ray cluster...")
    ray.init(address=args.address)

    run_consume(args, data_dir=None)
    if "TEST_OUTPUT_JSON" in os.environ:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        results = {
            "success": 1
        }
        json.dump(results, out_file)
