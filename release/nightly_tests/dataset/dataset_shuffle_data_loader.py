import argparse
import os
import json
import time

import ray

import numpy as np
import torch

PATH = [
    f"s3://shuffling-data-loader-benchmarks/data/input_data_{i}.parquet.snappy"
    for i in range(0, 25)
]


def create_parser():
    parser = argparse.ArgumentParser(description="Dataset shuffle")
    parser.add_argument("--address", type=str, default=os.environ["RAY_ADDRESS"])
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250000,
        metavar="N",
        help="input batch size for training (default: 250000)",
    )
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--repeat-times", type=int, default=16)
    return parser


def create_torch_iterator(split, batch_size, rank=None):
    print(
        f"Creating Torch shuffling dataset for worker {rank} with "
        f"{batch_size} batch size."
    )
    numpy_to_torch_dtype = {
        np.bool_: torch.bool,
        np.uint8: torch.uint8,
        np.int8: torch.int8,
        np.int16: torch.int16,
        np.int32: torch.int32,
        np.int64: torch.int64,
        np.float16: torch.float16,
        np.float32: torch.float32,
        np.float64: torch.float64,
        np.complex64: torch.complex64,
        np.complex128: torch.complex128,
    }
    DATA_SPEC = {
        "embeddings_name0": (0, 2385, np.int64),
        "embeddings_name1": (0, 201, np.int64),
        "embeddings_name2": (0, 201, np.int64),
        "embeddings_name3": (0, 6, np.int64),
        "embeddings_name4": (0, 19, np.int64),
        "embeddings_name5": (0, 1441, np.int64),
        "embeddings_name6": (0, 201, np.int64),
        "embeddings_name7": (0, 22, np.int64),
        "embeddings_name8": (0, 156, np.int64),
        "embeddings_name9": (0, 1216, np.int64),
        "embeddings_name10": (0, 9216, np.int64),
        "embeddings_name11": (0, 88999, np.int64),
        "embeddings_name12": (0, 941792, np.int64),
        "embeddings_name13": (0, 9405, np.int64),
        "embeddings_name14": (0, 83332, np.int64),
        "embeddings_name15": (0, 828767, np.int64),
        "embeddings_name16": (0, 945195, np.int64),
        "one_hot0": (0, 3, np.int64),
        "one_hot1": (0, 50, np.int64),
        "labels": (0, 1, np.float64),
    }
    feature_columns = list(DATA_SPEC.keys())
    feature_types = [numpy_to_torch_dtype[dtype] for _, _, dtype in DATA_SPEC.values()]
    label_column = feature_columns.pop()
    label_type = feature_types.pop()
    torch_iterator = split.to_torch(
        label_column=label_column,
        feature_columns=feature_columns,
        label_column_dtype=label_type,
        feature_column_dtypes=feature_types[0],
        batch_size=batch_size,
    )
    return torch_iterator


def create_dataset(filenames, repeat_times):
    pipeline = (
        ray.data.read_parquet(list(filenames))
        .repeat(times=repeat_times)
        .random_shuffle_each_window()
    )
    return pipeline


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    print("Connecting to Ray cluster...")
    ray.init(address=args.address)

    start = time.time()

    pipeline = create_dataset(PATH, args.repeat_times)
    splits = pipeline.split(args.num_workers)

    @ray.remote(num_gpus=1)
    def consume(split, rank=None, batch_size=None):
        torch_iterator = create_torch_iterator(split, batch_size=batch_size, rank=rank)
        for i, (x, y) in enumerate(torch_iterator):
            time.sleep(1)
            if i % 10 == 0:
                print(i)
        return

    tasks = [
        consume.remote(split, rank=idx, batch_size=args.batch_size)
        for idx, split in enumerate(splits)
    ]

    ray.get(tasks)

    delta = time.time() - start
    print(f"success! total time {delta}")
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"shuffle_time": delta, "success": 1}))
