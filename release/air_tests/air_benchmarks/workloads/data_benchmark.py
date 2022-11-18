import argparse
import json
import os
import time

import ray
from ray.air.config import DatasetConfig, ScalingConfig
from ray.air.util.check_ingest import DummyTrainer
from ray.data.preprocessors import BatchMapper

GiB = 1024 * 1024 * 1024


def make_ds(size_gb: int):
    # Dataset of 10KiB tensor records.
    total_size = GiB * size_gb
    record_dim = 1280
    record_size = record_dim * 8
    num_records = int(total_size / record_size)
    dataset = ray.data.range_tensor(num_records, shape=(record_dim,))
    print("Created dataset", dataset, "of size", dataset.size_bytes())
    return dataset


def run_ingest_bulk(dataset, num_workers, num_cpus_per_worker):
    dummy_prep = BatchMapper(lambda df: df * 2, batch_format="pandas")
    trainer = DummyTrainer(
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            trainer_resources={"CPU": 0},
            resources_per_worker={"CPU": num_cpus_per_worker},
            _max_cpu_fraction_per_node=0.1,
        ),
        datasets={"train": dataset},
        preprocessor=dummy_prep,
        num_epochs=1,
        prefetch_blocks=1,
        dataset_config={"train": DatasetConfig(split=True)},
    )
    trainer.fit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument(
        "--num-cpus-per-worker",
        type=int,
        default=1,
        help="Number of CPUs for each training worker.",
    )
    parser.add_argument("--dataset-size-gb", type=int, default=200)
    args = parser.parse_args()
    ds = make_ds(args.dataset_size_gb)

    start = time.time()
    run_ingest_bulk(ds, args.num_workers, args.num_cpus_per_worker)
    end = time.time()
    time_taken = end - start

    result = {"time_taken_s": time_taken}

    print("Results:", result)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)
