import argparse
import time

import numpy as np

import ray
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig
from ray.air.util.check_ingest import DummyTrainer
from ray.data.preprocessors import BatchMapper
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import Predictor

GiB = 1024 * 1024 * 1024


class DummyPredictor(Predictor):
    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "Predictor":
        return DummyPredictor()

    def predict(self, data, **kwargs):
        # For 20k records (200GiB), this amounts to 2000 seconds of work.
        time.sleep(len(data) * 0.0001)
        return np.array([42] * len(data))


def make_ds(size_gb: int):
    # Dataset of 10KiB tensor records.
    total_size = 1024 * 1024 * 1024 * size_gb
    record_dim = 1280
    record_size = record_dim * 8
    num_records = int(total_size / record_size)
    dataset = ray.data.range_tensor(num_records, shape=(record_dim,))
    print("Created dataset", dataset, "of size", dataset.size_bytes())
    return dataset


def run_ingest_bulk(dataset, num_workers):
    dummy_prep = BatchMapper(lambda df: df * 2)
    trainer = DummyTrainer(
        scaling_config={
            "num_workers": num_workers,
            "trainer_resources": {"CPU": 0},
            "resources_per_worker": {"CPU": 3},
        },
        datasets={"train": dataset},
        preprocessor=dummy_prep,
        num_epochs=1,
        prefetch_blocks=1,
        dataset_config={"train": DatasetConfig(split=True)},
    )
    trainer.fit()


def run_ingest_streaming(dataset, num_workers):
    dummy_prep = BatchMapper(lambda df: df * 2)
    trainer = DummyTrainer(
        scaling_config={
            "num_workers": num_workers,
            "trainer_resources": {"CPU": 0},
            "resources_per_worker": {"CPU": 3},
        },
        datasets={"train": dataset},
        preprocessor=dummy_prep,
        num_epochs=1,
        prefetch_blocks=1,
        dataset_config={
            "train": DatasetConfig(
                split=True, use_stream_api=True, stream_window_size=num_workers * GiB
            )
        },
    )
    trainer.fit()


def run_infer_bulk(dataset, num_workers, post=None, stream=False, window_size_gb=10):
    start = time.time()
    checkpoint = Checkpoint.from_dict({"dummy": 1})
    # TODO: add preprocessor here
    predictor = BatchPredictor(checkpoint, DummyPredictor)
    if stream:
        result = predictor.predict_pipelined(
            dataset,
            bytes_per_window=window_size_gb * 1024 * 1024 * 1024,
            batch_size=1024 // 8,
            min_scoring_workers=num_workers,
            max_scoring_workers=num_workers,
            num_cpus_per_worker=1,
        )
    else:
        result = predictor.predict(
            dataset,
            batch_size=1024 // 8,
            min_scoring_workers=num_workers,
            max_scoring_workers=num_workers,
            num_cpus_per_worker=1,
        )
    if post:
        post(result)
    print(result.stats())
    print("Total runtime", time.time() - start)


def run_infer_streaming(dataset, num_workers, window_size_gb):
    def post(result):
        for b in result.iter_batches():
            pass

    return run_infer_bulk(
        dataset, num_workers, post, stream=True, window_size_gb=window_size_gb
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--benchmark", type=str, default="ingest", help="ingest or infer"
    )
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--dataset-size-gb", type=int, default=200)
    parser.add_argument("--streaming", action="store_true", default=False)
    parser.add_argument("--window-size-gb", type=int, default=10)
    args = parser.parse_args()
    ds = make_ds(args.dataset_size_gb)
    if args.benchmark == "ingest":
        if args.streaming:
            run_ingest_streaming(ds, args.num_workers)
        else:
            run_ingest_bulk(ds, args.num_workers)
    elif args.benchmark == "infer":
        if args.streaming:
            run_infer_streaming(ds, args.num_workers, args.window_size_gb)
        else:
            run_infer_bulk(ds, args.num_workers)
    else:
        assert False
