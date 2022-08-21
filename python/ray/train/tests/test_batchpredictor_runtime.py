import gc
import os
import shutil
import time
from pathlib import Path
from typing import Type

import numpy as np
import pytest

import ray
from ray.air.checkpoint import Checkpoint
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import Predictor

NUM_REPEATS = 3


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class DummyPredictor(Predictor):
    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "Predictor":
        with checkpoint.as_directory() as checkpoint_path:
            read_test_data(checkpoint_path)
        return cls()

    def predict(self, data, **kwargs):
        return data


def create_test_data():
    large = 1024 * 1024 * 1024  # 1GB
    medium = 1024 * 1024 * 195  # 195 MB
    small = 1024 * 1024  # 1 MB

    path = Path("checkpoint_dir").absolute()
    shutil.rmtree(str(path), ignore_errors=True)
    os.makedirs(str(path), exist_ok=True)

    with open(path / "large_file", "wb") as f:
        f.write(os.urandom(large))

    with open(path / "medium_file", "wb") as f:
        f.write(os.urandom(medium))

    for i in range(5):
        with open(path / f"small_file_{i}", "wb") as f:
            f.write(os.urandom(small))

    return path


def read_test_data(path):
    path = Path(path).absolute()
    data = []
    with open(path / "large_file", "rb") as f:
        data.append(f.read())

    with open(path / "medium_file", "rb") as f:
        data.append(f.read())

    for i in range(5):
        with open(path / f"small_file_{i}", "rb") as f:
            data.append(f.read())
    return data


def run_predictor(
    checkpoint, data, batch_predictor: Type[BatchPredictor], num_scoring_workers: int
):
    predictor = batch_predictor(checkpoint, DummyPredictor)
    predictor.predict(
        data,
        batch_size=1024 // 8,
        min_scoring_workers=num_scoring_workers,
        max_scoring_workers=num_scoring_workers,
        num_cpus_per_worker=0,
    )


@pytest.fixture
def create_data():
    path = create_test_data()
    yield path
    # The code after the yield will run as teardown code.
    shutil.rmtree(str(path), ignore_errors=True)


# This should always take less than 15 seconds (min of 5) on an m5 machine,
# no matter the number of nodes, cores or workers.
# Results measured on May 25th 2022.
# 1 node * 16 cores/workers
# Times: [9.2977, 9.2603, 9.1659, 9.2752, 9.0771]
# Min: 9.0771, StdDev: 0.0823, peak tmpdir usage: 1.2 GB
#
# 4 nodes * 8 cores/workers
# Times: [12.0017, 12.0717, 11.9984, 11.8895, 11.8899]
# Min: 11.8895, StdDev: 0.0707, peak tmpdir usage: 1.2 GB


@pytest.mark.parametrize("num_scoring_workers", [1, 4])
def test_batchpredictor_runtime(
    num_scoring_workers, ray_start_4_cpus, create_data, tmpdir
):
    """Test BatchPredictor runtimes with different number of workers.

    Should always be below 15s on an m5 instance."""
    data = ray.data.range(1024).repartition(64)
    path = create_data
    checkpoint = Checkpoint.from_directory(str(path))
    results = []
    print("start test")
    for i in range(NUM_REPEATS):
        start_time = time.time()
        run_predictor(checkpoint, data, BatchPredictor, num_scoring_workers)
        runtime = time.time() - start_time
        results.append(runtime)
        gc.collect()
    print(f"results: {results} min: {min(results)}, stddev: {np.std(results)}")

    # should take less than 15 seconds in min case
    assert min(results) < 15


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
