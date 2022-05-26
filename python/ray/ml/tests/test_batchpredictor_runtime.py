import numpy as np
import os
import pytest
import shutil
import time
from pathlib import Path
from typing import Type

import ray
from ray.ml.checkpoint import Checkpoint
from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictor import Predictor

NUM_REPEATS = 3


@pytest.fixture
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class DummyPredictor(Predictor):
    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "Predictor":
        with checkpoint.as_directory() as checkpoint_path:
            read_test_data(checkpoint_path)
        return DummyPredictor()

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


@pytest.mark.parametrize("num_scoring_workers", [1, 4, 8])
def test_batchpredictor_runtime(
    num_scoring_workers, ray_start_8_cpus, create_data, tmpdir
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
    print(f"results: {results} min: {min(results)}, stddev: {np.std(results)}")

    # should take less than 15 seconds in all cases
    assert min(results) < 15


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
