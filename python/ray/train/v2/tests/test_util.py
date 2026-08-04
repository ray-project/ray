import pytest

from ray.train.utils import _in_ray_train_worker
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer


def test_in_ray_train_worker(ray_start_4_cpus):
    assert not _in_ray_train_worker()

    def train_fn():
        assert _in_ray_train_worker()

    trainer = DataParallelTrainer(train_fn)
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
