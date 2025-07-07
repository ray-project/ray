from unittest import mock

import pytest

import ray
import ray.train.collective
from ray.train.collective import collectives
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer


def test_barrier(ray_start_4_cpus):
    @ray.remote
    class Counter:
        def __init__(self):
            self.num_reached_barrier = 0

        def increment(self):
            self.num_reached_barrier += 1

        def get_num_reached_barrier(self):
            return self.num_reached_barrier

    counter = Counter.remote()

    def train_fn():
        counter.increment.remote()
        ray.train.collective.barrier()
        assert ray.get(counter.get_num_reached_barrier.remote()) == 2

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    trainer.fit()


def test_broadcast_from_rank_zero(ray_start_4_cpus):
    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        value = ray.train.collective.broadcast_from_rank_zero({"key": rank})
        assert value == {"key": 0}

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    trainer.fit()


def test_broadcast_from_rank_zero_data_too_big(ray_start_4_cpus):
    def train_fn():
        collectives.logger = mock.create_autospec(collectives.logger, instance=True)
        collectives._MAX_BROADCAST_SIZE_BYTES = 0
        rank = ray.train.get_context().get_world_rank()
        value = ray.train.collective.broadcast_from_rank_zero({"key": rank})
        assert value == {"key": 0}
        collectives.logger.warning.assert_called_once()

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
