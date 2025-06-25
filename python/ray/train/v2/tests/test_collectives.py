import pytest

import ray
import ray.train.collectives
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer


def test_barrier():
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
        ray.train.collectives.barrier()
        assert ray.get(counter.get_num_reached_barrier.remote()) == 2

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    trainer.fit()


def test_broadcast_from_rank_zero():
    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        value = ray.train.collectives.broadcast_from_rank_zero({"key": rank})
        assert value == {"key": 0}

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
