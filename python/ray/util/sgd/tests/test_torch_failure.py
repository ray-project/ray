from unittest.mock import patch
import pytest
import time
import torch.nn as nn
import torch.distributed as dist
from torch.utils.data import DataLoader

import ray
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.torch.worker_group import RemoteWorkerGroup
from ray.util.sgd.torch.training_operator import TrainingOperator

from ray.util.sgd.torch.examples.train_example import (
    model_creator, optimizer_creator, data_creator, LinearDataset)

Operator = TrainingOperator.from_creators(
    model_creator, optimizer_creator, data_creator, loss_creator=nn.MSELoss)


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Ensure that tests don't ALL fail
    if dist.is_initialized():
        dist.destroy_process_group()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Ensure that tests don't ALL fail
    if dist.is_initialized():
        dist.destroy_process_group()


def remote_worker_train_with_fail(self, num_steps, profile, info,
                                  dataset=None):
    remote_worker_stats = []
    for i, w in enumerate(self.remote_workers):
        params = dict(num_steps=num_steps, profile=profile, info=info)
        if dataset:
            params["iterator"] = dataset.get_shard(i)
        stats = w.train_epoch.remote(**params)
        remote_worker_stats.append(stats)
        if i == 0 and hasattr(self, "should_fail") and self.should_fail:
            time.sleep(1)
            ray.kill(self.remote_workers[i])
    return remote_worker_stats


start_workers = TorchTrainer._start_workers


def gen_start_with_fail(num_fails):
    def start_with_fail(self, *args, **kwargs):
        start_workers(self, *args, **kwargs)
        fail = self._num_failures < num_fails
        if self.use_local:
            self.worker_group.remote_worker_group.should_fail = fail
        else:
            self.worker_group.should_fail = fail

    return start_with_fail


@pytest.mark.parametrize("use_local", [False, True])
@patch.object(RemoteWorkerGroup, "_train", remote_worker_train_with_fail)
def test_resize(ray_start_2_cpus, use_local):  # noqa: F811
    if not dist.is_available():
        return

    def single_loader(config):
        dataset = LinearDataset(2, 5, size=1000000)
        return DataLoader(dataset, batch_size=config.get("batch_size", 32))

    start_with_fail = gen_start_with_fail(1)

    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        single_loader,
        loss_creator=lambda config: nn.MSELoss())
    with patch.object(TorchTrainer, "_start_workers", start_with_fail):
        trainer1 = TorchTrainer(
            training_operator_cls=TestOperator,
            config={"batch_size": 100000},
            use_local=use_local,
            num_workers=2)

        @ray.remote(num_cpus=1)
        class DummyActor:
            def get(self):
                return 1

        dummy_handler = DummyActor.remote()
        trainer1.train(max_retries=1)
        assert trainer1.worker_group.num_workers == 1
        assert trainer1._num_failures == 1

        ray.get(dummy_handler.get.remote())
        ray.kill(dummy_handler)
        time.sleep(1)
        # trigger scale up
        trainer1.train()
        assert trainer1.worker_group.num_workers == 2

        trainer1.shutdown(force=True)


@pytest.mark.parametrize("use_local", [False, True])
@patch.object(RemoteWorkerGroup, "_train", remote_worker_train_with_fail)
def test_fail_twice(ray_start_2_cpus, use_local):  # noqa: F811
    if not dist.is_available():
        return

    def single_loader(config):
        dataset = LinearDataset(2, 5, size=1000000)
        return DataLoader(dataset, batch_size=config.get("batch_size", 32))

    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        single_loader,
        loss_creator=lambda config: nn.MSELoss())

    start_with_fail = gen_start_with_fail(2)

    with patch.object(TorchTrainer, "_start_workers", start_with_fail):
        trainer1 = TorchTrainer(
            training_operator_cls=TestOperator,
            config={"batch_size": 100000},
            use_local=use_local,
            num_workers=2)

        # MAX RETRIES SHOULD BE ON BY DEFAULT
        trainer1.train()
        assert trainer1._num_failures == 2
        assert trainer1.worker_group.num_workers == 2
        trainer1.shutdown(force=True)


@pytest.mark.parametrize("use_local", [False, True])
@patch.object(RemoteWorkerGroup, "_train", remote_worker_train_with_fail)
def test_fail_with_recover(ray_start_2_cpus, use_local):  # noqa: F811
    print(locals())
    if not dist.is_available():
        return

    def single_loader(config):
        dataset = LinearDataset(2, 5, size=1000000)
        return DataLoader(dataset, batch_size=config.get("batch_size", 32))

    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        single_loader,
        loss_creator=lambda config: nn.MSELoss())

    start_with_fail = gen_start_with_fail(3)

    with patch.object(TorchTrainer, "_start_workers", start_with_fail):
        trainer1 = TorchTrainer(
            training_operator_cls=TestOperator,
            config={"batch_size": 100000},
            timeout_s=5,
            use_local=use_local,
            num_workers=2)

        with pytest.raises(RuntimeError):
            trainer1.train(max_retries=1)

        trainer1.shutdown(force=True)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
