from unittest.mock import patch
import numpy as np
import os
import pytest
import time
import torch
import torch.nn as nn
import torch.distributed as dist
from torch.utils.data import DataLoader

import ray
from ray import tune
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


@pytest.mark.parametrize("use_local", [False, True])
def test_resize(ray_start_2_cpus, use_local):  # noqa: F811
    if not dist.is_available():
        return

    def single_loader(config):
        dataset = LinearDataset(2, 5, size=1000000)
        return DataLoader(dataset, batch_size=config.get("batch_size", 32))

    step_with_fail = gen_step_with_fail(1)

    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        single_loader,
        loss_creator=lambda config: nn.MSELoss())
    with patch.object(RemoteWorkerGroup, "_train", step_with_fail):
        trainer1 = TorchTrainer(
            training_operator_cls=TestOperator,
            config={"batch_size": 100000},
            use_local=use_local,
            backend="gloo",
            num_workers=2)

        @ray.remote
        def try_test():
            import time
            time.sleep(100)

        try_test.remote()
        trainer1.train(max_retries=1)
        assert len(trainer1.worker_group.remote_workers) == 1

        trainer1.shutdown()


@pytest.mark.parametrize("use_local", [False, True])
def test_fail_twice(ray_start_2_cpus, use_local):  # noqa: F811
    if not dist.is_available():
        return

    def single_loader(config):
        dataset = LinearDataset(2, 5, size=1000000)
        return DataLoader(dataset, batch_size=config.get("batch_size", 32))

    step_with_fail = gen_step_with_fail(2)

    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        single_loader,
        loss_creator=lambda config: nn.MSELoss())

    with patch.object(RemoteWorkerGroup, "_train", step_with_fail):
        trainer1 = TorchTrainer(
            training_operator_cls=TestOperator,
            config={"batch_size": 100000},
            use_local=use_local,
            backend="gloo",
            num_workers=2)

        # MAX RETRIES SHOULD BE ON BY DEFAULT
        trainer1.train()
        trainer1.shutdown()


def gen_step_with_fail(num_fails):
    def train_with_fail(self, num_steps, profile, info, dataset=None):
        if not hasattr(self, "_num_failures"):
            self._num_failures = 0
        remote_worker_stats = []
        for i, w in enumerate(self.remote_workers):
            params = dict(num_steps=num_steps, profile=profile, info=info)
            if dataset:
                params["iterator"] = dataset.get_shard(i)
            stats = w.train_epoch.remote(**params)
            remote_worker_stats.append(stats)
        if self._num_failures < num_fails:
            time.sleep(1)
            ray.kill(self.remote_workers[0])
            self._num_failures += 1
        return remote_worker_stats

    return train_with_fail


@pytest.mark.parametrize("use_local", [False, True])
def test_fail_with_recover(ray_start_2_cpus, use_local):  # noqa: F811
    print(locals())
    if not dist.is_available():
        return

    def single_loader(config):
        dataset = LinearDataset(2, 5, size=1000000)
        return DataLoader(dataset, batch_size=config.get("batch_size", 32))

    step_with_fail = gen_step_with_fail(3)
    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        single_loader,
        loss_creator=lambda config: nn.MSELoss())
    with patch.object(RemoteWorkerGroup, "_train", step_with_fail):
        trainer1 = TorchTrainer(
            training_operator_cls=TestOperator,
            config={"batch_size": 100000},
            timeout_s=5,
            backend="gloo",
            use_local=use_local,
            num_workers=2)

        with pytest.raises(RuntimeError):
            trainer1.train(max_retries=1)

        trainer1.shutdown(force=True)


def test_wrap_ddp(ray_start_2_cpus, tmp_path):  # noqa: F811
    if not dist.is_available():
        return
    trainer1 = TorchTrainer(
        training_operator_cls=Operator,
        backend="gloo",
        use_gpu=False,
        wrap_ddp=False,
        num_workers=2,
        use_local=True)
    trainer1.train()
    checkpoint_path = os.path.join(tmp_path, "checkpoint")
    trainer1.save(checkpoint_path)

    model1 = trainer1.get_model()
    trainer1.shutdown()

    trainer2 = TorchTrainer(
        training_operator_cls=Operator,
        backend="gloo",
        use_gpu=False,
        wrap_ddp=False,
        num_workers=2)
    trainer2.load(checkpoint_path)

    model2 = trainer2.get_model()

    model1_state_dict = model1.state_dict()
    model2_state_dict = model2.state_dict()

    assert set(model1_state_dict.keys()) == set(model2_state_dict.keys())

    for k in model1_state_dict:
        assert torch.equal(model1_state_dict[k], model2_state_dict[k])
    trainer2.shutdown()
