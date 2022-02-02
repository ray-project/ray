import os

import numpy as np
import pytest
import torch
import torch.distributed as dist
import torch.nn as nn
from torch.utils.data import DataLoader

import ray
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.torch.constants import SCHEDULER_STEP
from ray.util.sgd.torch.examples.train_example import (
    model_creator,
    optimizer_creator,
    data_creator,
    LinearDataset,
)
from ray.util.sgd.torch.training_operator import (
    get_test_operator,
    get_test_metrics_operator,
    TrainingOperator,
)
from ray.util.sgd.utils import NUM_SAMPLES, BATCH_COUNT, BATCH_SIZE


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


Operator = TrainingOperator.from_creators(
    model_creator, optimizer_creator, data_creator, loss_creator=nn.MSELoss
)


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_apply_all_workers(ray_start_2_cpus, num_workers, use_local):
    def fn():
        return 1

    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        use_local=use_local,
        use_gpu=False,
    )

    results = trainer.apply_all_workers(fn)
    assert all(x == 1 for x in results)

    trainer.shutdown()


@pytest.mark.parametrize("scheduler_freq", ["epoch", "batch", "manual", None])
def test_scheduler_freq(ray_start_2_cpus, scheduler_freq):  # noqa:
    # F811
    def train_epoch(self, iterator, info):
        assert info[SCHEDULER_STEP] == scheduler_freq
        return {"done": 1}

    def scheduler_creator(optimizer, config):
        return torch.optim.lr_scheduler.StepLR(optimizer, step_size=30, gamma=0.1)

    class TestTrainingOperator(TrainingOperator):
        def setup(self, config):
            model = model_creator(config)
            optimizer = optimizer_creator(model, config)
            train_loader, val_loader = data_creator(config)
            scheduler = scheduler_creator(optimizer, config)
            loss = nn.MSELoss()

            self.model, self.optimizer, self.criterion, self.scheduler = self.register(
                models=model, optimizers=optimizer, criterion=loss, schedulers=scheduler
            )
            self.register_data(train_loader=train_loader, validation_loader=val_loader)

    if scheduler_freq is None:
        with pytest.raises(ValueError):
            trainer = TorchTrainer(
                config={"custom_func": train_epoch},
                training_operator_cls=TestTrainingOperator,
                scheduler_step_freq=scheduler_freq,
            )
    else:
        trainer = TorchTrainer(
            config={"custom_func": train_epoch},
            training_operator_cls=TestTrainingOperator,
            scheduler_step_freq=scheduler_freq,
        )

        for i in range(3):
            trainer.train()
        trainer.shutdown()


@pytest.mark.parametrize("use_local", [True, False])
def test_profiling(ray_start_2_cpus, use_local):  # noqa: F811
    trainer = TorchTrainer(training_operator_cls=Operator, use_local=use_local)

    stats = trainer.train(profile=True)
    assert "profile" in stats
    stats = trainer.validate(profile=True)
    assert "profile" in stats
    trainer.shutdown()


@pytest.mark.parametrize("use_local", [True, False])
def test_split_batch(ray_start_2_cpus, use_local):
    if not dist.is_available():
        return

    def data_creator(config):
        """Returns training dataloader, validation dataloader."""
        train_dataset = LinearDataset(2, 5, size=config["data_size"])
        return DataLoader(
            train_dataset,
            batch_size=config[BATCH_SIZE],
        )

    data_size = 600
    batch_size = 21
    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        data_creator,
        loss_creator=lambda config: nn.MSELoss(),
    )
    trainer = TorchTrainer(
        training_operator_cls=TestOperator,
        num_workers=2,
        use_local=use_local,
        config={
            BATCH_SIZE: batch_size,
            "data_size": data_size,
        },
    )
    stats = trainer.train()
    assert trainer.config[BATCH_SIZE] == (batch_size - 1)
    assert stats[NUM_SAMPLES] == 600
    assert stats[BATCH_COUNT] == (data_size // 20)
    trainer.shutdown()


@pytest.mark.parametrize("use_local", [True, False])
def test_reduce_result(ray_start_2_cpus, use_local):
    if not dist.is_available():
        return

    def data_creator(config):
        """Returns training dataloader, validation dataloader."""
        train_dataset = LinearDataset(2, 5, size=config["data_size"])
        test_dataset = LinearDataset(2, 5, size=config["data_size"])
        return DataLoader(train_dataset, batch_size=1), DataLoader(
            test_dataset, batch_size=1
        )

    data_size = 600

    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        data_creator,
        loss_creator=lambda config: nn.MSELoss(),
    )
    trainer = TorchTrainer(
        training_operator_cls=TestOperator,
        num_workers=2,
        use_local=use_local,
        config={"data_size": data_size},
    )
    list_stats = trainer.train(reduce_results=False, profile=True)
    assert len(list_stats) == 2
    assert [stats[NUM_SAMPLES] == data_size for stats in list_stats]
    assert [stats[BATCH_COUNT] == (data_size // 2) for stats in list_stats]
    list_stats = trainer.validate(reduce_results=False, profile=True)
    assert len(list_stats) == 2
    assert [stats[NUM_SAMPLES] == data_size for stats in list_stats]
    assert [stats[BATCH_COUNT] == (data_size // 2) for stats in list_stats]
    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_metrics(ray_start_2_cpus, num_workers, use_local):
    data_size, val_size = 600, 500
    batch_size = 4

    num_train_steps = int(data_size / batch_size)
    num_val_steps = int(val_size / batch_size)

    train_scores = [1] + ([0] * num_train_steps)
    val_scores = [1] + ([0] * num_val_steps)

    TestOperator = get_test_metrics_operator(Operator)
    trainer = TorchTrainer(
        training_operator_cls=TestOperator,
        num_workers=num_workers,
        use_local=use_local,
        config={
            "scores": train_scores,
            "val_scores": val_scores,
            "key": "score",
            "batch_size": batch_size,
            "data_size": data_size,
            "val_size": val_size,
        },
    )

    stats = trainer.train()
    # Test that we output mean and last of custom metrics in an epoch
    assert "score" in stats
    assert stats["last_score"] == 0

    assert stats[NUM_SAMPLES] == num_train_steps * batch_size
    expected_score = num_workers * (sum(train_scores) / (num_train_steps * batch_size))
    assert np.allclose(stats["score"], expected_score)

    val_stats = trainer.validate()
    # Test that we output mean and last of custom metrics in validation
    assert val_stats["last_score"] == 0
    expected_score = (sum(val_scores) / (num_val_steps * batch_size)) * num_workers
    assert np.allclose(val_stats["score"], expected_score)
    assert val_stats[BATCH_COUNT] == np.ceil(num_val_steps / num_workers)
    assert val_stats[NUM_SAMPLES] == num_val_steps * batch_size
    assert val_stats[NUM_SAMPLES] == val_size

    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_metrics_nan(ray_start_2_cpus, num_workers, use_local):
    data_size, val_size = 100, 100
    batch_size = 10

    num_train_steps = int(data_size / batch_size)
    num_val_steps = int(val_size / batch_size)

    train_scores = [np.nan] + ([0] * num_train_steps)
    val_scores = [np.nan] + ([0] * num_val_steps)
    TestOperator = get_test_metrics_operator(Operator)
    trainer = TorchTrainer(
        training_operator_cls=TestOperator,
        num_workers=num_workers,
        use_local=use_local,
        config={
            "scores": train_scores,
            "val_scores": val_scores,
            "key": "score",
            "batch_size": batch_size,
            "data_size": data_size,
            "val_size": val_size,
        },
    )

    stats = trainer.train(num_steps=num_train_steps)
    assert "score" in stats
    assert stats["last_score"] == 0
    assert np.isnan(stats["score"])

    stats = trainer.validate()
    assert "score" in stats
    assert stats["last_score"] == 0
    assert np.isnan(stats["score"])
    trainer.shutdown()


@pytest.mark.parametrize("use_local", [True, False])
def test_scheduler_validate(ray_start_2_cpus, use_local):  # noqa: F811
    from torch.optim.lr_scheduler import ReduceLROnPlateau

    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        data_creator,
        scheduler_creator=lambda optimizer, cfg: ReduceLROnPlateau(optimizer),
        loss_creator=lambda config: nn.MSELoss(),
    )
    TestOperator = get_test_operator(TestOperator)
    trainer = TorchTrainer(
        scheduler_step_freq="manual",
        training_operator_cls=TestOperator,
        use_local=use_local,
    )
    trainer.update_scheduler(0.5)
    trainer.update_scheduler(0.5)
    assert all(
        trainer.apply_all_operators(lambda op: op._schedulers[0].last_epoch == 2)
    )
    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_save_and_restore(
    ray_start_2_cpus, num_workers, use_local, tmp_path
):  # noqa: F811
    trainer1 = TorchTrainer(
        training_operator_cls=Operator, num_workers=num_workers, use_local=use_local
    )
    trainer1.train()
    checkpoint_path = os.path.join(tmp_path, "checkpoint")
    trainer1.save(checkpoint_path)

    model1 = trainer1.get_model()

    trainer1.shutdown()

    trainer2 = TorchTrainer(
        training_operator_cls=Operator, num_workers=num_workers, use_local=use_local
    )
    trainer2.load(checkpoint_path)

    model2 = trainer2.get_model()

    model1_state_dict = model1.state_dict()
    model2_state_dict = model2.state_dict()

    assert set(model1_state_dict.keys()) == set(model2_state_dict.keys())

    for k in model1_state_dict:
        assert torch.equal(model1_state_dict[k], model2_state_dict[k])
    trainer2.shutdown()


def test_wrap_ddp(ray_start_2_cpus, tmp_path):  # noqa: F811
    if not dist.is_available():
        return
    trainer1 = TorchTrainer(
        training_operator_cls=Operator, wrap_ddp=False, num_workers=2, use_local=True
    )
    trainer1.train()
    checkpoint_path = os.path.join(tmp_path, "checkpoint")
    trainer1.save(checkpoint_path)

    model1 = trainer1.get_model()
    trainer1.shutdown()

    trainer2 = TorchTrainer(
        training_operator_cls=Operator, wrap_ddp=False, num_workers=2
    )
    trainer2.load(checkpoint_path)

    model2 = trainer2.get_model()

    model1_state_dict = model1.state_dict()
    model2_state_dict = model2.state_dict()

    assert set(model1_state_dict.keys()) == set(model2_state_dict.keys())

    for k in model1_state_dict:
        assert torch.equal(model1_state_dict[k], model2_state_dict[k])
    trainer2.shutdown()


def test_custom_ddp_args(ray_start_2_cpus):
    class TestTrainingOperator(TrainingOperator):
        def setup(self, config):
            model = model_creator(config)
            optimizer = optimizer_creator(model, config)
            train_loader, val_loader = data_creator(config)

            self.model, self.optimizer, = self.register(
                models=model,
                optimizers=optimizer,
                ddp_args={"find_unused_parameters": True},
            )
            assert self.model.find_unused_parameters

    TorchTrainer(training_operator_cls=TestTrainingOperator, num_workers=2)


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_iterable_model(ray_start_2_cpus, num_workers, use_local):  # noqa: F811
    class IterableOptimizer(torch.optim.SGD):
        def __iter__(self):
            return self.param_groups

    class Operator(TrainingOperator):
        def setup(self, config):
            model = nn.Sequential(nn.Linear(1, config.get("hidden_size", 1)))
            optimizer = IterableOptimizer(model.parameters(), lr=config.get("lr", 1e-2))
            criterion = nn.MSELoss()

            self.model, self.optimizer, self.criterion = self.register(
                models=model, optimizers=optimizer, criterion=criterion
            )
            train_ld, val_ld = data_creator(config)
            self.register_data(train_loader=train_ld, validation_loader=val_ld)

    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        use_local=use_local,
        use_gpu=False,
    )
    for i in range(3):
        train_loss1 = trainer.train()["train_loss"]
    validation_loss1 = trainer.validate()["val_loss"]

    for i in range(3):
        train_loss2 = trainer.train()["train_loss"]
    validation_loss2 = trainer.validate()["val_loss"]

    assert train_loss2 <= train_loss1, (train_loss2, train_loss1)
    assert validation_loss2 <= validation_loss1, (validation_loss2, validation_loss1)
    trainer.shutdown()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
