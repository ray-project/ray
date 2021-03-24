import os

import numpy as np
import pytest
import torch
import torch.distributed as dist
import torch.nn as nn
from torch.utils.data import DataLoader

import ray
import ray.util.data as ml_data
import ray.util.iter as parallel_it
from ray import tune
from ray.tune.utils import merge_dicts
from ray.util.data.examples.mlp_identity_torch import make_train_operator
from ray.util.sgd.data.examples import mlp_identity
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.torch.constants import SCHEDULER_STEP
from ray.util.sgd.torch.examples.train_example import (
    model_creator, optimizer_creator, data_creator, LinearDataset)
from ray.util.sgd.torch.training_operator import (
    get_test_operator, get_test_metrics_operator, TrainingOperator)
from ray.util.sgd.utils import (NUM_SAMPLES, BATCH_COUNT, BATCH_SIZE)


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
    model_creator, optimizer_creator, data_creator, loss_creator=nn.MSELoss)


@pytest.mark.parametrize("use_local", [True, False])
def test_single_step(ray_start_2_cpus, use_local):  # noqa: F811
    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=1,
        use_local=use_local,
        use_gpu=False)
    metrics = trainer.train(num_steps=1)
    assert metrics[BATCH_COUNT] == 1

    val_metrics = trainer.validate(num_steps=1)
    assert val_metrics[BATCH_COUNT] == 1
    trainer.shutdown()


@pytest.mark.parametrize("use_local", [True, False])
def test_dead_trainer(ray_start_2_cpus, use_local):  # noqa: F811
    TestOperator = get_test_operator(Operator)
    trainer = TorchTrainer(
        training_operator_cls=TestOperator,
        num_workers=2,
        use_local=use_local,
        use_gpu=False)
    trainer.train(num_steps=1)
    trainer.shutdown()
    with pytest.raises(RuntimeError):
        trainer.train()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_train(ray_start_2_cpus, num_workers, use_local):  # noqa: F811
    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        use_local=use_local,
        use_gpu=False)
    for i in range(3):
        train_loss1 = trainer.train()["train_loss"]
    validation_loss1 = trainer.validate()["val_loss"]

    for i in range(3):
        train_loss2 = trainer.train()["train_loss"]
    validation_loss2 = trainer.validate()["val_loss"]

    assert train_loss2 <= train_loss1, (train_loss2, train_loss1)
    assert validation_loss2 <= validation_loss1, (validation_loss2,
                                                  validation_loss1)
    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_apply_all_workers(ray_start_2_cpus, num_workers, use_local):
    def fn():
        return 1

    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        use_local=use_local,
        use_gpu=False)

    results = trainer.apply_all_workers(fn)
    assert all(x == 1 for x in results)

    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_multi_model(ray_start_2_cpus, num_workers, use_local):
    def train(*, model=None, criterion=None, optimizer=None, iterator=None):
        model.train()
        train_loss = 0
        correct = 0
        total = 0
        for batch_idx, (inputs, targets) in enumerate(iterator):
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()

            train_loss += loss.item()
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += predicted.eq(targets).sum().item()
        return {
            "accuracy": correct / total,
            "train_loss": train_loss / (batch_idx + 1)
        }

    def train_epoch(self, iterator, info):
        result = {}
        data = list(iterator)
        for i, (model, optimizer) in enumerate(
                zip(self.models, self.optimizers)):
            result[f"model_{i}"] = train(
                model=model,
                criterion=self.criterion,
                optimizer=optimizer,
                iterator=iter(data))
        return result

    class MultiModelOperator(TrainingOperator):
        def setup(self, config):
            models = nn.Linear(1, 1), nn.Linear(1, 1)
            opts = [
                torch.optim.SGD(model.parameters(), lr=0.0001)
                for model in models
            ]
            loss = nn.MSELoss()
            train_dataloader, val_dataloader = data_creator(config)
            self.models, self.optimizers, self.criterion = self.register(
                models=models, optimizers=opts, criterion=loss)
            self.register_data(
                train_loader=train_dataloader,
                validation_loader=val_dataloader)

    TestOperator = get_test_operator(MultiModelOperator)

    trainer1 = TorchTrainer(
        config={"custom_func": train_epoch},
        training_operator_cls=TestOperator,
        num_workers=num_workers,
        use_local=use_local,
        use_gpu=False,
    )
    trainer1.train()
    state = trainer1.state_dict()

    models1 = trainer1.get_model()

    trainer1.shutdown()

    trainer2 = TorchTrainer(
        config={"custom_func": train_epoch},
        training_operator_cls=TestOperator,
        num_workers=num_workers,
        use_local=use_local,
        use_gpu=False,
    )
    trainer2.load_state_dict(state)

    models2 = trainer2.get_model()

    for model_1, model_2 in zip(models1, models2):

        model1_state_dict = model_1.state_dict()
        model2_state_dict = model_2.state_dict()

        assert set(model1_state_dict.keys()) == set(model2_state_dict.keys())

        for k in model1_state_dict:
            assert torch.equal(model1_state_dict[k], model2_state_dict[k])

    trainer2.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_multi_model_matrix(ray_start_2_cpus, num_workers, use_local):  #
    # noqa: F811
    def train_epoch(self, iterator, info):
        if self.config.get("models", 1) > 1:
            assert len(self.models) == self.config["models"], self.config

        if self.config.get("optimizers", 1) > 1:
            assert len(
                self.optimizers) == self.config["optimizers"], self.config

        if self.config.get("schedulers", 1) > 1:
            assert len(
                self.schedulers) == self.config["schedulers"], self.config
        return {"done": 1}

    def multi_model_creator(config):
        models = []
        for i in range(config.get("models", 1)):
            models += [nn.Linear(1, 1)]
        return models[0] if len(models) == 1 else models

    def multi_optimizer_creator(models, config):
        optimizers = []
        main_model = models[0] if type(models) is list else models
        for i in range(config.get("optimizers", 1)):
            optimizers += [torch.optim.SGD(main_model.parameters(), lr=0.0001)]
        return optimizers[0] if len(optimizers) == 1 else optimizers

    def multi_scheduler_creator(optimizer, config):
        schedulers = []
        main_opt = optimizer[0] if type(optimizer) is list else optimizer
        for i in range(config.get("schedulers", 1)):
            schedulers += [
                torch.optim.lr_scheduler.StepLR(
                    main_opt, step_size=30, gamma=0.1)
            ]
        return schedulers[0] if len(schedulers) == 1 else schedulers

    class MultiModelOperator(TrainingOperator):
        def setup(self, config):
            models = multi_model_creator(config)
            optimizers = multi_optimizer_creator(models, config)
            schedulers = multi_scheduler_creator(optimizers, config)
            train_loader, val_loader = data_creator(config)
            loss = nn.MSELoss()

            self.models, self.optimizers, self.criterion, self.schedulers = \
                self.register(models=models, optimizers=optimizers,
                              schedulers=schedulers,
                              criterion=loss)
            self.register_data(
                train_loader=train_loader, validation_loader=val_loader)

    TestOperator = get_test_operator(MultiModelOperator)

    for model_count in range(1, 3):
        for optimizer_count in range(1, 3):
            for scheduler_count in range(1, 3):
                trainer = TorchTrainer(
                    scheduler_step_freq="epoch",
                    training_operator_cls=TestOperator,
                    num_workers=num_workers,
                    use_local=use_local,
                    config={
                        "models": model_count,
                        "optimizers": optimizer_count,
                        "schedulers": scheduler_count,
                        "custom_func": train_epoch
                    })
                trainer.train()
                trainer.shutdown()


@pytest.mark.parametrize("scheduler_freq", ["epoch", "batch", "manual", None])
def test_scheduler_freq(ray_start_2_cpus, scheduler_freq):  # noqa:
    # F811
    def train_epoch(self, iterator, info):
        assert info[SCHEDULER_STEP] == scheduler_freq
        return {"done": 1}

    def scheduler_creator(optimizer, config):
        return torch.optim.lr_scheduler.StepLR(
            optimizer, step_size=30, gamma=0.1)

    class TestTrainingOperator(TrainingOperator):
        def setup(self, config):
            model = model_creator(config)
            optimizer = optimizer_creator(model, config)
            train_loader, val_loader = data_creator(config)
            scheduler = scheduler_creator(optimizer, config)
            loss = nn.MSELoss()

            self.model, self.optimizer, self.criterion, self.scheduler = \
                self.register(
                    models=model, optimizers=optimizer,
                    criterion=loss, schedulers=scheduler)
            self.register_data(
                train_loader=train_loader, validation_loader=val_loader)

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
def test_dataset(ray_start_4_cpus, use_local):
    """
    This test tries training the mlp_identity example. We check the accuracy of
    the model as an all inclusive way of ensuring that we are properly sharding
    and iterating over the entire dataset (instead of repeating the first set
    of points for example).
    """

    model_creator = mlp_identity.model_creator
    optimizer_creator = mlp_identity.optimizer_creator
    dataset_creator = mlp_identity.dataset_creator

    DatasetOperator = TrainingOperator.from_creators(
        model_creator=model_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=nn.MSELoss)

    trainer = TorchTrainer(
        training_operator_cls=DatasetOperator,
        use_local=use_local,
        num_workers=2,
    )

    dataset = dataset_creator()
    for i in range(5):
        trainer.train(dataset=dataset, num_steps=100)

    x = mlp_identity.to_mat(0.5)
    prediction = float(trainer.get_model()(x)[0][0])
    assert 0.4 <= prediction <= 0.6
    trainer.shutdown()


@pytest.mark.parametrize("use_local", [True, False])
def test_num_steps(ray_start_2_cpus, use_local):
    """Tests if num_steps continues training from the subsampled dataset."""

    def data_creator(config):
        train_dataset = [0] * 5 + [1] * 5
        val_dataset = [0] * 5 + [1] * 5
        return DataLoader(train_dataset, batch_size=config["batch_size"]), \
            DataLoader(val_dataset, batch_size=config["batch_size"])

    batch_size = 1
    Operator = TrainingOperator.from_creators(model_creator, optimizer_creator,
                                              data_creator)

    def train_func(self, iterator, info=None):
        total_sum = 0
        num_items = 0
        for e in iterator:
            total_sum += e
            num_items += 1
        return {"average": total_sum.item() / num_items}

    TestOperator = get_test_operator(Operator)
    trainer = TorchTrainer(
        training_operator_cls=TestOperator,
        num_workers=2,
        use_local=use_local,
        add_dist_sampler=False,
        config={
            "batch_size": batch_size,
            "custom_func": train_func
        })

    # If num_steps not passed, should do one full epoch.
    result = trainer.train()
    # Average of 5 0s and 5 1s
    assert result["average"] == 0.5
    assert result["epoch"] == 1
    val_result = trainer.validate()
    assert val_result["average"] == 0.5

    # Train again with num_steps.
    result = trainer.train(num_steps=5)
    # 5 zeros
    assert result["average"] == 0
    assert result["epoch"] == 2
    val_result = trainer.validate(num_steps=5)
    assert val_result["average"] == 0

    # Should continue where last train run left off.
    result = trainer.train(num_steps=3)
    # 3 ones.
    assert result["average"] == 1
    assert result["epoch"] == 2
    val_result = trainer.validate(num_steps=3)
    assert val_result["average"] == 1

    # Should continue from last train run, and cycle to beginning.
    result = trainer.train(num_steps=5)
    # 2 ones and 3 zeros.
    assert result["average"] == 0.4
    assert result["epoch"] == 3
    val_result = trainer.validate(num_steps=5)
    assert val_result["average"] == 0.4

    # Should continue, and since num_steps not passed in, just finishes epoch.
    result = trainer.train()
    # 2 zeros and 5 ones.
    assert result["average"] == 5 / 7
    assert result["epoch"] == 3
    val_result = trainer.validate()
    assert val_result["average"] == 5 / 7

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
        loss_creator=lambda config: nn.MSELoss())
    trainer = TorchTrainer(
        training_operator_cls=TestOperator,
        num_workers=2,
        use_local=use_local,
        config={
            BATCH_SIZE: batch_size,
            "data_size": data_size,
        })
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
        return DataLoader(
            train_dataset, batch_size=1), DataLoader(
                test_dataset, batch_size=1)

    data_size = 600

    TestOperator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        data_creator,
        loss_creator=lambda config: nn.MSELoss())
    trainer = TorchTrainer(
        training_operator_cls=TestOperator,
        num_workers=2,
        use_local=use_local,
        config={"data_size": data_size})
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
            "val_size": val_size
        })

    stats = trainer.train()
    # Test that we output mean and last of custom metrics in an epoch
    assert "score" in stats
    assert stats["last_score"] == 0

    assert stats[NUM_SAMPLES] == num_train_steps * batch_size
    expected_score = num_workers * (sum(train_scores) /
                                    (num_train_steps * batch_size))
    assert np.allclose(stats["score"], expected_score)

    val_stats = trainer.validate()
    # Test that we output mean and last of custom metrics in validation
    assert val_stats["last_score"] == 0
    expected_score = (sum(val_scores) /
                      (num_val_steps * batch_size)) * num_workers
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
            "val_size": val_size
        })

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
        loss_creator=lambda config: nn.MSELoss())
    TestOperator = get_test_operator(TestOperator)
    trainer = TorchTrainer(
        scheduler_step_freq="manual",
        training_operator_cls=TestOperator,
        use_local=use_local)
    trainer.update_scheduler(0.5)
    trainer.update_scheduler(0.5)
    assert all(
        trainer.apply_all_operators(
            lambda op: op._schedulers[0].last_epoch == 2))
    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_tune_train(ray_start_4_cpus, num_workers, use_local):  # noqa: F811
    TorchTrainable = TorchTrainer.as_trainable(
        **{
            "training_operator_cls": Operator,
            "num_workers": num_workers,
            "use_gpu": False,
            "backend": "gloo",
            "use_local": use_local,
            "config": {
                "batch_size": 512,
                "lr": 0.001
            }
        })

    analysis = tune.run(
        TorchTrainable,
        num_samples=2,
        stop={"training_iteration": 2},
        verbose=1)

    # checks loss decreasing for every trials
    for path, df in analysis.trial_dataframes.items():
        mean_train_loss1 = df.loc[0, "train_loss"]
        mean_train_loss2 = df.loc[1, "train_loss"]
        mean_val_loss1 = df.loc[0, "val_loss"]
        mean_val_loss2 = df.loc[1, "val_loss"]

        assert mean_train_loss2 <= mean_train_loss1
        assert mean_val_loss2 <= mean_val_loss1


@pytest.mark.parametrize("num_workers", [2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_tune_custom_train(ray_start_4_cpus, num_workers,
                           use_local):  # noqa: F811
    def custom_train_func(trainer, info):
        train_stats = trainer.train(profile=True)
        val_stats = trainer.validate(profile=True)
        stats = merge_dicts(train_stats, val_stats)
        return stats

    TorchTrainable = TorchTrainer.as_trainable(
        **{
            "override_tune_step": custom_train_func,
            "training_operator_cls": Operator,
            "num_workers": num_workers,
            "use_gpu": False,
            "backend": "gloo",
            "use_local": use_local,
            "config": {
                "batch_size": 512,
                "lr": 0.001
            }
        })

    analysis = tune.run(
        TorchTrainable,
        num_samples=2,
        stop={"training_iteration": 2},
        verbose=1)

    # checks loss decreasing for every trials
    for path, df in analysis.trial_dataframes.items():
        mean_train_loss1 = df.loc[0, "train_loss"]
        mean_train_loss2 = df.loc[1, "train_loss"]
        mean_val_loss1 = df.loc[0, "val_loss"]
        mean_val_loss2 = df.loc[1, "val_loss"]

        assert mean_train_loss2 <= mean_train_loss1
        assert mean_val_loss2 <= mean_val_loss1


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_save_and_restore(ray_start_2_cpus, num_workers, use_local,
                          tmp_path):  # noqa: F811
    trainer1 = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        use_local=use_local)
    trainer1.train()
    checkpoint_path = os.path.join(tmp_path, "checkpoint")
    trainer1.save(checkpoint_path)

    model1 = trainer1.get_model()

    trainer1.shutdown()

    trainer2 = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        use_local=use_local)
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
        training_operator_cls=Operator,
        wrap_ddp=False,
        num_workers=2,
        use_local=True)
    trainer1.train()
    checkpoint_path = os.path.join(tmp_path, "checkpoint")
    trainer1.save(checkpoint_path)

    model1 = trainer1.get_model()
    trainer1.shutdown()

    trainer2 = TorchTrainer(
        training_operator_cls=Operator, wrap_ddp=False, num_workers=2)
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

            self.model, self.optimizer, = \
                self.register(
                    models=model, optimizers=optimizer, ddp_args={
                        "find_unused_parameters": True})
            assert self.model.find_unused_parameters

    TorchTrainer(training_operator_cls=TestTrainingOperator, num_workers=2)


@pytest.mark.parametrize("use_local", [True, False])
def test_multi_input_model(ray_start_2_cpus, use_local):
    def model_creator(config):
        class MultiInputModel(nn.Module):
            def __init__(self):
                super(MultiInputModel, self).__init__()
                self._fc1 = torch.nn.Linear(1, 1)
                self._fc2 = torch.nn.Linear(1, 1)

            def forward(self, x, y):
                return self._fc1(x) + self._fc2(y)

        return MultiInputModel()

    def data_creator(config):
        class LinearDataset(torch.utils.data.Dataset):
            def __init__(self, a, b, size=1000):
                x = np.random.randn(size)
                y = np.random.randn(size)
                self.x = torch.tensor(x, dtype=torch.float32)
                self.y = torch.tensor(y, dtype=torch.float32)
                self.z = torch.tensor(a * (x + y) + 2 * b, dtype=torch.float32)

            def __getitem__(self, index):
                return (self.x[index, None], self.y[index, None],
                        self.z[index, None])

            def __len__(self):
                return len(self.x)

        train_dataset = LinearDataset(3, 4)
        train_loader = torch.utils.data.DataLoader(
            train_dataset,
            batch_size=config.get("batch_size", 32),
        )
        return train_loader, None

    Operator = TrainingOperator.from_creators(
        model_creator,
        optimizer_creator,
        data_creator,
        loss_creator=lambda config: nn.MSELoss())

    trainer = TorchTrainer(
        training_operator_cls=Operator, num_workers=1, use_local=use_local)

    metrics = trainer.train(num_steps=1)
    assert metrics[BATCH_COUNT] == 1

    trainer.shutdown()


@pytest.mark.parametrize("use_local", [True, False])
def test_torch_dataset(ray_start_4_cpus, use_local):
    num_points = 32 * 100 * 2
    data = [i * (1 / num_points) for i in range(num_points)]
    para_it = parallel_it.from_items(data, 2, False).for_each(lambda x: [x, x])
    ds = ml_data.from_parallel_iter(para_it, batch_size=32)

    torch_ds = ds.to_torch(feature_columns=[0], label_column=1)
    operator = make_train_operator(torch_ds)
    trainer = TorchTrainer(
        training_operator_cls=operator,
        num_workers=2,
        use_local=use_local,
        add_dist_sampler=False,
        config={"batch_size": 32})
    for i in range(10):
        trainer.train(num_steps=100)

    model = trainer.get_model()
    prediction = float(model(torch.tensor([[0.5]]).float())[0][0])
    assert 0.4 <= prediction <= 0.6
    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_iterable_model(ray_start_2_cpus, num_workers,
                        use_local):  # noqa: F811
    class IterableOptimizer(torch.optim.SGD):
        def __iter__(self):
            return self.param_groups

    class Operator(TrainingOperator):
        def setup(self, config):
            model = nn.Sequential(nn.Linear(1, config.get("hidden_size", 1)))
            optimizer = IterableOptimizer(
                model.parameters(), lr=config.get("lr", 1e-2))
            criterion = nn.MSELoss()

            self.model, self.optimizer, self.criterion = self.register(
                models=model, optimizers=optimizer, criterion=criterion)
            train_ld, val_ld = data_creator(config)
            self.register_data(train_loader=train_ld, validation_loader=val_ld)

    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        use_local=use_local,
        use_gpu=False)
    for i in range(3):
        train_loss1 = trainer.train()["train_loss"]
    validation_loss1 = trainer.validate()["val_loss"]

    for i in range(3):
        train_loss2 = trainer.train()["train_loss"]
    validation_loss2 = trainer.validate()["val_loss"]

    assert train_loss2 <= train_loss1, (train_loss2, train_loss1)
    assert validation_loss2 <= validation_loss1, (validation_loss2,
                                                  validation_loss1)
    trainer.shutdown()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
