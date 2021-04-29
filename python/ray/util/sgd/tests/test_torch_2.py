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
from ray.util.sgd.torch.examples.train_example import (
    model_creator, optimizer_creator, data_creator)
from ray.util.sgd.torch.training_operator import (get_test_operator,
                                                  TrainingOperator)
from ray.util.sgd.utils import BATCH_COUNT


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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
