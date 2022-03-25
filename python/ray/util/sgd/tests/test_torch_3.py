import numpy as np
import pytest
import torch
import torch.distributed as dist
import torch.nn as nn

import ray
import ray.util.data as ml_data
import ray.util.iter as parallel_it
from ray import tune
from ray.tune.utils import merge_dicts
from ray.util.data.examples.mlp_identity_torch import make_train_operator
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.torch.examples.train_example import (
    model_creator,
    optimizer_creator,
    data_creator,
)
from ray.util.sgd.torch.training_operator import TrainingOperator
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
    model_creator, optimizer_creator, data_creator, loss_creator=nn.MSELoss
)


@pytest.mark.parametrize("num_workers", [2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_tune_custom_train(ray_start_4_cpus, num_workers, use_local):  # noqa: F811
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
            "config": {"batch_size": 512, "lr": 0.001},
        }
    )

    analysis = tune.run(
        TorchTrainable, num_samples=2, stop={"training_iteration": 2}, verbose=1
    )

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
                return (self.x[index, None], self.y[index, None], self.z[index, None])

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
        loss_creator=lambda config: nn.MSELoss(),
    )

    trainer = TorchTrainer(
        training_operator_cls=Operator, num_workers=1, use_local=use_local
    )

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
        config={"batch_size": 32},
    )
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
