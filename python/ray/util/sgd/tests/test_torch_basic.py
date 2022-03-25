import pytest
import ray
import torch.distributed as dist
import torch.nn as nn

from ray import tune
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.torch.examples.train_example import (
    model_creator,
    optimizer_creator,
    data_creator,
)
from ray.util.sgd.torch.training_operator import TrainingOperator
from ray.util.sgd.utils import BATCH_COUNT

Operator = TrainingOperator.from_creators(
    model_creator, optimizer_creator, data_creator, loss_creator=nn.MSELoss
)


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


@pytest.mark.parametrize("use_local", [True, False])
def test_single_step(ray_start_2_cpus, use_local):  # noqa: F811
    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=1,
        use_local=use_local,
        use_gpu=False,
    )
    metrics = trainer.train(num_steps=1)
    assert metrics[BATCH_COUNT] == 1

    val_metrics = trainer.validate(num_steps=1)
    assert val_metrics[BATCH_COUNT] == 1
    trainer.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
@pytest.mark.parametrize("use_fp16", [False, True])
def test_train(ray_start_2_cpus, num_workers, use_local, use_fp16):  # noqa: F811
    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        use_local=use_local,
        use_gpu=False,
        # use_fp16 has no effect here but allows
        # us to check syntax
        use_fp16=use_fp16,
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
