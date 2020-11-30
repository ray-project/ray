import copy
import os

import pytest
import ray
import torch
from ray.util.sgd.utils import BATCH_COUNT
import torch.distributed as dist
from pytorch_lightning import LightningModule
from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator
from ray.util.sgd.torch.examples.train_example import \
    optimizer_creator, data_creator, scheduler_creator, model_creator
from torch import nn
import numpy as np

torch.manual_seed(0)
np.random.seed(0)


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Ensure that tests don't ALL fail
    if dist.is_initialized():
        dist.destroy_process_group()


class PTL_Module(LightningModule):
    def __init__(self, config):
        super().__init__()

        self.config = config
        if "layer" in config:
            self.layer = copy.deepcopy(config["layer"])
        else:
            self.layer = model_creator(self.config)

        self.rand_int = np.random.randint(10)

    def forward(self, x):
        return self.layer.forward(x)

    def configure_optimizers(self):
        optimizer = optimizer_creator(self, self.config)
        scheduler = scheduler_creator(optimizer, self.config)
        return [optimizer], [scheduler]

    def training_step(self, batch, batch_idx):
        x, y = batch
        output = self(x)
        loss = self.loss(output, y)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        output = self(x)
        loss = self.loss(output, y)
        _, predicted = torch.max(output.data, 1)
        num_correct = (predicted == y).sum().item()
        num_samples = y.size(0)
        return {"val_loss": loss.item(), "val_acc": num_correct / num_samples}

    def setup(self, stage):
        self.train_loader, self.val_loader = data_creator(self.config)
        self.loss = nn.MSELoss()

    def train_dataloader(self):
        return self.train_loader

    def val_dataloader(self):
        return self.val_loader

    def on_save_checkpoint(self, checkpoint):
        checkpoint["int"] = self.rand_int

    def on_load_checkpoint(self, checkpoint):
        self.rand_int = checkpoint["int"]


Operator = TrainingOperator.from_ptl(PTL_Module)


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
    ints1 = trainer1.apply_all_operators(lambda op: op.get_model().rand_int)[0]

    trainer1.shutdown()

    trainer2 = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        use_local=use_local)
    trainer2.load(checkpoint_path)

    model2 = trainer2.get_model()
    ints2 = trainer2.apply_all_operators(lambda op: op.get_model().rand_int)

    model1_state_dict = model1.state_dict()
    model2_state_dict = model2.state_dict()

    assert set(model1_state_dict.keys()) == set(model2_state_dict.keys())

    for k in model1_state_dict:
        assert torch.equal(model1_state_dict[k], model2_state_dict[k])
    for i in ints2:
        assert i == ints1
    trainer2.shutdown()


class CorrectnessOperator(TrainingOperator):
    def setup(self, config):
        model = PTL_Module(config)
        opt = optimizer_creator(model, config)
        scheduler = scheduler_creator(opt, config)
        self.model, self.optimizer, self.criterion, self.scheduler = \
            self.register(
                models=model, optimizers=opt, criterion=nn.MSELoss(),
                schedulers=scheduler)

        train_loader, val_loader = data_creator(config)
        self.register_data(
            train_loader=train_loader, validation_loader=val_loader)


@pytest.mark.parametrize("num_workers", [1, 2] if dist.is_available() else [1])
@pytest.mark.parametrize("use_local", [True, False])
def test_correctness(ray_start_2_cpus, num_workers, use_local):
    layer = nn.Linear(1, 1)
    ptl_op = TrainingOperator.from_ptl(PTL_Module)
    trainer1 = TorchTrainer(
        training_operator_cls=ptl_op,
        config={
            "layer": layer,
            "data_size": 3,
            "batch_size": 1
        },
        num_workers=num_workers,
        use_local=use_local)
    train1_stats = trainer1.train()
    val1_stats = trainer1.validate()
    trainer1.shutdown()

    trainer2 = TorchTrainer(
        training_operator_cls=CorrectnessOperator,
        scheduler_step_freq="manual",
        config={
            "layer": layer,
            "data_size": 3,
            "batch_size": 1
        },
        num_workers=num_workers,
        use_local=use_local)
    train2_stats = trainer2.train()
    val2_stats = trainer2.validate()
    trainer2.shutdown()

    assert train1_stats["train_loss"] == train2_stats["train_loss"]
    assert val1_stats["val_loss"] == val2_stats["val_loss"]
    assert val1_stats["val_acc"] == val2_stats["val_accuracy"]


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(sys.argv[1:] + ["-v", __file__]))
