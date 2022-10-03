import numpy as np
import torch

import ray
from ray import train

from ray.rllib import SampleBatch
from ray.rllib.core.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.torch.torch_sarl_trainer import TorchSARLTrainer


# ==================== Testing Helpers ==================== #


def error_message_fn(model, name_value_being_checked):
    msg = (
        f"model {model}, inside of the DummyCompositionRLModule being "
        "optimized by TorchDummyCompositionModuleTrainer should have the "
        f"same {name_value_being_checked} computed on each of their workers "
        "after each update but they DON'T. Something is probably wrong with "
        "the TorchSARLTrainer or torch DDP."
    )
    return msg


def model_grad_norm(model):
    total_norm = 0
    for p in model.parameters():
        param_norm = p.grad.detach().data.norm(2)
        total_norm += param_norm.item() ** 2
    total_norm = total_norm ** 0.5
    return total_norm


def model_norm(model):
    total_norm = 0
    for p in model.parameters():
        param_norm = p.detach().data.norm(2)
        total_norm += param_norm.item() ** 2
    total_norm = total_norm ** 0.5
    return total_norm


def make_dataset():
    size = 1000
    x = np.arange(0, 10, 10 / size, dtype=np.float32)
    a, b = 2, 5
    y = a * x + b
    return x, y


def test_2_torch_sarl_trainer(trainer_class_fn):
    """Testing to see that 2 trainers can be created in
    the same session
    """
    ray.init()

    batch_size = 10
    x, y = make_dataset()
    trainer = trainer_class_fn({"num_gpus": 1, "module_config": {}})
    trainer2 = trainer_class_fn({"num_gpus": 1, "module_config": {}})

    for i in range(2):
        batch = SampleBatch(
            {
                "x": x[i * batch_size : (i + 1) * batch_size],
                "y": y[i * batch_size : (i + 1) * batch_size],
            }
        )
        trainer.train(batch)
        trainer2.train(batch)

    del trainer
    del trainer2
    ray.shutdown()


def test_1_torch_sarl_trainer(trainer_class_fn):
    ray.init()

    x, y = make_dataset()
    batch_size = 10

    trainer = trainer_class_fn(
        {
            "num_gpus": 2,
            "module_config": {},
        }
    )

    for i in range(2):
        batch = SampleBatch(
            {
                "x": x[i * batch_size : (i + 1) * batch_size],
                "y": y[i * batch_size : (i + 1) * batch_size],
            }
        )
        results_worker_1, results_worker_2 = trainer.train(batch)
        results_worker_1 = results_worker_1["training_results"]
        results_worker_2 = results_worker_2["training_results"]
        assert (
            results_worker_1["a_norm"] == results_worker_2["a_norm"]
        ), error_message_fn("a", "parameter norm")
        assert results_worker_1["b_norm"] == results_worker_2["b_norm"], (
            error_message_fn
        )("b", "parameter norm")
        assert results_worker_1["a_grad_norm"] == results_worker_2["a_grad_norm"], (
            error_message_fn
        )("a", "gradient norm")
        assert results_worker_1["b_grad_norm"] == results_worker_2["b_grad_norm"], (
            error_message_fn
        )("b", "gradient norm")
    del trainer
    ray.shutdown()


# ============= TestModule that has multiple independent models ============= #


class DummyRLModule(TorchRLModule):
    def __init__(self, config):
        """This RL module has 2 networks a and b, and its output is a(x), b(x)"""
        super().__init__(config)
        self.config = config
        self.a = torch.nn.Linear(1, 1)
        self.b = torch.nn.Linear(1, 1)
        self.not_nn = torch.nn.BatchNorm1d(100)

    def forward_train(self, batch):
        return self.a(batch), self.b(batch)


class TorchIndependentModulesTrainer(TorchSARLTrainer):
    def __init__(self, config):
        """Train networks a and b of DummyRLModule separately."""
        super().__init__(config)

    @staticmethod
    def compute_loss_and_update(module, batch, optimizer):
        # this dummy module is actually going to
        # do supervised learning on the batch
        # and return the loss
        device = train.torch.get_device()
        x = torch.reshape(torch.Tensor(batch["x"]), (-1, 1)).to(device)
        y = torch.reshape(torch.Tensor(batch["y"]), (-1, 1)).to(device)
        out_a, out_b = module(x)
        unwrapped_module = module.module

        loss_a = torch.nn.functional.mse_loss(out_a, y)
        loss_b = torch.nn.functional.mse_loss(out_b, y)
        loss = loss_a + loss_b
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        a_norm = model_norm(unwrapped_module.a)
        b_norm = model_norm(unwrapped_module.b)
        a_grad_norm = model_grad_norm(unwrapped_module.a)
        b_grad_norm = model_grad_norm(unwrapped_module.b)

        return {
            "a_norm": a_norm,
            "b_norm": b_norm,
            "a_grad_norm": a_grad_norm,
            "b_grad_norm": b_grad_norm,
        }

    @staticmethod
    def init_rl_module(module_config):
        # fixing the weights for this test
        def init_weights(m):
            if isinstance(m, torch.nn.Linear):
                m.weight.data.fill_(0.01)
                m.bias.data.fill_(0.01)

        module = DummyRLModule(module_config)
        module.apply(init_weights)
        return module

    @staticmethod
    def init_optimizer(module, optimizer_config):
        del optimizer_config
        unwrapped_module = module.module
        optimizer = torch.optim.SGD(
            [
                {"params": unwrapped_module.a.parameters()},
                {"params": unwrapped_module.b.parameters(), "lr": 1e-3},
            ],
            lr=1e-2,
        )

        return optimizer


# ==================== TestModule that has model composition ==================== #


class DummyCompositionRLModule(TorchRLModule):
    def __init__(self, config):
        """This RL module has 2 networks a and b, and its output is a(b(x))"""
        super().__init__(config)
        self.config = config
        self.a = torch.nn.Linear(1, 1)
        self.b = torch.nn.Linear(1, 1)

    def forward_train(self, batch):
        return self.a(self.b(batch))


class TorchDummyCompositionModuleTrainer(TorchSARLTrainer):
    def __init__(self, config):
        """Train networks a and b that are composed as a(b(x))."""
        super().__init__(config)

    @staticmethod
    def compute_loss_and_update(module, batch, optimizer):
        # this dummy module is actually going to
        # do supervised learning on the batch
        # and return the loss
        device = train.torch.get_device()
        x = torch.reshape(torch.Tensor(batch["x"]), (-1, 1)).to(device)
        y = torch.reshape(torch.Tensor(batch["y"]), (-1, 1)).to(device)
        out = module(x)
        optimizer.zero_grad()
        loss = torch.abs(out - y).mean()
        loss.backward()
        optimizer.step()
        loss.item()

        unwrapped_module = module.module

        a_norm = model_norm(unwrapped_module.a)
        b_norm = model_norm(unwrapped_module.b)
        a_grad_norm = model_grad_norm(unwrapped_module.a)
        b_grad_norm = model_grad_norm(unwrapped_module.b)

        return {
            "a_norm": a_norm,
            "b_norm": b_norm,
            "a_grad_norm": a_grad_norm,
            "b_grad_norm": b_grad_norm,
        }

    @staticmethod
    def init_rl_module(module_config):
        # fixing the weights for this test
        def init_weights(m):
            if isinstance(m, torch.nn.Linear):
                m.weight.data.fill_(0.01)
                m.bias.data.fill_(0.01)

        module = DummyCompositionRLModule(module_config)
        module.apply(init_weights)
        return module

    @staticmethod
    def init_optimizer(module, optimizer_config):
        unwrapped_module = module.module
        optimizer = torch.optim.SGD(
            [
                {"params": unwrapped_module.a.parameters()},
                {"params": unwrapped_module.b.parameters(), "lr": 1e-3},
            ],
            lr=1e-2,
        )

        del optimizer_config
        return optimizer


if __name__ == "__main__":
    for trainer_class in [TorchIndependentModulesTrainer]:
        test_1_torch_sarl_trainer(trainer_class)
        test_2_torch_sarl_trainer(trainer_class)
