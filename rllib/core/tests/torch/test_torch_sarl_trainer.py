import numpy as np
import torch

import ray

from ray.rllib import SampleBatch
from ray.rllib.core.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.torch.torch_sarl_trainer import TorchSARLTrainer
from ray.rllib.utils.test_utils import check


# ==================== Testing Helpers ==================== #


def error_message_fn_1(model, name_value_being_checked):
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


def _test_1_torch_sarl_trainer_2_gpu(trainer_class_fn):
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
        ), error_message_fn_1("a", "parameter norm")
        assert results_worker_1["b_norm"] == results_worker_2["b_norm"], (
            error_message_fn_1
        )("b", "parameter norm")
        assert results_worker_1["a_grad_norm"] == results_worker_2["a_grad_norm"], (
            error_message_fn_1
        )("a", "gradient norm")
        assert results_worker_1["b_grad_norm"] == results_worker_2["b_grad_norm"], (
            error_message_fn_1
        )("b", "gradient norm")
    del trainer
    ray.shutdown()


def _test_gradients_params_same_on_all_configurations(trainer_class_fn):
    results = []
    for num_gpus in [0, 1, 2]:
        ray.init()
        x, y = make_dataset()
        batch_size = 10
        trainer = trainer_class_fn({"num_gpus": num_gpus})

        for i in range(3):
            batch = SampleBatch(
                {
                    "x": x[i * batch_size : (i + 1) * batch_size],
                    "y": y[i * batch_size : (i + 1) * batch_size],
                }
            )
            result = trainer.train(batch)
        results.append(result)
        ray.shutdown()
    # flatten results
    # IMPORTANT:
    # results[0] is from cpu, results[1] is from 1 gpu, results[2] is from 2
    # gpus first gpu worker, results[3] is from 2 gpus second gpu worker
    results = [r["training_results"] for result in results for r in result]
    a_norms = [r["a_norm"] for r in results]
    b_norms = [r["b_norm"] for r in results]
    a_grad_norms = [r["a_grad_norm"] for r in results]
    b_grad_norms = [r["b_grad_norm"] for r in results]
    for a_norm in a_norms:
        check(a_norms[0], a_norm)
    for b_norm in b_norms:
        check(b_norms[0], b_norm)
    for a_grad_norm in a_grad_norms:
        check(a_grad_norms[0], a_grad_norm)
    for b_grad_norm in b_grad_norms:
        check(b_grad_norms[0], b_grad_norm)

    # in TorchIndependentModulesTrainer the a and b networks are both the same
    # so check that a and b params and grads are the same as well

    if trainer_class_fn == TorchIndependentModulesTrainer:
        assert all(a_norms[0] == b_norm for b_norm in b_norms)
        assert all(b_norms[0] == a_norm for a_norm in a_norms)


# ============= TestModule that has multiple independent models ============= #


class DummyRLModule(TorchRLModule):
    def __init__(self, config):
        """This RL module has 2 networks a and b, and its output is a(x), b(x)"""
        super().__init__(config)
        self.config = config
        self.a = torch.nn.Linear(1, 1)
        self.b = torch.nn.Linear(1, 1)

    def forward_train(self, batch, device=None):
        x = torch.reshape(torch.Tensor(batch["x"]), (-1, 1)).to(device)
        return self.a(x), self.b(x)


class TorchIndependentModulesTrainer(TorchSARLTrainer):
    def __init__(self, config):
        """Train networks a and b of DummyRLModule separately."""
        super().__init__(config)

    @staticmethod
    def compute_loss(batch, fwd_out, device, **kwargs):
        out_a, out_b = fwd_out
        y = torch.reshape(torch.Tensor(batch["y"]), (-1, 1)).to(device)
        loss_a = torch.nn.functional.mse_loss(out_a, y)
        loss_b = torch.nn.functional.mse_loss(out_b, y)
        return {"total_loss": loss_a + loss_b, "loss_a": loss_a, "loss_b": loss_b}

    @staticmethod
    def compile_results(
        batch,
        fwd_out,
        loss_out,
        compute_grads_and_apply_if_needed_info_dict,
        rl_module,
        **kwargs,
    ):
        a_norm = model_norm(rl_module.a)
        b_norm = model_norm(rl_module.b)
        a_grad_norm = model_grad_norm(rl_module.a)
        b_grad_norm = model_grad_norm(rl_module.b)

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
        optimizer_a = torch.optim.SGD(
            unwrapped_module.a.parameters(),
            lr=1e-2,
        )
        optimizer_b = torch.optim.SGD(
            unwrapped_module.b.parameters(),
            lr=1e-2,
        )

        return optimizer_a, optimizer_b


# ==================== TestModule that has model composition ==================== #


class DummyCompositionRLModule(TorchRLModule):
    def __init__(self, config):
        """This RL module has 2 networks a and b, and its output is a(b(x))"""
        super().__init__(config)
        self.config = config
        self.a = torch.nn.Linear(1, 1)
        self.b = torch.nn.Linear(1, 1)

    def forward_train(self, batch, device=None):
        x = torch.reshape(torch.Tensor(batch["x"]), (-1, 1)).to(device)
        return self.a(self.b(x))


class TorchDummyCompositionModuleTrainer(TorchIndependentModulesTrainer):
    def __init__(self, config):
        """Train networks a and b that are composed as a(b(x))."""
        super().__init__(config)

    @staticmethod
    def init_rl_module(module_config):
        # fixing the weights for this test
        # torch.use_deterministic_algorithms(True)
        torch.manual_seed(0)

        def init_weights(m):
            if isinstance(m, torch.nn.Linear):
                m.weight.data.fill_(0.01)
                m.bias.data.fill_(0.01)

        module = DummyCompositionRLModule(module_config)
        module.apply(init_weights)
        return module

    @staticmethod
    def compute_loss(batch, fwd_out, device, **kwargs):
        out = fwd_out
        y = torch.reshape(torch.Tensor(batch["y"]), (-1, 1)).to(device)
        loss = torch.nn.functional.mse_loss(out, y)
        return {"total_loss": loss}


if __name__ == "__main__":
    for trainer_class in [
        TorchIndependentModulesTrainer,
        TorchDummyCompositionModuleTrainer,
    ]:
        _test_1_torch_sarl_trainer_2_gpu(trainer_class)
        _test_gradients_params_same_on_all_configurations(trainer_class)
