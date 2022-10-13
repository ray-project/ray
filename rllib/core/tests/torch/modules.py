from ray.rllib.core.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.torch.torch_sarl_trainer import TorchSARLTrainer
from ray.rllib.utils.numpy import convert_to_numpy

import torch


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


# ============= TestModule that has multiple independent models ============= #


class DummyRLModule(TorchRLModule):
    def __init__(self, config):
        """This RL module has 2 networks a and b, and its output is a(x), b(x)"""
        super().__init__(config)
        self.config = config
        self.a = torch.nn.Linear(1, 1)
        self.b = torch.nn.Linear(1, 1)

    def forward_train(self, batch, **kwargs):
        x = torch.reshape(torch.Tensor(batch["x"]), (-1, 1))
        return self.a(x), self.b(x)


class TorchIndependentModulesTrainer(TorchSARLTrainer):
    def __init__(self, config):
        """Train networks a and b independently."""
        super().__init__(config)

    def make_module(self, module_config):
        # fixing the weights for this test
        torch.manual_seed(0)

        def init_weights(m):
            if isinstance(m, torch.nn.Linear):
                m.weight.data.fill_(0.01)
                m.bias.data.fill_(0.01)

        module = DummyRLModule(module_config)
        module.apply(init_weights)
        return module

    def make_optimizer(self, optimizer_config):
        optimizer_a = torch.optim.SGD(self.unwrapped_module.a.parameters(), lr=1e-3)
        optimizer_b = torch.optim.SGD(self.unwrapped_module.b.parameters(), lr=1e-3)
        return [optimizer_a, optimizer_b]

    def compute_loss(self, batch, fwd_out, **kwargs):
        out_a, out_b = fwd_out
        y = torch.reshape(torch.Tensor(batch["y"]), (-1, 1))
        loss_a = torch.nn.functional.mse_loss(out_a, y)
        loss_b = torch.nn.functional.mse_loss(out_b, y)
        return {
            "total_loss": loss_a + loss_b,
            "loss_a": loss_a,
            "loss_b": loss_b,
        }

    def compute_grads_and_apply_if_needed(self, batch, fwd_out, loss_out, **kwargs):
        loss = loss_out["total_loss"]
        for optimizer in self.optimizer:
            optimizer.zero_grad()
        loss.backward()
        for optimizer in self.optimizer:
            optimizer.step()
        return {}

    def compile_results(self, batch, fwd_out, loss_out, update_out, **kwargs):
        a_norm = model_norm(self.unwrapped_module.a)
        b_norm = model_norm(self.unwrapped_module.b)
        a_grad_norm = model_grad_norm(self.unwrapped_module.a)
        b_grad_norm = model_grad_norm(self.unwrapped_module.b)

        return {
            "compiled_results": {
                "a_norm": convert_to_numpy(a_norm),
                "b_norm": convert_to_numpy(b_norm),
                "a_grad_norm": convert_to_numpy(a_grad_norm),
                "b_grad_norm": convert_to_numpy(b_grad_norm),
            },
            "loss_out": convert_to_numpy(loss_out),
            "update_out": convert_to_numpy(update_out),
        }

    def module_str(self):
        return str(self.module)


# ==================== TestModule that has model composition ==================== #


class DummyCompositionRLModule(TorchRLModule):
    def __init__(self, config):
        """This RL module has 2 networks a and b, and its output is a(b(x))"""
        super().__init__(config)
        self.config = config
        self.a = torch.nn.Linear(1, 1)
        self.b = torch.nn.Linear(1, 1)

    def forward_train(self, batch, **kwargs):
        x = torch.reshape(torch.Tensor(batch["x"]), (-1, 1))
        return self.a(self.b(x))


class TorchDummyCompositionModuleTrainer(TorchIndependentModulesTrainer):
    def __init__(self, config):
        """Train networks a and b that are composed as a(b(x))."""
        super().__init__(config)

    def make_module(self, module_config):
        # fixing the weights for this test
        torch.manual_seed(0)

        def init_weights(m):
            if isinstance(m, torch.nn.Linear):
                m.weight.data.fill_(0.01)
                m.bias.data.fill_(0.01)

        module = DummyCompositionRLModule(module_config)
        module.apply(init_weights)
        return module

    def compute_loss(self, batch, fwd_out, **kwargs):
        out = fwd_out
        y = torch.reshape(torch.Tensor(batch["y"]), (-1, 1))
        loss = torch.nn.functional.mse_loss(out, y)
        return {"total_loss": loss}


# ================== TestModule that has a shared encoder =================== #


class DummySharedEncoderModule(TorchRLModule):
    def __init__(self, config):
        """This RL module has 2 networks a and b, and its output is a(b(x))"""
        super().__init__(config)
        self.config = config
        self.a = torch.nn.Linear(1, 1)
        self.b = torch.nn.Linear(1, 1)
        self.encoder = torch.nn.Linear(1, 1)

    def forward_train(self, batch, **kwargs):
        x = torch.reshape(torch.Tensor(batch["x"]), (-1, 1))
        inputs = self.encoder(x)
        return self.a(inputs), self.b(inputs)


class TorchSharedEncoderTrainer(TorchIndependentModulesTrainer):
    def __init__(self, config):
        """Train networks a and b that are composed as a(b(x))."""
        super().__init__(config)

    def make_module(self, module_config):
        # fixing the weights for this test
        torch.manual_seed(0)

        def init_weights(m):
            if isinstance(m, torch.nn.Linear):
                m.weight.data.fill_(0.01)
                m.bias.data.fill_(0.01)

        module = DummySharedEncoderModule(module_config)
        module.apply(init_weights)
        return module

    def make_optimizer(self, optimizer_config):
        optimizer_a = torch.optim.SGD(self.unwrapped_module.a.parameters(), lr=1e-3)
        optimizer_b = torch.optim.SGD(self.unwrapped_module.b.parameters(), lr=1e-3)
        optimizer_encoder = torch.optim.SGD(
            self.unwrapped_module.encoder.parameters(), lr=1e-3
        )
        return [optimizer_a, optimizer_b, optimizer_encoder]

    def compile_results(self, batch, fwd_out, loss_out, update_out, **kwargs):
        a_norm = model_norm(self.unwrapped_module.a)
        b_norm = model_norm(self.unwrapped_module.b)
        a_grad_norm = model_grad_norm(self.unwrapped_module.a)
        b_grad_norm = model_grad_norm(self.unwrapped_module.b)
        encoder_norm = model_norm(self.unwrapped_module.encoder)
        encoder_grad_norm = model_grad_norm(self.unwrapped_module.encoder)

        return {
            "compiled_results": {
                "a_norm": convert_to_numpy(a_norm),
                "b_norm": convert_to_numpy(b_norm),
                "encoder_norm": convert_to_numpy(encoder_norm),
                "encoder_grad_norm": convert_to_numpy(encoder_grad_norm),
                "a_grad_norm": convert_to_numpy(a_grad_norm),
                "b_grad_norm": convert_to_numpy(b_grad_norm),
            },
            "loss_out": convert_to_numpy(loss_out),
            "update_out": convert_to_numpy(update_out),
        }


# ======== TestModule that has a shared encoder with auxillary loss ========= #


class DummySharedEncoderAuxLossModule(TorchRLModule):
    def __init__(self, config):
        """This RL module has 2 networks a and b, and its output is a(b(x))"""
        super().__init__(config)
        self.config = config
        self.a = torch.nn.Linear(1, 1)
        self.b = torch.nn.Linear(1, 1)
        self.encoder = torch.nn.Linear(1, 1)

    def forward_train(self, batch, **kwargs):
        x = torch.reshape(torch.Tensor(batch["x"]), (-1, 1))
        with torch.no_grad():
            inputs = self.encoder(x)
        return self.a(inputs), self.b(inputs), self.encoder(x)


class TorchSharedEncoderAuxLossTrainer(TorchSharedEncoderTrainer):
    def __init__(self, config):
        """Train networks a and b that are composed as a(b(x))."""
        super().__init__(config)

    def make_module(self, module_config):
        # fixing the weights for this test
        torch.manual_seed(0)

        def init_weights(m):
            if isinstance(m, torch.nn.Linear):
                m.weight.data.fill_(0.01)
                m.bias.data.fill_(0.01)

        module = DummySharedEncoderAuxLossModule(module_config)
        module.apply(init_weights)
        return module

    def compute_loss(self, batch, fwd_out, **kwargs):
        out_a, out_b, out_encoder = fwd_out
        x = torch.reshape(torch.Tensor(batch["x"]), (-1, 1))
        y = torch.reshape(torch.Tensor(batch["y"]), (-1, 1))
        loss_a = torch.nn.functional.mse_loss(out_a, y)
        loss_b = torch.nn.functional.mse_loss(out_b, y)

        # the encoder is supposed to learn to act as the identity function
        loss_encoder = torch.nn.functional.mse_loss(out_encoder, x)
        return {
            "total_loss": loss_a + loss_b + loss_encoder,
            "loss_a": loss_a,
            "loss_b": loss_b,
            "loss_encoder": loss_encoder,
        }


# ==================== The actual tests here ==================== #
