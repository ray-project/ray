import torch

from ray.rllib.core.rl_module import RLModule


class TorchRLModule(torch.nn.Module, RLModule):
    def __init__(self, config):
        super().__init__()
        self.config = config

    def forward(self, batch, device=None, **kwargs):
        """a passthrough for forward train"""
        return self.forward_train(batch, device=device, **kwargs)


if __name__ == "__main__":
    model = TorchRLModule({})
    print(model)
