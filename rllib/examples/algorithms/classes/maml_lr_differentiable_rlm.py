from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class DifferentiableTorchRLModule(TorchRLModule):
    """Differentiable neural network to learn sinusoid curves.

    This `TorchRLModule`:
    - defines a simple neural network to learn sinusoid curves with two
    feed forward layern and ReLU activations,
    - defines a differentiable `forward` call by overriding the `_forward`
    method (which is implicitly used by the module's `forward` method); this
    enables `torch.func.functional_call?` to work.
    """

    def setup(self):
        """Sets up a simple neural network

        The network contains two hidden layers and ReLU activations. Note,
        input and output are single dimensional b/c the sinusoid curve is.
        """
        self.net = nn.Sequential(
            nn.Linear(1, 40), nn.ReLU(), nn.Linear(40, 40), nn.ReLU(), nn.Linear(40, 1)
        )

    def _forward(self, batch, **kwargs):
        """Defines method to be called for general forward path.

        Note, it is important that the `RLModule.forward` method contains the
        logic to be used for training forward pass b/c otherwise the functional
        call via `torch.func.functional_call` will not work. See for reference
        https://pytorch.org/docs/stable/generated/torch.func.functional_call.html.
        """
        outs = {}
        outs["y_pred"] = self.net(batch[Columns.OBS])
        return outs
