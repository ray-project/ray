import math
from typing import Optional, Sequence, Union
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()

DEVICE_TYPING = Union[torch.device, str, int]


class NoisyLinear(nn.Linear):
    """Noisy Linear Layer.

    Presented in "Noisy Networks for Exploration",
    https://arxiv.org/abs/1706.10295v3, implemented in relation to
    `torchrl`'s `NoisyLinear` layer.

    A Noisy Linear Layer is a linear layer with parametric noise added to
    the weights. This induced stochasticity can be used in RL networks for
    the agent's policy to aid efficient exploration. The parameters of the
    noise are learned with gradient descent along with any other remaining
    network weights. Factorized Gaussian noise is the type of noise usually
    employed.


    Args:
        in_features: Input features dimension.
        out_features: Out features dimension.
        bias: If `True`, a bias term will be added to the matrix
            multiplication: `Ax + b`. Defaults to `True`.
        device: Device of the layer. Defaults to `"cpu"`.
        dtype: `dtype` of the parameters. Defaults to `None` (default `torch`
            `dtype`).
        std_init: Initial value of the Gaussian standard deviation before
            optimization. Defaults to `0.1`.

    """

    def __init__(
        self,
        in_features: int,
        out_features: int,
        bias: bool = True,
        device: Optional[DEVICE_TYPING] = None,
        dtype: Optional[torch.dtype] = None,
        std_init: float = 0.1,
    ):
        nn.Module.__init__(self)
        self.in_features = int(in_features)
        self.out_features = int(out_features)
        self.std_init = std_init

        self.weight_mu = nn.Parameter(
            torch.empty(
                out_features,
                in_features,
                device=device,
                dtype=dtype,
                requires_grad=True,
            )
        )
        self.weight_sigma = nn.Parameter(
            torch.empty(
                out_features,
                in_features,
                device=device,
                dtype=dtype,
                requires_grad=True,
            )
        )
        self.register_buffer(
            "weight_epsilon",
            torch.empty(out_features, in_features, device=device, dtype=dtype),
        )
        if bias:
            self.bias_mu = nn.Parameter(
                torch.empty(
                    out_features,
                    device=device,
                    dtype=dtype,
                    requires_grad=True,
                )
            )
            self.bias_sigma = nn.Parameter(
                torch.empty(
                    out_features,
                    device=device,
                    dtype=dtype,
                    requires_grad=True,
                )
            )
            self.register_buffer(
                "bias_epsilon",
                torch.empty(out_features, device=device, dtype=dtype),
            )
        else:
            self.bias_mu = None
        self.reset_parameters()
        self.reset_noise()
        self.training = True

    @torch.no_grad()
    def reset_parameters(self) -> None:
        # Use initialization for factorized noisy linear layers.
        mu_range = 1 / math.sqrt(self.in_features)
        # Initialize weight distribution parameters.
        self.weight_mu.data.uniform_(-mu_range, mu_range)
        self.weight_sigma.data.fill_(self.std_init / math.sqrt(self.in_features))
        # If bias is used initial these parameters, too.
        if self.bias_mu is not None:
            self.bias_mu.data.zero_()  # (-mu_range, mu_range)
            self.bias_sigma.data.fill_(self.std_init / math.sqrt(self.out_features))

    @torch.no_grad()
    def reset_noise(self) -> None:
        with torch.no_grad():
            # Use factorized noise for better performance.
            epsilon_in = self._scale_noise(self.in_features)
            epsilon_out = self._scale_noise(self.out_features)
            self.weight_epsilon.copy_(epsilon_out.outer(epsilon_in))
            if self.bias_mu is not None:
                self.bias_epsilon.copy_(epsilon_out)

    @torch.no_grad()
    def _scale_noise(self, size: Union[int, torch.Size, Sequence]) -> torch.Tensor:
        if isinstance(size, int):
            size = (size,)
        x = torch.randn(*size, device=self.weight_mu.device)
        return x.sign().mul_(x.abs().sqrt_())

    @property
    def weight(self) -> torch.Tensor:
        if self.training:
            # If in training mode, sample the noise.
            return self.weight_mu + self.weight_sigma * self.weight_epsilon
        else:
            return self.weight_mu

    @property
    def bias(self) -> Optional[torch.Tensor]:
        if self.bias_mu is not None:
            if self.training:
                # If in training mode, sample the noise.
                return self.bias_mu + self.bias_sigma * self.bias_epsilon
            else:
                return self.bias_mu
        else:
            return None
