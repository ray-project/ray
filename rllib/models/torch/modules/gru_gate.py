from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


@OldAPIStack
class GRUGate(nn.Module):
    """Implements a gated recurrent unit for use in AttentionNet"""

    def __init__(self, dim: int, init_bias: int = 0.0, **kwargs):
        """
        input_shape (torch.Tensor): dimension of the input
        init_bias: Bias added to every input to stabilize training
        """
        super().__init__(**kwargs)
        # Xavier initialization of torch tensors
        self._w_r = nn.Parameter(torch.zeros(dim, dim))
        self._w_z = nn.Parameter(torch.zeros(dim, dim))
        self._w_h = nn.Parameter(torch.zeros(dim, dim))
        nn.init.xavier_uniform_(self._w_r)
        nn.init.xavier_uniform_(self._w_z)
        nn.init.xavier_uniform_(self._w_h)
        self.register_parameter("_w_r", self._w_r)
        self.register_parameter("_w_z", self._w_z)
        self.register_parameter("_w_h", self._w_h)

        self._u_r = nn.Parameter(torch.zeros(dim, dim))
        self._u_z = nn.Parameter(torch.zeros(dim, dim))
        self._u_h = nn.Parameter(torch.zeros(dim, dim))
        nn.init.xavier_uniform_(self._u_r)
        nn.init.xavier_uniform_(self._u_z)
        nn.init.xavier_uniform_(self._u_h)
        self.register_parameter("_u_r", self._u_r)
        self.register_parameter("_u_z", self._u_z)
        self.register_parameter("_u_h", self._u_h)

        self._bias_z = nn.Parameter(
            torch.zeros(
                dim,
            ).fill_(init_bias)
        )
        self.register_parameter("_bias_z", self._bias_z)

    def forward(self, inputs: TensorType, **kwargs) -> TensorType:
        # Pass in internal state first.
        h, X = inputs

        r = torch.tensordot(X, self._w_r, dims=1) + torch.tensordot(
            h, self._u_r, dims=1
        )
        r = torch.sigmoid(r)

        z = (
            torch.tensordot(X, self._w_z, dims=1)
            + torch.tensordot(h, self._u_z, dims=1)
            - self._bias_z
        )
        z = torch.sigmoid(z)

        h_next = torch.tensordot(X, self._w_h, dims=1) + torch.tensordot(
            (h * r), self._u_h, dims=1
        )
        h_next = torch.tanh(h_next)

        return (1 - z) * h + z * h_next
