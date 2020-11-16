from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.framework import TensorType

torch, nn = try_import_torch()


class GRUGate(nn.Module):
    """Implements a gated recurrent unit for use in AttentionNet"""

    def __init__(self, dim: int, init_bias: int = 0., **kwargs):
        """
        input_shape (torch.Tensor): dimension of the input
        init_bias (int): Bias added to every input to stabilize training
        """
        super().__init__(**kwargs)
        self._init_bias = init_bias

        # Xavier initialization of torch tensors
        self._w_r = torch.zeros(dim, dim)
        self._w_z = torch.zeros(dim, dim)
        self._w_h = torch.zeros(dim, dim)

        self._u_r = torch.zeros(dim, dim)
        self._u_z = torch.zeros(dim, dim)
        self._u_h = torch.zeros(dim, dim)

        nn.init.xavier_uniform_(self._w_r)
        nn.init.xavier_uniform_(self._w_z)
        nn.init.xavier_uniform_(self._w_h)

        nn.init.xavier_uniform_(self._u_r)
        nn.init.xavier_uniform_(self._u_z)
        nn.init.xavier_uniform_(self._u_h)

        self._bias_z = torch.zeros(dim, ).fill_(self._init_bias)

    def forward(self, inputs: TensorType, **kwargs) -> TensorType:
        # Pass in internal state first.
        h, X = inputs

        r = torch.tensordot(X, self._w_r, dims=1) + \
            torch.tensordot(h, self._u_r, dims=1)
        r = torch.sigmoid(r)

        z = torch.tensordot(X, self._w_z, dims=1) + \
            torch.tensordot(h, self._u_z, dims=1) - self._bias_z
        z = torch.sigmoid(z)

        h_next = torch.tensordot(X, self._w_h, dims=1) + \
            torch.tensordot((h * r), self._u_h, dims=1)
        h_next = torch.tanh(h_next)

        return (1 - z) * h + z * h_next
