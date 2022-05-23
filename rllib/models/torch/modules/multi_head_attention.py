"""
[1] - Attention Is All You Need - Vaswani, Jones, Shazeer, Parmar,
      Uszkoreit, Gomez, Kaiser - Google Brain/Research, U Toronto - 2017.
      https://arxiv.org/pdf/1706.03762.pdf
"""
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.utils.torch_utils import sequence_mask
from ray.rllib.utils.framework import TensorType

torch, nn = try_import_torch()


class MultiHeadAttention(nn.Module):
    """A multi-head attention layer described in [1]."""

    def __init__(
        self, in_dim: int, out_dim: int, num_heads: int, head_dim: int, **kwargs
    ):
        """
        in_dim (int): Dimension of input
        out_dim (int): Dimension of output
        num_heads (int): Number of attention heads
        head_dim (int): Output dimension of each attention head
        """
        super().__init__(**kwargs)

        # No bias or non-linearity.
        self._num_heads = num_heads
        self._head_dim = head_dim
        self._qkv_layer = SlimFC(
            in_size=in_dim, out_size=3 * num_heads * head_dim, use_bias=False
        )

        self._linear_layer = SlimFC(
            in_size=num_heads * head_dim, out_size=out_dim, use_bias=False
        )

    def forward(self, inputs: TensorType) -> TensorType:
        L = list(inputs.size())[1]  # length of segment
        H = self._num_heads  # number of attention heads
        D = self._head_dim  # attention head dimension

        qkv = self._qkv_layer(inputs)

        queries, keys, values = torch.chunk(input=qkv, chunks=3, dim=-1)
        queries = queries[:, -L:]  # only query based on the segment

        queries = torch.reshape(queries, [-1, L, H, D])
        keys = torch.reshape(keys, [-1, L, H, D])
        values = torch.reshape(values, [-1, L, H, D])

        score = torch.einsum("bihd,bjhd->bijh", queries, keys)
        score = score / D ** 0.5

        # causal mask of the same length as the sequence
        mask = sequence_mask(torch.arange(1, L + 1), dtype=score.dtype)
        mask = mask[None, :, :, None]
        mask = mask.float()

        masked_score = score * mask + 1e30 * (mask - 1.0)
        wmat = nn.functional.softmax(masked_score, dim=2)

        out = torch.einsum("bijh,bjhd->bihd", wmat, values)
        shape = list(out.size())[:2] + [H * D]
        #        temp = torch.cat(temp2, [H * D], dim=0)
        out = torch.reshape(out, shape)
        return self._linear_layer(out)
