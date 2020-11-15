from typing import Union

from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.utils.torch_ops import sequence_mask
from ray.rllib.utils.typing import TensorType, Any

torch, nn = try_import_torch()


class RelativeMultiHeadAttention(nn.Module):
    """A RelativeMultiHeadAttention layer as described in [3].

    Uses segment level recurrence with state reuse.
    """

    def __init__(self,
                 in_dim: int,
                 out_dim: int,
                 num_heads: int,
                 head_dim: int,
                 rel_pos_encoder_inference: nn.Module,
                 rel_pos_encoder_training: nn.Module,
                 input_layernorm: bool = False,
                 output_activation: Union[str, callable] = None,
                 **kwargs):
        """Initializes a RelativeMultiHeadAttention nn.Module object.

        Args:
            in_dim (int):
            out_dim (int):
            num_heads (int): The number of attention heads to use.
                Denoted `H` in [2].
            head_dim (int): The dimension of a single(!) attention head
                Denoted `D` in [2].
            rel_pos_encoder_inference (nn.Module): TF op to be used as
                relative positional encoders of the inference (action
                computation) input sequence(s).
            rel_pos_encoder_training (nn.Module): TF op to be used as
                relative positional encoders of the train batch input
                sequence(s).
            input_layernorm (bool): Whether to prepend a LayerNorm before
                everything else. Should be True for building a GTrXL.
            output_activation (Union[str, callable]): Optional activation
                function or activation function specifier (str).
                Should be "relu" for GTrXL.
            **kwargs:
        """
        super().__init__(**kwargs)

        # No bias or non-linearity.
        self._num_heads = num_heads
        self._head_dim = head_dim

        # 3=Query, key, and value inputs.
        self._qkv_layer = SlimFC(
            in_size=in_dim, out_size=3 * num_heads * head_dim, use_bias=False)

        self._linear_layer = SlimFC(
            in_size=num_heads * head_dim,
            out_size=out_dim,
            use_bias=False,
            activation_fn=output_activation)

        self._uvar = nn.Parameter(torch.zeros(num_heads, head_dim))
        self._vvar = nn.Parameter(torch.zeros(num_heads, head_dim))
        nn.init.xavier_uniform_(self._uvar)
        nn.init.xavier_uniform_(self._vvar)
        self.register_parameter("_uvar", self._uvar)
        self.register_parameter("_vvar", self._vvar)

        self._pos_proj = SlimFC(
            in_size=in_dim, out_size=num_heads * head_dim, use_bias=False)
        self._rel_pos_encoder_inference = rel_pos_encoder_inference
        self._rel_pos_encoder_training = rel_pos_encoder_training

        self._input_layernorm = None

        if input_layernorm:
            self._input_layernorm = torch.nn.LayerNorm(in_dim)

        # Go into eval mode by default.
        self.eval()

    def forward(self, inputs: TensorType,
                memory: TensorType = None) -> TensorType:
        T = list(inputs.size())[1]  # length of segment (time)
        H = self._num_heads  # number of attention heads
        d = self._head_dim  # attention head dimension

        # Add previous memory chunk (as const, w/o gradient) to input.
        # Tau (number of (prev) time slices in each memory chunk).
        Tau = list(memory.shape)[1]
        #memory.requires_grad_(False)
        inputs = torch.cat((memory.detach(), inputs), dim=1)

        # Apply the Layer-Norm.
        if self._input_layernorm is not None:
            inputs = self._input_layernorm(inputs)

        qkv = self._qkv_layer(inputs)

        queries, keys, values = torch.chunk(input=qkv, chunks=3, dim=-1)
        # Cut out Tau memory timesteps from query.
        queries = queries[:, -T:]

        queries = torch.reshape(queries, [-1, T, H, d])
        keys = torch.reshape(keys, [-1, Tau + T, H, d])
        values = torch.reshape(values, [-1, Tau + T, H, d])

        R = self._pos_proj(
            self._rel_pos_encoder_training if self.training
            else self._rel_pos_encoder_inference)
        try:#TODO
            R = torch.reshape(R, [Tau + T, H, d])
        except Exception as e:
            raise e

        # b=batch
        # i and j=time indices (i=max-timesteps (inputs); j=Tau memory space)
        # h=head
        # d=head-dim (over which we will reduce-sum)
        score = torch.einsum("bihd,bjhd->bijh", queries + self._uvar, keys)
        pos_score = torch.einsum("bihd,jhd->bijh", queries + self._vvar, R)
        score = score + self.rel_shift(pos_score)
        score = score / d**0.5

        # causal mask of the same length as the sequence
        mask = sequence_mask(
            torch.arange(Tau + 1, Tau + T + 1), dtype=score.dtype).to(
            score.device)
        mask = mask[None, :, :, None]

        masked_score = score * mask + 1e30 * (mask.float() - 1.)
        wmat = nn.functional.softmax(masked_score, dim=2)

        out = torch.einsum("bijh,bjhd->bihd", wmat, values)
        shape = list(out.shape)[:2] + [H * d]
        out = torch.reshape(out, shape)

        return self._linear_layer(out)

    @staticmethod
    def rel_shift(x: TensorType) -> TensorType:
        # Transposed version of the shift approach described in [3].
        # https://github.com/kimiyoung/transformer-xl/blob/
        # 44781ed21dbaec88b280f74d9ae2877f52b492a5/tf/model.py#L31
        x_size = list(x.shape)

        x = torch.nn.functional.pad(x, (0, 0, 1, 0, 0, 0, 0, 0))
        x = torch.reshape(x, [x_size[0], x_size[2] + 1, x_size[1], x_size[3]])
        x = x[:, 1:, :, :]
        x = torch.reshape(x, x_size)

        return x
