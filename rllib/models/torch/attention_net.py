"""
[1] - Attention Is All You Need - Vaswani, Jones, Shazeer, Parmar,
      Uszkoreit, Gomez, Kaiser - Google Brain/Research, U Toronto - 2017.
      https://arxiv.org/pdf/1706.03762.pdf
[2] - Stabilizing Transformers for Reinforcement Learning - E. Parisotto
      et al. - DeepMind - 2019. https://arxiv.org/pdf/1910.06764.pdf
[3] - Transformer-XL: Attentive Language Models Beyond a Fixed-Length Context.
      Z. Dai, Z. Yang, et al. - Carnegie Mellon U - 2019.
      https://www.aclweb.org/anthology/P19-1285.pdf
"""

from ray.rllib.models.torch.misc import normc_initializer as normc_init_torch
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.recurrent_net import RecurrentNetwork
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.models.torch.modules.multi_head_attention import MultiHeadAttention

torch, nn = try_import_torch()


class PositionwiseFeedforward(nn.Module):
    """A 2x linear layer with ReLU activation in between described in [1].

    Each timestep coming from the attention head will be passed through this
    layer separately.
    """

    def __init__(self,
                 input_dim,
                 hidden_dim,
                 output_dim,
                 output_activation=None,
                 **kwargs):
        super().__init__(**kwargs)

        self._hidden_layer = SlimFC(
            in_size=input_dim,
            out_size=hidden_dim,
            use_bias=False,
            activation_fn=nn.ReLU)

        self._output_layer = SlimFC(
            in_size=hidden_dim,
            out_size=output_dim,
            use_bias=False,
            activation_fn=output_activation)

    def forward(self, inputs, **kwargs):
        del kwargs
        output = self._hidden_layer(inputs)
        return self._output_layer(output)


if __name__ == '__main__':
    raise NotImplementedError
    #    model = PositionwiseFeedforward(D_in, H, D_out)
    #TODO a lot. port the whole gtrxl
