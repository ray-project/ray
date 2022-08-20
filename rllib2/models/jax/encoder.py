import equinox as eqx
import jax
from equinox import nn

from rllib2.models import specs, types

from ..configs import ModelConfig
from ..torch.encoder import Encoder
from .model_base import JaxModel, JaxRecurrentModel


class VectorEncoder(JaxModel, Encoder):
    net: eqx.Module

    def __init__(self, config: ModelConfig, key: jax.random.PRNGKeyArray) -> None:
        super().__init__(config)
        self.net = nn.Composed.MLP(..., key=key)

    def input_spec(self) -> types.SpecDict:
        return specs.SpecDict({"obs": specs.Spec(shape="b h", h=self.config.obs_dim)})

    def _forward(self, inputs: types.TensorDict) -> ForwardOutputType:
        out = self.net(inputs["obs"])
        return types.TensorDict({"encoder_out": out})


class RNNEncoder(JaxRecurrentModel, Encoder):
    net: eqx.Module

    def __init__(self, config: ModelConfig, key: jax.random.PRNGKeyArray) -> None:
        super().__init__(config)
        if config.rnn_cell == "lstm":
            self.net = nn.LSTMCell(..., key=key)
        elif config.rnn_cell == "gru":
            self.net = nn.GRUCell(..., key=key)
        else:
            raise ValueError(f"Unknown rnn_cell: {config.rnn_cell}")

    def input_spec(self) -> types.SpecDict:
        return specs.SpecDict({"obs": specs.Spec(shape="b t h", h=self.config.obs_dim)})

    def prev_state_spec(self) -> types.SpecDict:
        prev_state_dict = {
            f"state_in_{i}": specs.Spec(shape="b h", h=self.config.hidden_size)
            for i in range(self.config.num_layers)
        }
        return types.SpecDict(prev_state_dict)

    def next_state_spec(self) -> types.SpecDict:
        next_state_dict = {
            f"state_out_{i}": specs.Spec(shape="b h", h=self.config.hidden_size)
            for i in range(self.config.num_layers)
        }
        return types.SpecDict(next_state_dict)

    def _unroll(
        self, inputs: types.TensorDict, prev_state: types.TensorDict
    ) -> UnrollOutputType:
        out, next_state = self.net(inputs["obs"], prev_state)
        return types.TensorDict({"encoder_out": out[:, -1]}), next_state
