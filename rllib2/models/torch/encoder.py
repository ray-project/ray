import torch.nn as nn

from .model_base import TorchModel, TorchRecurrentModel

from ..configs import ModelConfig
from rllib2.models import types
from rllib2.models import specs

class ModelWithEncoder:

    def __init__(self, config: ModelConfig) -> None:
        # save config
        self.config = config
        # encoder
        self.encoder = self._make_encoder()
    
    def _make_encoder(self) -> Tuple[Encoder, int]:
        if isinstance(self.config.encoder, str):
            # interpret this as a registered model name
            encoder = None
        elif self.config.encoder:
            # if not empty and not a string, assume it is the model
            encoder = self.config.encoder
        else:
            # return a default encoder if none 
            encoder = model_catalog.get_encoder(self.config)
    
        return encoder


class Encoder:

    def output_spec(self) -> types.SpecDict:
        return specs.SpecDict({
            'encoder_out': specs.Spec(shape='b h', h=self.config.hidden_size)
        })
    
class VectorEncoder(TorchModel, Encoder):
    
    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)
        self.net = MLP(...)

    def input_spec(self) -> types.SpecDict:
        return specs.SpecDict({
            'obs': specs.Spec(shape='b h', h=self.config.obs_dim)
        })

    def _forward(self, inputs: types.TensorDict) -> ForwardOutputType:
        out = self.net(inputs['obs'])
        return types.TensorDict({'encoder_out': out})

class VisionEncoder(TorchModel, Encoder):
    
    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)
        self.net = CNN(...)

    def input_spec(self) -> types.SpecDict:
        return specs.SpecDict({
            'obs': specs.Spec(shape='b h w c', c=self.config.in_channels)
        })

    def _forward(self, inputs: types.TensorDict) -> ForwardOutputType:
        out = self.net(inputs['obs'])
        return types.TensorDict({'encoder_out': out})

class NestedEncoder(TorchModel, Encoder):
    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)
        obs_space = self.config.observation_space

        if not isinstance(obs_space, (gym.spaces.Dict, gym.spaces.Tuple)):
            raise ValueError(
                'NestedEncoder only works with gym.spaces.Dict or gym.spaces.Tuple'
            )

        self.net = tree.map_structure(
            lambda space: model_catalog.get_encoder(space),
            obs_space.spaces
        )

    def input_spec(self) -> types.SpecDict:
        return tree.map_structure(lambda space: space.input_spec(), self.net)

    def _forward(self, inputs: types.TensorDict) -> ForwardOutputType:
        out = tree.map_structure(lambda net, input: net(input), self.net, inputs['obs'])
        out = tree.flatten(out, dim=-1)
        return types.TensorDict({'encoder_out': out})


class TransformerEncoder(TorchModel, Encoder):
    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)
        self.net = Transformer(...)

    def input_spec(self) -> types.SpecDict:
        return specs.SpecDict({
            'obs': specs.Spec(
                shape='b t h', 
                t=self.config.seq_len, 
                h=self.config.obs_dim
            )
        })

    def _forward(self, inputs: types.TensorDict) -> ForwardOutputType:
        out = self.net(inputs['obs'])
        return types.TensorDict({'encoder_out': out[:, -1]})


class RNNEncoder(TorchRecurrentModel, Encoder):
    
    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)
        if config.rnn_cell == 'lstm':
            self.net = LSTM(...)
        elif config.rnn_cell == 'gru':
            self.net = GRU(...)
        else:
            raise ValueError(f'Unknown rnn_cell: {config.rnn_cell}')

    def input_spec(self) -> types.SpecDict:
        return specs.SpecDict({
            'obs': specs.Spec(shape='b t h', h=self.config.obs_dim)
        })

    def prev_state_spec(self) -> types.SpecDict:
        prev_state_dict = {
            f'state_in_{i}': specs.Spec(shape='b h', h=self.config.hidden_size) 
            for i in range(self.config.num_layers)
        }
        return types.SpecDict(prev_state_dict)

    def next_state_spec(self) -> types.SpecDict:
        next_state_dict = {
            f'state_out_{i}': specs.Spec(shape='b h', h=self.config.hidden_size) 
            for i in range(self.config.num_layers)
        }
        return types.SpecDict(next_state_dict)

    def _unroll(self, inputs: types.TensorDict, prev_state: types.TensorDict) -> UnrollOutputType:
        out, next_state = self.net(inputs['obs'], prev_state)
        return types.TensorDict({'encoder_out': out[:, -1]}), next_state


class GraphEncoder:
    pass

