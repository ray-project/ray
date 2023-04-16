from typing import Optional

import tree

from ray.rllib.core.models.base import (
    Encoder,
    ActorCriticEncoder,
    STATE_IN,
    STATE_OUT,
    ENCODER_OUT,
)
from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import (
    CNNEncoderConfig,
    MLPEncoderConfig,
    ModelConfig,
)
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.torch.primitives import TorchMLP, TorchCNN
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict

torch, nn = try_import_torch()


class TorchMLPEncoder(TorchModel, Encoder):
    def __init__(self, config: MLPEncoderConfig) -> None:
        TorchModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural network.
        self.net = TorchMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            hidden_layer_activation=config.hidden_layer_activation,
            hidden_layer_use_layernorm=config.hidden_layer_use_layernorm,
            output_dim=config.output_dims[0],
            output_activation=config.output_activation,
            use_bias=config.use_bias,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                SampleBatch.OBS: TorchTensorSpec("b, d", d=self.config.input_dims[0]),
                STATE_IN: None,
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TorchTensorSpec("b, d", d=self.config.output_dims[0]),
                STATE_OUT: None,
            }
        )

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        return NestedDict(
            {
                ENCODER_OUT: self.net(inputs[SampleBatch.OBS]),
                STATE_OUT: inputs[STATE_IN],
            }
        )


class TorchCNNEncoder(TorchModel, Encoder):
    def __init__(self, config: CNNEncoderConfig) -> None:
        TorchModel.__init__(self, config)
        Encoder.__init__(self, config)

        layers = []
        # The bare-bones CNN (no flatten, no succeeding dense).
        cnn = TorchCNN(
            input_dims=config.input_dims,
            cnn_filter_specifiers=config.cnn_filter_specifiers,
            cnn_activation=config.cnn_activation,
            cnn_use_layernorm=config.cnn_use_layernorm,
            use_bias=config.use_bias,
        )
        layers.append(cnn)

        # Add a flatten operation to move from 2/3D into 1D space.
        layers.append(nn.Flatten())

        # Add a final linear layer to make sure that the outputs have the correct
        # dimensionality (output_dims).
        layers.append(
            nn.Linear(
                int(cnn.output_width) * int(cnn.output_height) * int(cnn.output_depth),
                config.output_dims[0],
            )
        )
        output_activation = get_activation_fn(
            config.output_activation, framework="torch"
        )
        if output_activation is not None:
            layers.append(output_activation())

        # Create the network from gathered layers.
        self.net = nn.Sequential(*layers)

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                SampleBatch.OBS: TorchTensorSpec(
                    "b, w, h, c",
                    w=self.config.input_dims[0],
                    h=self.config.input_dims[1],
                    c=self.config.input_dims[2],
                ),
                STATE_IN: None,
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TorchTensorSpec("b, d", d=self.config.output_dims[0]),
                STATE_OUT: None,
            }
        )

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        return NestedDict(
            {
                ENCODER_OUT: self.net(inputs[SampleBatch.OBS]),
                STATE_OUT: inputs[STATE_IN],
            }
        )


class TorchLSTMEncoder(TorchModel, Encoder):
    """An encoder that uses an LSTM cell and a linear layer."""

    def __init__(self, config: ModelConfig) -> None:
        TorchModel.__init__(self, config)

        # Create the neural networks
        self.lstm = nn.LSTM(
            # We only support 1D spaces right now.
            config.observation_space.shape[0],
            config.hidden_dim,
            config.num_layers,
            batch_first=config.batch_first,
        )
        self.linear = nn.Linear(config.hidden_dim, config.output_dims[0])

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                # `bxt` is just a name for better readability to indicate padded batch.
                SampleBatch.OBS: TorchTensorSpec("bxt, h", h=self.config.input_dims[0]),
                STATE_IN: {
                    "h": TorchTensorSpec(
                        "b, l, h", h=self.config.hidden_dim, l=self.config.num_layers
                    ),
                    "c": TorchTensorSpec(
                        "b, l, h", h=self.config.hidden_dim, l=self.config.num_layers
                    ),
                },
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TorchTensorSpec("bxt, h", h=self.config.output_dims[0]),
                STATE_OUT: {
                    "h": TorchTensorSpec(
                        "b, l, h", h=self.config.hidden_dim, l=self.config.num_layers
                    ),
                    "c": TorchTensorSpec(
                        "b, l, h", h=self.config.hidden_dim, l=self.config.num_layers
                    ),
                },
            }
        )

    @override(Model)
    def get_initial_state(self):
        config = self.config
        return {
            "h": torch.zeros(config.num_layers, config.hidden_dim),
            "c": torch.zeros(config.num_layers, config.hidden_dim),
        }

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        x = inputs[SampleBatch.OBS].float()
        states = inputs[STATE_IN]
        # states are batch-first when coming in
        states = tree.map_structure(lambda x: x.transpose(0, 1), states)

        x = add_time_dimension(
            x,
            seq_lens=inputs[SampleBatch.SEQ_LENS],
            framework="torch",
            time_major=not self.config.batch_first,
        )
        states_o = {}
        x, (states_o["h"], states_o["c"]) = self.lstm(x, (states["h"], states["c"]))

        x = self.linear(x)
        x = x.view(-1, x.shape[-1])

        return {
            ENCODER_OUT: x,
            STATE_OUT: tree.map_structure(lambda x: x.transpose(0, 1), states_o),
        }


class TorchActorCriticEncoder(TorchModel, ActorCriticEncoder):
    """An actor-critic encoder for torch."""

    framework = "torch"

    def __init__(self, config: ModelConfig) -> None:
        TorchModel.__init__(self, config, skip_nn_module_init=True)
        ActorCriticEncoder.__init__(self, config)
