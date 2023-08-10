from typing import Optional

import tree

from ray.rllib.core.models.base import (
    Encoder,
    ActorCriticEncoder,
    StatefulActorCriticEncoder,
    STATE_IN,
    STATE_OUT,
    ENCODER_OUT,
)
from ray.rllib.core.models.base import Model, tokenize
from ray.rllib.core.models.configs import (
    ActorCriticEncoderConfig,
    CNNEncoderConfig,
    MLPEncoderConfig,
    RecurrentEncoderConfig,
)
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.torch.primitives import TorchMLP, TorchCNN
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchActorCriticEncoder(TorchModel, ActorCriticEncoder):
    """An actor-critic encoder for torch."""

    framework = "torch"

    def __init__(self, config: ActorCriticEncoderConfig) -> None:
        TorchModel.__init__(self, config)
        ActorCriticEncoder.__init__(self, config)


class TorchStatefulActorCriticEncoder(TorchModel, StatefulActorCriticEncoder):
    """A stateful actor-critic encoder for torch."""

    framework = "torch"

    def __init__(self, config: ActorCriticEncoderConfig) -> None:
        TorchModel.__init__(self, config)
        StatefulActorCriticEncoder.__init__(self, config)


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
            hidden_layer_use_bias=config.hidden_layer_use_bias,
            output_dim=config.output_layer_dim,
            output_activation=config.output_layer_activation,
            output_use_bias=config.output_layer_use_bias,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                SampleBatch.OBS: TensorSpec(
                    "b, d", d=self.config.input_dims[0], framework="torch"
                ),
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TensorSpec(
                    "b, d", d=self.config.output_dims[0], framework="torch"
                ),
            }
        )

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        return {ENCODER_OUT: self.net(inputs[SampleBatch.OBS])}


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
            cnn_use_bias=config.cnn_use_bias,
        )
        layers.append(cnn)

        # Add a flatten operation to move from 2/3D into 1D space.
        if config.flatten_at_end:
            layers.append(nn.Flatten())

        # Create the network from gathered layers.
        self.net = nn.Sequential(*layers)

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                SampleBatch.OBS: TensorSpec(
                    "b, w, h, c",
                    w=self.config.input_dims[0],
                    h=self.config.input_dims[1],
                    c=self.config.input_dims[2],
                    framework="torch",
                ),
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: (
                    TensorSpec("b, d", d=self.config.output_dims[0], framework="torch")
                    if self.config.flatten_at_end
                    else TensorSpec(
                        "b, w, h, c",
                        w=self.config.output_dims[0],
                        h=self.config.output_dims[1],
                        d=self.config.output_dims[2],
                        framework="torch",
                    )
                )
            }
        )

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        return {ENCODER_OUT: self.net(inputs[SampleBatch.OBS])}


class TorchGRUEncoder(TorchModel, Encoder):
    """A recurrent GRU encoder.

    This encoder has...
    - Zero or one tokenizers.
    - One or more GRU layers.
    - One linear output layer.
    """

    def __init__(self, config: RecurrentEncoderConfig) -> None:
        TorchModel.__init__(self, config)

        # Maybe create a tokenizer
        if config.tokenizer_config is not None:
            self.tokenizer = config.tokenizer_config.build(framework="torch")
            gru_input_dims = config.tokenizer_config.output_dims
        else:
            self.tokenizer = None
            gru_input_dims = config.input_dims

        # We only support 1D spaces right now.
        assert len(gru_input_dims) == 1
        gru_input_dim = gru_input_dims[0]

        # Create the torch LSTM layer.
        self.gru = nn.GRU(
            gru_input_dim,
            config.hidden_dim,
            config.num_layers,
            batch_first=config.batch_major,
            bias=config.use_bias,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                # b, t for batch major; t, b for time major.
                SampleBatch.OBS: TensorSpec(
                    "b, t, d",
                    d=self.config.input_dims[0],
                    framework="torch",
                ),
                STATE_IN: {
                    "h": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="torch",
                    ),
                },
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TensorSpec(
                    "b, t, d", d=self.config.output_dims[0], framework="torch"
                ),
                STATE_OUT: {
                    "h": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="torch",
                    ),
                },
            }
        )

    @override(Model)
    def get_initial_state(self):
        return {
            "h": torch.zeros(self.config.num_layers, self.config.hidden_dim),
        }

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        outputs = {}

        if self.tokenizer is not None:
            # Push observations through the tokenizer encoder if we built one.
            out = tokenize(self.tokenizer, inputs, framework="torch")
        else:
            # Otherwise, just use the raw observations.
            out = inputs[SampleBatch.OBS].float()

        # States are batch-first when coming in. Make them layers-first.
        states_in = tree.map_structure(lambda s: s.transpose(0, 1), inputs[STATE_IN])

        out, states_out = self.gru(out, states_in["h"])
        states_out = {"h": states_out}

        # Insert them into the output dict.
        outputs[ENCODER_OUT] = out
        outputs[STATE_OUT] = tree.map_structure(lambda s: s.transpose(0, 1), states_out)
        return outputs


class TorchLSTMEncoder(TorchModel, Encoder):
    """A recurrent LSTM encoder.

    This encoder has...
    - Zero or one tokenizers.
    - One or more LSTM layers.
    - One linear output layer.
    """

    def __init__(self, config: RecurrentEncoderConfig) -> None:
        TorchModel.__init__(self, config)

        # Maybe create a tokenizer
        if config.tokenizer_config is not None:
            self.tokenizer = config.tokenizer_config.build(framework="torch")
            lstm_input_dims = config.tokenizer_config.output_dims
        else:
            self.tokenizer = None
            lstm_input_dims = config.input_dims

        # We only support 1D spaces right now.
        assert len(lstm_input_dims) == 1
        lstm_input_dim = lstm_input_dims[0]

        # Create the torch LSTM layer.
        self.lstm = nn.LSTM(
            lstm_input_dim,
            config.hidden_dim,
            config.num_layers,
            batch_first=config.batch_major,
            bias=config.use_bias,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                # b, t for batch major; t, b for time major.
                SampleBatch.OBS: TensorSpec(
                    "b, t, d", d=self.config.input_dims[0], framework="torch"
                ),
                STATE_IN: {
                    "h": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="torch",
                    ),
                    "c": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="torch",
                    ),
                },
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TensorSpec(
                    "b, t, d", d=self.config.output_dims[0], framework="torch"
                ),
                STATE_OUT: {
                    "h": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="torch",
                    ),
                    "c": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="torch",
                    ),
                },
            }
        )

    @override(Model)
    def get_initial_state(self):
        return {
            "h": torch.zeros(self.config.num_layers, self.config.hidden_dim),
            "c": torch.zeros(self.config.num_layers, self.config.hidden_dim),
        }

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        outputs = {}

        if self.tokenizer is not None:
            # Push observations through the tokenizer encoder if we built one.
            out = tokenize(self.tokenizer, inputs, framework="torch")
        else:
            # Otherwise, just use the raw observations.
            out = inputs[SampleBatch.OBS].float()

        # States are batch-first when coming in. Make them layers-first.
        states_in = tree.map_structure(lambda s: s.transpose(0, 1), inputs[STATE_IN])

        out, states_out = self.lstm(out, (states_in["h"], states_in["c"]))
        states_out = {"h": states_out[0], "c": states_out[1]}

        # Insert them into the output dict.
        outputs[ENCODER_OUT] = out
        outputs[STATE_OUT] = tree.map_structure(lambda s: s.transpose(0, 1), states_out)
        return outputs
