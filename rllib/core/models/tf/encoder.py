from typing import Optional

import tree  # pip install dm_tree

from ray.rllib.core.models.base import (
    Encoder,
    ActorCriticEncoder,
    STATE_IN,
    STATE_OUT,
    ENCODER_OUT,
)
from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import (
    ActorCriticEncoderConfig,
    CNNEncoderConfig,
    MLPEncoderConfig,
    RecurrentEncoderConfig,
)
from ray.rllib.core.models.tf.base import TfModel
from ray.rllib.core.models.tf.primitives import TfMLP, TfCNN
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

_, tf, _ = try_import_tf()


class TfActorCriticEncoder(TfModel, ActorCriticEncoder):
    """An encoder that can hold two encoders."""

    framework = "tf2"

    def __init__(self, config: ActorCriticEncoderConfig) -> None:
        # We have to call TfModel.__init__ first, because it calls the constructor of
        # tf.keras.Model, which is required to be called before models are created.
        TfModel.__init__(self, config)
        ActorCriticEncoder.__init__(self, config)


class TfCNNEncoder(TfModel, Encoder):
    def __init__(self, config: CNNEncoderConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        layers = []
        # The bare-bones CNN (no flatten, no succeeding dense).
        cnn = TfCNN(
            input_dims=config.input_dims,
            cnn_filter_specifiers=config.cnn_filter_specifiers,
            cnn_activation=config.cnn_activation,
            cnn_use_layernorm=config.cnn_use_layernorm,
            use_bias=config.use_bias,
        )
        layers.append(cnn)

        # Add a flatten operation to move from 2/3D into 1D space.
        layers.append(tf.keras.layers.Flatten())

        # Add a final linear layer to make sure that the outputs have the correct
        # dimensionality (output_dims).
        output_activation = get_activation_fn(config.output_activation, framework="tf2")
        layers.append(
            tf.keras.layers.Dense(config.output_dims[0], activation=output_activation),
        )

        # Create the network from gathered layers.
        self.net = tf.keras.Sequential(layers)

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                SampleBatch.OBS: TensorSpec(
                    "b, w, h, c",
                    w=self.config.input_dims[0],
                    h=self.config.input_dims[1],
                    c=self.config.input_dims[2],
                    framework="tf2",
                ),
                STATE_IN: None,
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TensorSpec(
                    "b, d", d=self.config.output_dims[0], framework="tf2"
                ),
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


class TfMLPEncoder(Encoder, TfModel):
    def __init__(self, config: MLPEncoderConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural network.
        self.net = TfMLP(
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
                SampleBatch.OBS: TensorSpec(
                    "b, d", d=self.config.input_dims[0], framework="tf2"
                ),
                # STATE_IN: None,
                # SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TensorSpec(
                    "b, d", d=self.config.output_dims[0], framework="tf2"
                ),
                STATE_OUT: None,
            }
        )

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        return NestedDict(
            {
                ENCODER_OUT: self.net(inputs[SampleBatch.OBS]),
                STATE_OUT: None,  # inputs[STATE_IN],
            }
        )


class TfGRUEncoder(TfModel, Encoder):
    """An encoder that uses one or more GRU layers and a linear output layer."""

    def __init__(self, config: RecurrentEncoderConfig) -> None:
        TfModel.__init__(self, config)

        # Create the tf GRU layers.
        self.grus = []
        for _ in range(config.num_layers):
            self.grus.append(
                tf.keras.layers.GRU(
                    config.hidden_dim,
                    time_major=not config.batch_major,
                    use_bias=config.use_bias,
                    return_sequences=True,
                    return_state=True,
                )
            )

        # Create the final dense layer.
        self.linear = tf.keras.layers.Dense(
            units=config.output_dims[0],
            use_bias=config.use_bias,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                # b, t for batch major; t, b for time major.
                SampleBatch.OBS: TensorSpec(
                    "b, t, d", d=self.config.input_dims[0], framework="tf2"
                ),
                STATE_IN: {
                    "h": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="tf2",
                    ),
                },
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TensorSpec(
                    "b, t, d", d=self.config.output_dims[0], framework="tf2"
                ),
                STATE_OUT: {
                    "h": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="tf2",
                    ),
                },
            }
        )

    @override(Model)
    def get_initial_state(self):
        return {
            "h": tf.zeros((self.config.num_layers, self.config.hidden_dim)),
        }

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        out = tf.cast(inputs[SampleBatch.OBS], tf.float32)

        # States are batch-first when coming in. Make them layers-first.
        states_in = tree.map_structure(
            lambda s: tf.transpose(s, perm=[1, 0] + list(range(2, len(s.shape)))),
            inputs[STATE_IN],
        )

        states_out = []
        for i, layer in enumerate(self.grus):
            out, h = layer(out, states_in["h"][i])
            states_out.append(h)

        out = self.linear(out)

        return {
            ENCODER_OUT: out,
            # Make state_out batch-first.
            STATE_OUT: {"h": tf.stack(states_out, 1)},
        }


class TfLSTMEncoder(TfModel, Encoder):
    """An encoder that uses an LSTM cell and a linear layer."""

    def __init__(self, config: RecurrentEncoderConfig) -> None:
        TfModel.__init__(self, config)

        # Create the tf LSTM layers.
        self.lstms = []
        for _ in range(config.num_layers):
            self.lstms.append(
                tf.keras.layers.LSTM(
                    config.hidden_dim,
                    time_major=not config.batch_major,
                    use_bias=config.use_bias,
                    return_sequences=True,
                    return_state=True,
                )
            )

        # Create the final dense layer.
        self.linear = tf.keras.layers.Dense(
            units=config.output_dims[0],
            use_bias=config.use_bias,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                # b, t for batch major; t, b for time major.
                SampleBatch.OBS: TensorSpec(
                    "b, t, d", d=self.config.input_dims[0], framework="tf2"
                ),
                STATE_IN: {
                    "h": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="tf2",
                    ),
                    "c": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="tf2",
                    ),
                },
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TensorSpec(
                    "b, t, d", d=self.config.output_dims[0], framework="tf2"
                ),
                STATE_OUT: {
                    "h": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="tf2",
                    ),
                    "c": TensorSpec(
                        "b, l, h",
                        h=self.config.hidden_dim,
                        l=self.config.num_layers,
                        framework="tf2",
                    ),
                },
            }
        )

    @override(Model)
    def get_initial_state(self):
        return {
            "h": tf.zeros((self.config.num_layers, self.config.hidden_dim)),
            "c": tf.zeros((self.config.num_layers, self.config.hidden_dim)),
        }

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        out = tf.cast(inputs[SampleBatch.OBS], tf.float32)

        # States are batch-first when coming in. Make them layers-first.
        states_in = tree.map_structure(
            lambda s: tf.transpose(s, perm=[1, 0, 2]),
            inputs[STATE_IN],
        )

        states_out_h = []
        states_out_c = []
        for i, layer in enumerate(self.lstms):
            out, h, c = layer(out, (states_in["h"][i], states_in["c"][i]))
            states_out_h.append(h)
            states_out_c.append(c)

        out = self.linear(out)

        return {
            ENCODER_OUT: out,
            # Make state_out batch-first.
            STATE_OUT: {"h": tf.stack(states_out_h, 1), "c": tf.stack(states_out_c, 1)},
        }
