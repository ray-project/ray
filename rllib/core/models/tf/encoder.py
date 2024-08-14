from typing import Dict, Optional

import tree  # pip install dm_tree

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import (
    Encoder,
    ActorCriticEncoder,
    StatefulActorCriticEncoder,
    ENCODER_OUT,
    tokenize,
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
from ray.rllib.core.models.specs.specs_base import Spec, TensorSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.models.utils import get_initializer_fn
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

_, tf, _ = try_import_tf()


class TfActorCriticEncoder(TfModel, ActorCriticEncoder):
    """An encoder that can hold two encoders."""

    framework = "tf2"

    def __init__(self, config: ActorCriticEncoderConfig) -> None:
        # We have to call TfModel.__init__ first, because it calls the constructor of
        # tf.keras.Model, which is required to be called before models are created.
        TfModel.__init__(self, config)
        ActorCriticEncoder.__init__(self, config)


class TfStatefulActorCriticEncoder(TfModel, StatefulActorCriticEncoder):
    """A stateful actor-critic encoder for torch."""

    framework = "tf2"

    def __init__(self, config: ActorCriticEncoderConfig) -> None:
        # We have to call TfModel.__init__ first, because it calls the constructor of
        # tf.keras.Model, which is required to be called before models are created.
        TfModel.__init__(self, config)
        StatefulActorCriticEncoder.__init__(self, config)


class TfCNNEncoder(TfModel, Encoder):
    def __init__(self, config: CNNEncoderConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Add an input layer for the Sequential, created below. This is really
        # important to be able to derive the model's trainable_variables early on
        # (inside our Learners).
        layers = [tf.keras.layers.Input(shape=config.input_dims)]
        # The bare-bones CNN (no flatten, no succeeding dense).
        cnn = TfCNN(
            input_dims=config.input_dims,
            cnn_filter_specifiers=config.cnn_filter_specifiers,
            cnn_activation=config.cnn_activation,
            cnn_use_layernorm=config.cnn_use_layernorm,
            cnn_use_bias=config.cnn_use_bias,
            cnn_kernel_initializer=config.cnn_kernel_initializer,
            cnn_kernel_initializer_config=config.cnn_kernel_initializer_config,
            cnn_bias_initializer=config.cnn_bias_initializer,
            cnn_bias_initializer_config=config.cnn_bias_initializer_config,
        )
        layers.append(cnn)

        # Add a flatten operation to move from 2/3D into 1D space.
        if config.flatten_at_end:
            layers.append(tf.keras.layers.Flatten())

        # Create the network from gathered layers.
        self.net = tf.keras.Sequential(layers)

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                Columns.OBS: TensorSpec(
                    "b, w, h, c",
                    w=self.config.input_dims[0],
                    h=self.config.input_dims[1],
                    c=self.config.input_dims[2],
                    framework="tf2",
                ),
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: (
                    TensorSpec("b, d", d=self.config.output_dims[0], framework="tf2")
                    if self.config.flatten_at_end
                    else TensorSpec(
                        "b, w, h, c",
                        w=self.config.output_dims[0],
                        h=self.config.output_dims[1],
                        d=self.config.output_dims[2],
                        framework="tf2",
                    )
                )
            }
        )

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        return {ENCODER_OUT: self.net(inputs[Columns.OBS])}


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
            hidden_layer_use_bias=config.hidden_layer_use_bias,
            hidden_layer_weights_initializer=config.hidden_layer_weights_initializer,
            hidden_layer_weights_initializer_config=(
                config.hidden_layer_weights_initializer_config
            ),
            hidden_layer_bias_initializer=config.hidden_layer_bias_initializer,
            hidden_layer_bias_initializer_config=(
                config.hidden_layer_bias_initializer_config
            ),
            output_dim=config.output_layer_dim,
            output_activation=config.output_layer_activation,
            output_use_bias=config.output_layer_use_bias,
            output_weights_initializer=config.output_layer_weights_initializer,
            output_weights_initializer_config=(
                config.output_layer_weights_initializer_config
            ),
            output_bias_initializer=config.output_layer_bias_initializer,
            output_bias_initializer_config=config.output_layer_bias_initializer_config,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                Columns.OBS: TensorSpec(
                    "b, d", d=self.config.input_dims[0], framework="tf2"
                ),
            }
        )

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                ENCODER_OUT: TensorSpec(
                    "b, d", d=self.config.output_dims[0], framework="tf2"
                ),
            }
        )

    @override(Model)
    def _forward(self, inputs: Dict, **kwargs) -> Dict:
        return {ENCODER_OUT: self.net(inputs[Columns.OBS])}


class TfGRUEncoder(TfModel, Encoder):
    """A recurrent GRU encoder.

    This encoder has...
    - Zero or one tokenizers.
    - One or more GRU layers.
    - One linear output layer.
    """

    def __init__(self, config: RecurrentEncoderConfig) -> None:
        TfModel.__init__(self, config)

        # Maybe create a tokenizer
        if config.tokenizer_config is not None:
            self.tokenizer = config.tokenizer_config.build(framework="tf2")
            # For our first input dim, we infer from the tokenizer.
            # This is necessary because we need to build the layers in order to be
            # able to get/set weights directly after instantiation.
            input_dims = (1,) + tuple(
                self.tokenizer.output_specs[ENCODER_OUT].full_shape
            )
        else:
            self.tokenizer = None
            input_dims = (
                1,
                1,
            ) + tuple(config.input_dims)

        gru_weights_initializer = get_initializer_fn(
            config.hidden_weights_initializer, framework="tf2"
        )
        gru_bias_initializer = get_initializer_fn(
            config.hidden_bias_initializer, framework="tf2"
        )

        # Create the tf GRU layers.
        self.grus = []
        for _ in range(config.num_layers):
            layer = tf.keras.layers.GRU(
                config.hidden_dim,
                time_major=not config.batch_major,
                # Note, if the initializer is `None`, we want TensorFlow
                # to use its default one. So we pass in `None`.
                kernel_initializer=(
                    gru_weights_initializer(**config.hidden_weights_initializer_config)
                    if config.hidden_weights_initializer_config
                    else gru_weights_initializer
                ),
                use_bias=config.use_bias,
                bias_initializer=(
                    gru_bias_initializer(**config.hidden_bias_initializer_config)
                    if config.hidden_bias_initializer_config
                    else gru_bias_initializer
                ),
                return_sequences=True,
                return_state=True,
            )
            layer.build(input_dims)
            input_dims = (1, 1, config.hidden_dim)
            self.grus.append(layer)

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                # b, t for batch major; t, b for time major.
                Columns.OBS: TensorSpec(
                    "b, t, d", d=self.config.input_dims[0], framework="tf2"
                ),
                Columns.STATE_IN: {
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
                Columns.STATE_OUT: {
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
    def _forward(self, inputs: Dict, **kwargs) -> Dict:
        outputs = {}

        if self.tokenizer is not None:
            # Push observations through the tokenizer encoder if we built one.
            out = tokenize(self.tokenizer, inputs, framework="tf2")
        else:
            # Otherwise, just use the raw observations.
            out = tf.cast(inputs[Columns.OBS], tf.float32)

        # States are batch-first when coming in. Make them layers-first.
        states_in = tree.map_structure(
            lambda s: tf.transpose(s, perm=[1, 0] + list(range(2, len(s.shape)))),
            inputs[Columns.STATE_IN],
        )

        states_out = []
        for i, layer in enumerate(self.grus):
            out, h = layer(out, states_in["h"][i])
            states_out.append(h)

        # Insert them into the output dict.
        outputs[ENCODER_OUT] = out
        outputs[Columns.STATE_OUT] = {"h": tf.stack(states_out, 1)}
        return outputs


class TfLSTMEncoder(TfModel, Encoder):
    """A recurrent LSTM encoder.

    This encoder has...
    - Zero or one tokenizers.
    - One or more LSTM layers.
    - One linear output layer.
    """

    def __init__(self, config: RecurrentEncoderConfig) -> None:
        TfModel.__init__(self, config)

        # Maybe create a tokenizer
        if config.tokenizer_config is not None:
            self.tokenizer = config.tokenizer_config.build(framework="tf2")
            # For our first input dim, we infer from the tokenizer.
            # This is necessary because we need to build the layers in order to be
            # able to get/set weights directly after instantiation.
            input_dims = (1,) + tuple(
                self.tokenizer.output_specs[ENCODER_OUT].full_shape
            )
        else:
            self.tokenizer = None
            input_dims = (
                1,
                1,
            ) + tuple(config.input_dims)

        lstm_weights_initializer = get_initializer_fn(
            config.hidden_weights_initializer, framework="tf2"
        )
        lstm_bias_initializer = get_initializer_fn(
            config.hidden_bias_initializer, framework="tf2"
        )

        # Create the tf LSTM layers.
        self.lstms = []
        for _ in range(config.num_layers):
            layer = tf.keras.layers.LSTM(
                config.hidden_dim,
                time_major=not config.batch_major,
                # Note, if the initializer is `None`, we want TensorFlow
                # to use its default one. So we pass in `None`.
                kernel_initializer=(
                    lstm_weights_initializer(**config.hidden_weights_initializer_config)
                    if config.hidden_weights_initializer_config
                    else lstm_weights_initializer
                ),
                use_bias=config.use_bias,
                bias_initializer=(
                    lstm_bias_initializer(**config.hidden_bias_initializer_config)
                    if config.hidden_bias_initializer_config
                    else "zeros"
                ),
                return_sequences=True,
                return_state=True,
            )
            layer.build(input_dims)
            input_dims = (1, 1, config.hidden_dim)
            self.lstms.append(layer)

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return SpecDict(
            {
                # b, t for batch major; t, b for time major.
                Columns.OBS: TensorSpec(
                    "b, t, d", d=self.config.input_dims[0], framework="tf2"
                ),
                Columns.STATE_IN: {
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
                Columns.STATE_OUT: {
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
    def _forward(self, inputs: Dict, **kwargs) -> Dict:
        outputs = {}

        if self.tokenizer is not None:
            # Push observations through the tokenizer encoder if we built one.
            out = tokenize(self.tokenizer, inputs, framework="tf2")
        else:
            # Otherwise, just use the raw observations.
            out = tf.cast(inputs[Columns.OBS], tf.float32)

        # States are batch-first when coming in. Make them layers-first.
        states_in = tree.map_structure(
            lambda s: tf.transpose(s, perm=[1, 0, 2]),
            inputs[Columns.STATE_IN],
        )

        states_out_h = []
        states_out_c = []
        for i, layer in enumerate(self.lstms):
            out, h, c = layer(out, (states_in["h"][i], states_in["c"][i]))
            states_out_h.append(h)
            states_out_c.append(c)

        # Insert them into the output dict.
        outputs[ENCODER_OUT] = out
        outputs[Columns.STATE_OUT] = {
            "h": tf.stack(states_out_h, 1),
            "c": tf.stack(states_out_c, 1),
        }
        return outputs
