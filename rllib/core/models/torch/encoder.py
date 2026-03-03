import tree

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import (
    ENCODER_OUT,
    ActorCriticEncoder,
    Encoder,
    Model,
    StatefulActorCriticEncoder,
    tokenize,
)
from ray.rllib.core.models.configs import (
    ActorCriticEncoderConfig,
    CNNEncoderConfig,
    MLPEncoderConfig,
    MultiStreamEncoderConfig,
    RecurrentEncoderConfig,
)
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.torch.primitives import TorchCNN, TorchMLP
from ray.rllib.models.utils import get_activation_fn, get_initializer_fn
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
            output_layer_use_layernorm=config.output_layer_use_layernorm,
            output_weights_initializer=config.output_layer_weights_initializer,
            output_weights_initializer_config=(
                config.output_layer_weights_initializer_config
            ),
            output_bias_initializer=config.output_layer_bias_initializer,
            output_bias_initializer_config=config.output_layer_bias_initializer_config,
        )

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        return {ENCODER_OUT: self.net(inputs[Columns.OBS])}


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
            cnn_kernel_initializer=config.cnn_kernel_initializer,
            cnn_kernel_initializer_config=config.cnn_kernel_initializer_config,
            cnn_bias_initializer=config.cnn_bias_initializer,
            cnn_bias_initializer_config=config.cnn_bias_initializer_config,
        )
        layers.append(cnn)

        # Add a flatten operation to move from 2/3D into 1D space.
        if config.flatten_at_end:
            layers.append(nn.Flatten())

        # Create the network from gathered layers.
        self.net = nn.Sequential(*layers)

    @override(Model)
    def _forward(self, inputs: dict, **kwargs) -> dict:
        return {ENCODER_OUT: self.net(inputs[Columns.OBS])}


class TorchGRUEncoder(TorchModel, Encoder):
    """A recurrent GRU encoder.

    This encoder has...
    - Zero or one tokenizers.
    - One or more GRU layers.
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

        gru_weights_initializer = get_initializer_fn(
            config.hidden_weights_initializer, framework="torch"
        )
        gru_bias_initializer = get_initializer_fn(
            config.hidden_bias_initializer, framework="torch"
        )

        # Create the torch GRU layer.
        self.gru = nn.GRU(
            gru_input_dim,
            config.hidden_dim,
            config.num_layers,
            batch_first=config.batch_major,
            bias=config.use_bias,
        )

        # Initialize, GRU weights, if necessary.
        if gru_weights_initializer:
            gru_weights_initializer(
                self.gru.weight, **config.hidden_weights_initializer_config or {}
            )
        # Initialize GRU bias, if necessary.
        if gru_bias_initializer:
            gru_bias_initializer(
                self.gru.weight, **config.hidden_bias_initializer_config or {}
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
            out = inputs[Columns.OBS].float()

        # States are batch-first when coming in. Make them layers-first.
        states_in = tree.map_structure(
            lambda s: s.transpose(0, 1), inputs[Columns.STATE_IN]
        )

        out, states_out = self.gru(out, states_in["h"])
        states_out = {"h": states_out}

        # Insert them into the output dict.
        outputs[ENCODER_OUT] = out
        outputs[Columns.STATE_OUT] = tree.map_structure(
            lambda s: s.transpose(0, 1), states_out
        )
        return outputs


class TorchLSTMEncoder(TorchModel, Encoder):
    """A recurrent LSTM encoder.

    This encoder has...
    - Zero or one tokenizers.
    - One or more LSTM layers.
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

        lstm_weights_initializer = get_initializer_fn(
            config.hidden_weights_initializer, framework="torch"
        )
        lstm_bias_initializer = get_initializer_fn(
            config.hidden_bias_initializer, framework="torch"
        )

        # Create the torch LSTM layer.
        self.lstm = nn.LSTM(
            lstm_input_dim,
            config.hidden_dim,
            config.num_layers,
            batch_first=config.batch_major,
            bias=config.use_bias,
        )

        # Initialize LSTM layer weigths and biases, if necessary.
        for layer in self.lstm.all_weights:
            if lstm_weights_initializer:
                lstm_weights_initializer(
                    layer[0], **config.hidden_weights_initializer_config or {}
                )
                lstm_weights_initializer(
                    layer[1], **config.hidden_weights_initializer_config or {}
                )
            if lstm_bias_initializer:
                lstm_bias_initializer(
                    layer[2], **config.hidden_bias_initializer_config or {}
                )
                lstm_bias_initializer(
                    layer[3], **config.hidden_bias_initializer_config or {}
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
            out = inputs[Columns.OBS].float()

        # States are batch-first when coming in. Make them layers-first.
        states_in = tree.map_structure(
            lambda s: s.transpose(0, 1), inputs[Columns.STATE_IN]
        )

        out, states_out = self.lstm(out, (states_in["h"], states_in["c"]))
        states_out = {"h": states_out[0], "c": states_out[1]}

        # Insert them into the output dict.
        outputs[ENCODER_OUT] = out
        outputs[Columns.STATE_OUT] = tree.map_structure(
            lambda s: s.transpose(0, 1), states_out
        )
        return outputs


class TorchMultiStreamEncoder(TorchModel, Encoder):
    """An encoder that encodes multiple input streams separately.

    Each input stream is encoded with its own encoder defined in
    `base_encoder_configs`. The resulting embeddings are concatenated and
    passed through a final fusion network to produce the output embedding.
    """

    def __init__(self, config: MultiStreamEncoderConfig) -> None:
        TorchModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural network for observation stream.
        self.base_encoders = nn.ModuleDict(
            {
                k: cfg.build(framework="torch")
                for k, cfg in sorted(config.base_encoder_configs.items())
            }
        )

        # Get activation functions.
        self.hidden_activation = (
            get_activation_fn(config.hidden_layer_activation, framework="torch")
            or nn.Identity
        )

        # Calculate total embed dim.
        self._total_embed_dim = sum(
            cfg.output_dims[0] for cfg in config.base_encoder_configs.values()
        )

        # Build fusion layers (hidden layers with skip connections).
        fusion_layers = []
        if config.hidden_layer_dims:
            # Input layer: total_embed_dim -> hidden_layer_dims[0]
            input_layer = nn.Linear(
                self._total_embed_dim,
                config.hidden_layer_dims[0],
                bias=config.hidden_layer_use_bias,
            )
            fusion_layers.append(input_layer)

            # Intermediate fusion layers with skip connections.
            for i in range(1, len(config.hidden_layer_dims)):
                fusion_layers.append(
                    nn.Linear(
                        config.hidden_layer_dims[i - 1] + self._total_embed_dim,
                        config.hidden_layer_dims[i],
                        bias=config.hidden_layer_use_bias,
                    )
                )

            # Initialize hidden layer weights if necessary.
            if config.hidden_layer_weights_initializer:
                hidden_weights_initializer = get_initializer_fn(
                    config.hidden_layer_weights_initializer, framework="torch"
                )
                for layer in fusion_layers:
                    hidden_weights_initializer(
                        layer.weight,
                        **config.hidden_layer_weights_initializer_config or {}
                    )
            # Initialize hidden layer bias if necessary.
            if config.hidden_layer_bias_initializer:
                hidden_bias_initializer = get_initializer_fn(
                    config.hidden_layer_bias_initializer, framework="torch"
                )
                for layer in fusion_layers:
                    hidden_bias_initializer(
                        layer.bias, **config.hidden_layer_bias_initializer_config or {}
                    )

        self.fusion_layers = nn.ModuleList(fusion_layers)

        # Build output layer only if output_layer_dim is defined.
        self.output_layer = None
        if config.output_layer_dim is not None:
            # Get output activation function.
            self.output_activation = (
                get_activation_fn(config.output_layer_activation, framework="torch")
                or nn.Identity
            )
            # Determine input dim for output layer.
            if config.hidden_layer_dims:
                output_input_dim = config.hidden_layer_dims[-1] + self._total_embed_dim
            else:
                output_input_dim = self._total_embed_dim

            self.output_layer = nn.Linear(
                output_input_dim,
                config.output_layer_dim,
                bias=config.output_layer_use_bias,
            )

            # Initialize output layer weights if necessary.
            if config.output_layer_weights_initializer:
                output_weights_initializer = get_initializer_fn(
                    config.output_layer_weights_initializer, framework="torch"
                )
                output_weights_initializer(
                    self.output_layer.weight,
                    **config.output_layer_weights_initializer_config or {}
                )
            # Initialize output layer bias if necessary.
            if config.output_layer_bias_initializer:
                output_bias_initializer = get_initializer_fn(
                    config.output_layer_bias_initializer, framework="torch"
                )
                output_bias_initializer(
                    self.output_layer.bias,
                    **config.output_layer_bias_initializer_config or {}
                )

    @override(Model)
    def _forward(self, inputs, **kwargs):
        # Run the inputs through the base encoders.
        keys = sorted(self.config.base_encoder_configs.keys())
        encoder_outs = [
            self.base_encoders[k]({Columns.OBS: inputs[k]})[ENCODER_OUT] for k in keys
        ]
        # Concatenate the embeddings.
        embeds = torch.cat(encoder_outs, dim=-1)

        # Pass through fusion layers (if any).
        out = embeds
        if self.fusion_layers:
            out = self.hidden_activation()(self.fusion_layers[0](embeds))
            for layer in self.fusion_layers[1:]:
                out = self.hidden_activation()(layer(torch.cat([out, embeds], dim=-1)))

        # Pass through output layer (if defined).
        if self.output_layer is not None:
            # Concatenate with skip connection if we have fusion layers.
            if self.fusion_layers:
                out = torch.cat([out, embeds], dim=-1)
            out = self.output_activation()(self.output_layer(out))

        return {ENCODER_OUT: out}
