import tree

from ray.rllib.core.models.base import ModelConfig, Model
from ray.rllib.core.models.encoder import (
    Encoder,
    STATE_IN,
    STATE_OUT,
    ENCODER_OUT,
    ACTOR,
    CRITIC,
)
from ray.rllib.core.models.tf.primitives import TfMLP
from ray.rllib.core.models.tf.primitives import TfModel
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict

torch, nn = try_import_torch()


class TfMLPEncoder(Encoder, TfModel):
    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural networks
        self.net = TfMLP(
            input_dim=config.input_dim,
            hidden_layer_dims=config.hidden_layer_dims,
            output_dim=config.output_dim,
            hidden_layer_activation=config.hidden_layer_activation,
        )

        # Define the input spec
        self.input_spec = SpecDict(
            {
                SampleBatch.OBS: TFTensorSpecs("b, h", h=self.config.input_dim),
                STATE_IN: None,
                SampleBatch.SEQ_LENS: None,
            }
        )

        # Define the output spec
        self.output_spec = SpecDict(
            {
                ENCODER_OUT: TFTensorSpecs("b, h", h=self.config.output_dim),
                STATE_OUT: None,
            }
        )

    @override(Model)
    def _forward(self, inputs: NestedDict) -> NestedDict:
        return {
            ENCODER_OUT: self.net(inputs[SampleBatch.OBS]),
            STATE_OUT: inputs[STATE_IN],
        }


class LSTMEncoder(Encoder, TfModel):
    """An encoder that uses an LSTM cell and a linear layer."""

    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural networks
        self.lstm = nn.LSTM(
            config.input_dim,
            config.hidden_dim,
            config.num_layers,
            batch_first=config.batch_first,
        )
        self.linear = nn.Linear(config.hidden_dim, config.output_dim)

        # Define the input spec
        self.input_spec = SpecDict(
            {
                # bxt is just a name for better readability to indicated padded batch
                SampleBatch.OBS: TFTensorSpecs("bxt, h", h=config.input_dim),
                STATE_IN: {
                    "h": TFTensorSpecs(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                    "c": TFTensorSpecs(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                },
                SampleBatch.SEQ_LENS: None,
            }
        )

        # Define the output spec
        self.output_spec = SpecDict(
            {
                ENCODER_OUT: TFTensorSpecs("bxt, h", h=config.output_dim),
                STATE_OUT: {
                    "h": TFTensorSpecs(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                    "c": TFTensorSpecs(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
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
        x = inputs[SampleBatch.OBS]
        states = inputs[STATE_IN]
        # states are batch-first when coming in
        states = tree.map_structure(lambda x: x.transpose(0, 1), states)

        x = add_time_dimension(
            x,
            seq_lens=inputs[SampleBatch.SEQ_LENS],
            framework="tf",
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


class TfIdentityEncoder(TfModel):
    """An encoder that does nothing but passing on inputs.

    We use this so that we avoid having many if/else statements in the RLModule.
    """

    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)

        # Define the input spec
        self.input_spec = SpecDict(
            {
                # Use the output dim as input dim because identity.
                SampleBatch.OBS: TFTensorSpecs("b, h", h=config.output_dim),
                STATE_IN: None,
                SampleBatch.SEQ_LENS: None,
            }
        )

        # Define the output spec
        self.output_spec = SpecDict(
            {
                ENCODER_OUT: TFTensorSpecs("b, h", h=config.input_dim),
                STATE_OUT: None,
            }
        )

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        return {ENCODER_OUT: inputs[SampleBatch.OBS], STATE_OUT: inputs[STATE_IN]}


class TfActorCriticEncoder(TfModel, Encoder):
    """An encoder that potentially holds two encoders.

    This is a special case of encoder that potentially holds two encoders:
    One for the actor and one for the critic. If not, it will use the same encoder
    for both. The two encoders are of the same type and we can therefore make the
    assumption that they have the same input and output specs.
    """

    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural networks
        if self.config.shared:
            self.encoder = self.config.base_encoder_config.build(framework="tf")
        else:
            self.actor_encoder = self.config.base_encoder_config.build(framework="tf")
            self.critic_encoder = self.config.base_encoder_config.build(framework="tf")

        # Define the input spec
        if self.config.shared:
            self.input_spec = self.encoder.input_spec
        else:
            actor_input_spec = self.actor_encoder.input_spec
            critic_input_spec = self.critic_encoder.input_spec
            assert actor_input_spec == critic_input_spec
            # We only make assumptions about OBS, STATE_IN and SEQ_LENS here.
            # By extending this class, one could feed inputs that hold more keys.

            self.input_spec = SpecDict(
                {
                    SampleBatch.OBS: actor_input_spec[SampleBatch.OBS],
                    STATE_IN: {
                        ACTOR: actor_input_spec[STATE_IN],
                        CRITIC: critic_input_spec[STATE_IN],
                    },
                    SampleBatch.SEQ_LENS: actor_input_spec[SampleBatch.SEQ_LENS],
                }
            )

        # Define the output spec
        if self.config.shared:
            state_out_spec = self.encoder.output_spec[STATE_OUT]
            actor_out_spec = self.encoder.output_spec[ENCODER_OUT]
            critic_out_spec = self.encoder.output_spec[ENCODER_OUT]
        else:
            state_out_spec = {
                ACTOR: self.actor_encoder.output_spec[STATE_OUT],
                CRITIC: self.critic_encoder.output_spec[STATE_OUT],
            }
            actor_out_spec = self.actor_encoder.output_spec[ENCODER_OUT]
            critic_out_spec = self.critic_encoder.output_spec[ENCODER_OUT]
            assert actor_out_spec == critic_out_spec

        self.output_spec = SpecDict(
            {
                ENCODER_OUT: {ACTOR: actor_out_spec, CRITIC: critic_out_spec},
                STATE_OUT: state_out_spec,
            }
        )

    @override(Model)
    def get_initial_state(self):
        if self.config.shared:
            return self.encoder.get_initial_state()
        else:
            return {
                ACTOR: self.actor_encoder.get_initial_state(),
                CRITIC: self.critic_encoder.get_initial_state(),
            }

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        if self.config.shared:
            outs = self.encoder(inputs, **kwargs)
            return {
                ENCODER_OUT: {ACTOR: outs[ENCODER_OUT], CRITIC: outs[ENCODER_OUT]},
                STATE_OUT: outs[STATE_OUT],
            }
        else:
            actor_inputs = NestedDict({**inputs, **{STATE_IN: inputs[STATE_IN][ACTOR]}})
            critic_inputs = NestedDict(
                {**inputs, **{STATE_IN: inputs[STATE_IN][CRITIC]}}
            )
            actor_out = self.actor_encoder(actor_inputs, **kwargs)
            critic_out = self.critic_encoder(critic_inputs, **kwargs)
            return {
                ENCODER_OUT: {
                    ACTOR: actor_out[ENCODER_OUT],
                    CRITIC: critic_out[ENCODER_OUT],
                },
                STATE_OUT: {
                    ACTOR: actor_out[STATE_OUT],
                    CRITIC: critic_out[ENCODER_OUT],
                },
            }
