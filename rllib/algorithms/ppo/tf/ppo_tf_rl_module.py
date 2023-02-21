from typing import Mapping, Any, List

import gymnasium as gym

from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOModuleConfig
from ray.rllib.core.models.base import ACTOR, CRITIC, STATE_IN
from ray.rllib.core.models.configs import (
    MLPHeadConfig,
    MLPEncoderConfig,
    ActorCriticEncoderConfig,
    LSTMEncoderConfig,
)
from ray.rllib.core.rl_module.rl_module import RLModuleConfig, RLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.tf.encoder import ENCODER_OUT
from ray.rllib.models.tf.tf_distributions import (
    TfCategorical,
    TfDiagGaussian,
    TfDeterministic,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.utils.nested_dict import NestedDict


tf1, tf, _ = try_import_tf()
tf1.enable_eager_execution()


class PPOTfRLModule(TfRLModule):
    def __init__(self, config: RLModuleConfig):
        super().__init__()
        self.config = config

        assert self.config.pi_config, "pi_config must be provided."
        assert self.config.vf_config, "vf_config must be provided."
        assert self.config.encoder_config, "Encoder config must be provided."

        self.config.encoder_config.input_dim = self.config.observation_space.shape[0]
        self.config.pi_config.input_dim = (
            self.config.encoder_config.base_encoder_config.output_dim
        )
        self.config.vf_config.input_dim = (
            self.config.encoder_config.base_encoder_config.output_dim
        )

        if isinstance(self.config.action_space, gym.spaces.Discrete):
            self.config.pi_config.output_dim = self.config.action_space.n
        else:
            self.config.pi_config.output_dim = self.config.action_space.shape[0] * 2
        self.config.vf_config.output_dim = 1

        # TODO(Artur): Unify to tf and torch setup with Catalog
        self.encoder = self.config.encoder_config.build(framework="tf")
        self.pi = self.config.pi_config.build(framework="tf")
        self.vf = self.config.vf_config.build(framework="tf")

        self._is_discrete = isinstance(
            convert_old_gym_space_to_gymnasium_space(self.config.action_space),
            gym.spaces.Discrete,
        )

    # TODO(Artur): Comment in as soon as we support RNNs from Polciy side
    # @override(RLModule)
    # def get_initial_state(self) -> NestedDict:
    #     if hasattr(self.encoder, "get_initial_state"):
    #         return self.encoder.get_initial_state()
    #     else:
    #         return NestedDict({})

    @override(RLModule)
    def input_specs_train(self) -> List[str]:
        return [SampleBatch.OBS, SampleBatch.ACTIONS]

    @override(RLModule)
    def output_specs_train(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST,
            SampleBatch.VF_PREDS,
        ]

    @override(RLModule)
    def input_specs_exploration(self):
        # TODO (Artur): Infer from encoder specs as soon as Policy supports RNN
        return SpecDict()

    @override(RLModule)
    def output_specs_exploration(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST,
            SampleBatch.VF_PREDS,
            SampleBatch.ACTION_DIST_INPUTS,
        ]

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return SpecDict({SampleBatch.ACTION_DIST: TfDeterministic})

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        # TODO (Artur): Remove this once Policy supports RNN
        if self.encoder.config.shared:
            batch[STATE_IN] = None
        else:
            batch[STATE_IN] = {
                ACTOR: None,
                CRITIC: None,
            }
        batch[SampleBatch.SEQ_LENS] = None

        encoder_outs = self.encoder(batch)
        # TODO (Artur): Un-uncomment once Policy supports RNN
        # output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Actions
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        if self._is_discrete:
            action = tf.math.argmax(action_logits, axis=-1)
        else:
            action, _ = tf.split(action_logits, num_or_size_splits=2, axis=1)

        action_dist = TfDeterministic(loc=action)
        output[SampleBatch.ACTION_DIST] = action_dist

        return output

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        """PPO forward pass during exploration.
        Besides the action distribution, this method also returns the parameters of the
        policy distribution to be used for computing KL divergence between the old
        policy and the new policy during training.
        """
        output = {}

        # TODO (Artur): Remove this once Policy supports RNN
        if self.encoder.config.shared:
            batch[STATE_IN] = None
        else:
            batch[STATE_IN] = {
                ACTOR: None,
                CRITIC: None,
            }
        batch[SampleBatch.SEQ_LENS] = None

        # Shared encoder
        encoder_outs = self.encoder(batch)
        # TODO (Artur): Un-uncomment once Policy supports RNN
        # output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        output[SampleBatch.VF_PREDS] = tf.squeeze(vf_out, axis=-1)

        # Policy head
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        if self._is_discrete:
            action_dist = TfCategorical(logits=action_logits)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"logits": action_logits}
        else:
            loc, log_std = tf.split(action_logits, num_or_size_splits=2, axis=1)
            scale = tf.math.exp(log_std)
            action_dist = TfDiagGaussian(loc=loc, scale=scale)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"loc": loc, "scale": scale}

        output[SampleBatch.ACTION_DIST] = action_dist

        return output

    @override(TfRLModule)
    def _forward_train(self, batch: NestedDict):
        output = {}

        # TODO (Artur): Remove this once Policy supports RNN
        if self.encoder.config.shared:
            batch[STATE_IN] = None
        else:
            batch[STATE_IN] = {
                ACTOR: None,
                CRITIC: None,
            }
        batch[SampleBatch.SEQ_LENS] = None

        # Shared encoder
        encoder_outs = self.encoder(batch)
        # TODO (Artur): Un-uncomment once Policy supports RNN
        # output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        output[SampleBatch.VF_PREDS] = tf.squeeze(vf_out, axis=-1)

        # Policy head
        pi_out = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        action_logits = pi_out
        if self._is_discrete:
            action_dist = TfCategorical(logits=action_logits)
        else:
            loc, log_std = tf.split(action_logits, num_or_size_splits=2, axis=1)
            scale = tf.math.exp(log_std)
            action_dist = TfDiagGaussian(loc=loc, scale=scale)

        logp = action_dist.logp(batch[SampleBatch.ACTIONS])
        entropy = action_dist.entropy()
        output[SampleBatch.ACTION_DIST] = action_dist
        output[SampleBatch.ACTION_LOGP] = logp
        output["entropy"] = entropy

        return output

    @classmethod
    @override(TfRLModule)
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        *,
        model_config: Mapping[str, Any],
    ) -> "PPOTfRLModule":
        # TODO: use the new catalog to perform this logic and construct the final config

        obs_dim = observation_space.shape[0]
        fcnet_hiddens = model_config["fcnet_hiddens"]
        fcnet_activation = model_config["fcnet_activation"]
        post_fcnet_hiddens = model_config["post_fcnet_hiddens"]
        post_fcnet_activation = model_config["post_fcnet_activation"]
        use_lstm = model_config["use_lstm"]
        shared_encoder = model_config["vf_share_layers"]

        if use_lstm:
            raise ValueError("LSTM not supported by PPOTfRLModule yet.")

        if model_config["use_lstm"]:
            base_encoder_config = LSTMEncoderConfig(
                input_dim=obs_dim,
                hidden_dim=model_config["lstm_cell_size"],
                batch_first=not model_config["_time_major"],
                num_layers=1,
                output_dim=model_config["lstm_cell_size"],
            )
        else:
            base_encoder_config = MLPEncoderConfig(
                input_dim=obs_dim,
                hidden_layer_dims=fcnet_hiddens[:-1],
                hidden_layer_activation=fcnet_activation,
                output_dim=fcnet_hiddens[-1],
                output_activation=fcnet_activation,
            )

        encoder_config = ActorCriticEncoderConfig(
            base_encoder_config=base_encoder_config,
            shared=shared_encoder,
        )

        pi_config = MLPHeadConfig(
            input_dim=base_encoder_config.output_dim,
            hidden_layer_dims=post_fcnet_hiddens,
            hidden_layer_activation=post_fcnet_activation,
            output_activation="linear",
        )
        vf_config = MLPHeadConfig(
            input_dim=base_encoder_config.output_dim,
            hidden_layer_dims=post_fcnet_hiddens,
            hidden_layer_activation=post_fcnet_activation,
            output_activation="linear",
        )

        assert isinstance(
            observation_space, gym.spaces.Box
        ), "This simple PPOModule only supports Box observation space."

        assert (
            len(observation_space.shape) == 1
        ), "This simple PPOModule only supports 1D observation space."

        assert isinstance(action_space, (gym.spaces.Discrete, gym.spaces.Box)), (
            "This simple PPOModule only supports Discrete and Box action space.",
        )

        config_ = PPOModuleConfig(
            observation_space=observation_space,
            action_space=action_space,
            encoder_config=encoder_config,
            pi_config=pi_config,
            vf_config=vf_config,
        )

        module = PPOTfRLModule(config_)
        return module
