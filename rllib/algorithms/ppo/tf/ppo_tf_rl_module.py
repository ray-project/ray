import gymnasium as gym
from typing import Mapping, Any, List
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.core.rl_module.model_configs import FCConfig, IdentityConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.tf.tf_action_dist import Categorical, Deterministic, DiagGaussian
from ray.rllib.models.tf.primitives import FCNet
from ray.rllib.core.rl_module.tf.encoder import ENCODER_OUT
from ray.rllib.algorithms.ppo.ppo import PPOModuleConfig


tf1, tf, _ = try_import_tf()
tf1.enable_eager_execution()


class PPOTfModule(TfRLModule):
    def __init__(self, config: RLModuleConfig):
        super().__init__()
        self.config = config
        self.setup()

    def setup(self) -> None:
        assert self.config.pi_config, "pi_config must be provided."
        assert self.config.vf_config, "vf_config must be provided."
        self.shared_encoder = self.config.shared_encoder_config.build(framework="tf")

        self.pi = FCNet(
            input_dim=self.config.shared_encoder_config.output_dim,
            output_dim=self.config.pi_config.output_dim,
            hidden_layers=self.config.pi_config.hidden_layers,
            activation=self.config.pi_config.activation,
        )

        self.vf = FCNet(
            input_dim=self.config.shared_encoder_config.output_dim,
            output_dim=1,
            hidden_layers=self.config.vf_config.hidden_layers,
            activation=self.config.vf_config.activation,
        )

        self._is_discrete = isinstance(
            convert_old_gym_space_to_gymnasium_space(self.config.action_space),
            gym.spaces.Discrete,
        )

    @override(TfRLModule)
    def input_specs_train(self) -> List[str]:
        return [SampleBatch.OBS, SampleBatch.ACTIONS]

    @override(TfRLModule)
    def output_specs_train(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST,
            SampleBatch.VF_PREDS,
        ]

    @override(TfRLModule)
    def _forward_train(self, batch: NestedDict):
        encoder_out = self.shared_encoder(batch)
        action_logits = self.pi(encoder_out[ENCODER_OUT])
        vf = self.vf(encoder_out[ENCODER_OUT])

        if self._is_discrete:
            action_dist = Categorical(action_logits)
        else:
            action_dist = DiagGaussian(
                action_logits, None, action_space=self.config.action_space
            )

        output = {
            SampleBatch.ACTION_DIST: action_dist,
            SampleBatch.VF_PREDS: tf.squeeze(vf, axis=-1),
        }
        return output

    @override(TfRLModule)
    def input_specs_inference(self) -> List[str]:
        return [SampleBatch.OBS]

    @override(TfRLModule)
    def output_specs_inference(self) -> List[str]:
        return [SampleBatch.ACTION_DIST]

    @override(TfRLModule)
    def _forward_inference(self, batch) -> Mapping[str, Any]:
        encoder_out = self.shared_encoder(batch)

        action_logits = self.pi(encoder_out[ENCODER_OUT])

        if self._is_discrete:
            action = tf.math.argmax(action_logits, axis=-1)
        else:
            action, _ = tf.split(action_logits, num_or_size_splits=2, axis=1)

        action_dist = Deterministic(action, model=None)
        output = {
            SampleBatch.ACTION_DIST: action_dist,
        }
        return output

    @override(TfRLModule)
    def input_specs_exploration(self) -> List[str]:
        return self.input_specs_inference()

    @override(TfRLModule)
    def output_specs_exploration(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST,
            SampleBatch.VF_PREDS,
            SampleBatch.ACTION_DIST_INPUTS,
        ]

    @override(TfRLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        encoder_out = self.shared_encoder(batch)

        action_logits = self.pi(encoder_out[ENCODER_OUT])
        vf = self.vf(encoder_out[ENCODER_OUT])

        if self._is_discrete:
            action_dist = Categorical(action_logits)
        else:
            action_dist = DiagGaussian(
                action_logits, None, action_space=self.config.action_space
            )
        output = {
            SampleBatch.ACTION_DIST: action_dist,
            SampleBatch.ACTION_DIST_INPUTS: action_logits,
            SampleBatch.VF_PREDS: tf.squeeze(vf, axis=-1),
        }
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
        """Create a PPOTfRLModule"""
        activation = model_config["fcnet_activation"]
        if activation == "tanh":
            activation = "Tanh"
        elif activation == "relu":
            activation = "ReLU"
        elif activation == "linear":
            activation = "linear"
        else:
            raise ValueError(f"Unsupported activation: {activation}")
        obs_dim = observation_space.shape[0]
        fcnet_hiddens = model_config["fcnet_hiddens"]
        vf_share_layers = model_config["vf_share_layers"]
        use_lstm = model_config["use_lstm"]
        if use_lstm:
            raise ValueError("LSTM not supported by PPOTfRLModule yet.")
        if vf_share_layers:
            shared_encoder_config = FCConfig(
                input_dim=obs_dim,
                hidden_layers=fcnet_hiddens,
                activation=activation,
                output_dim=model_config["fcnet_hiddens"][-1],
            )
        else:
            shared_encoder_config = IdentityConfig(output_dim=obs_dim)
        assert isinstance(
            observation_space, gym.spaces.Box
        ), "This simple PPOModule only supports Box observation space."

        assert (
            len(observation_space.shape) == 1
        ), "This simple PPOModule only supports 1D observation space."

        assert isinstance(action_space, (gym.spaces.Discrete, gym.spaces.Box)), (
            "This simple PPOModule only supports Discrete and Box action space.",
        )
        pi_config = FCConfig()
        vf_config = FCConfig()
        shared_encoder_config.input_dim = observation_space.shape[0]
        pi_config.input_dim = shared_encoder_config.output_dim
        pi_config.hidden_layers = fcnet_hiddens
        if isinstance(action_space, gym.spaces.Discrete):
            pi_config.output_dim = action_space.n
        else:
            pi_config.output_dim = action_space.shape[0] * 2
        # build vf network
        vf_config.input_dim = shared_encoder_config.output_dim
        vf_config.hidden_layers = fcnet_hiddens
        vf_config.output_dim = 1
        config_ = PPOModuleConfig(
            pi_config=pi_config,
            vf_config=vf_config,
            shared_encoder_config=shared_encoder_config,
            observation_space=observation_space,
            action_space=action_space,
        )
        module = cls(config_)
        return module
