import gymnasium as gym
from typing import Mapping, Any, Union
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOCatalog
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models.experimental.encoder import STATE_OUT, ACTOR, CRITIC
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.tf.tf_action_dist import Categorical, Deterministic, DiagGaussian
from ray.rllib.models.experimental.tf.encoder import ENCODER_OUT
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOModuleConfig

tf1, tf, _ = try_import_tf()
tf1.enable_eager_execution()


class PPOTfRLModule(TfRLModule):
    def __init__(self, config: RLModuleConfig):
        super().__init__()
        self.config = config

        self.encoder = self.config.catalog.build_actor_critic_encoder(framework="tf")
        self.pi = self.config.catalog.build_pi_head(framework="tf")
        self.vf = self.config.catalog.build_vf_head(framework="tf")

        self._is_discrete = isinstance(
            convert_old_gym_space_to_gymnasium_space(self.config.action_space),
            gym.spaces.Discrete,
        )

    @classmethod
    @override(RLModule)
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        *,
        model_config: Mapping[str, Any],
    ) -> Union["RLModule", Mapping[str, Any]]:
        free_log_std = model_config["free_log_std"]
        assert not free_log_std, "free_log_std not supported yet."

        if model_config["use_lstm"]:
            raise ValueError("LSTM not supported by PPOTfRLModule yet.")

        assert isinstance(
            observation_space, gym.spaces.Box
        ), "This simple PPOModule only supports Box observation space."

        assert (
            len(observation_space.shape) == 1
        ), "This simple PPOModule only supports 1D observation space."

        assert isinstance(action_space, (gym.spaces.Discrete, gym.spaces.Box)), (
            "This simple PPOModule only supports Discrete and Box action space.",
        )
        catalog = PPOCatalog(
            observation_space=observation_space,
            action_space=action_space,
            model_config=model_config,
        )

        config = PPOModuleConfig(
            observation_space=observation_space,
            action_space=action_space,
            catalog=catalog,
            free_log_std=free_log_std,
        )

        return config.build(framework="tf")

    def get_initial_state(self) -> NestedDict:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        else:
            return NestedDict({})

    @override(RLModule)
    def input_specs_train(self) -> SpecDict:
        if self._is_discrete:
            action_spec = TFTensorSpecs("b")
        else:
            action_dim = self.config.action_space.shape[0]
            action_spec = TFTensorSpecs("b, h", h=action_dim)

        # Convert to SpecDict if needed.
        spec_dict = SpecDict(self.encoder.input_spec)
        spec_dict.update({SampleBatch.ACTIONS: action_spec})
        if SampleBatch.OBS in spec_dict:
            spec_dict[SampleBatch.NEXT_OBS] = spec_dict[SampleBatch.OBS]
        spec = SpecDict(spec_dict)
        return spec

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        spec = SpecDict(
            {
                SampleBatch.ACTION_DIST: Categorical
                if self._is_discrete
                else DiagGaussian,
                SampleBatch.ACTION_LOGP: TFTensorSpecs("b", dtype=tf.float32),
                SampleBatch.VF_PREDS: TFTensorSpecs("b", dtype=tf.float32),
                "entropy": TFTensorSpecs("b", dtype=tf.float32),
            }
        )
        return spec

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        # Shared encoder
        encoder_outs = self.encoder(batch)
        output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        output[SampleBatch.VF_PREDS] = tf.squeeze(vf_out, axis=-1)

        # Policy head
        pi_out = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        action_logits = pi_out
        if self._is_discrete:
            action_dist = Categorical(action_logits)
        else:
            action_dist = DiagGaussian(
                action_logits, None, action_space=self.config.action_space
            )
        logp = action_dist.logp(batch[SampleBatch.ACTIONS])
        entropy = action_dist.entropy()
        output[SampleBatch.ACTION_DIST] = action_dist
        output[SampleBatch.ACTION_LOGP] = logp
        output["entropy"] = entropy

        return output

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return SpecDict({SampleBatch.ACTION_DIST: Deterministic})

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        encoder_outs = self.encoder(batch)
        output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Actions
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        if self._is_discrete:
            action = tf.math.argmax(action_logits, axis=-1)
        else:
            action, _ = tf.split(action_logits, num_or_size_splits=2, axis=1)
        action_dist = Deterministic(action, model=None)
        output[SampleBatch.ACTION_DIST] = action_dist

        return output

    @override(RLModule)
    def input_specs_exploration(self):
        return SpecDict(self.encoder.input_spec)

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        if self._is_discrete:
            action_dist_inputs = ((SampleBatch.ACTION_DIST_INPUTS, "logits"),)
        else:
            action_dist_inputs = (
                (SampleBatch.ACTION_DIST_INPUTS, "loc"),
                (SampleBatch.ACTION_DIST_INPUTS, "scale"),
            )
        return [
            SampleBatch.VF_PREDS,
            SampleBatch.ACTION_DIST,
            *action_dist_inputs,
        ]

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        """PPO forward pass during exploration.

        Besides the action distribution, this method also returns the parameters of the
        policy distribution to be used for computing KL divergence between the old
        policy and the new policy during training.
        """
        output = {}

        # Shared encoder
        encoder_outs = self.encoder(batch)
        output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        output[SampleBatch.VF_PREDS] = tf.squeeze(vf_out, axis=-1)

        # Policy head
        pi_out = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        action_logits = pi_out
        if self._is_discrete:
            action_dist = Categorical(action_logits)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"logits": action_logits}
        else:
            loc, log_std = tf.split(action_logits, 2, axis=-1)
            scale = tf.exp(log_std)
            action_dist = DiagGaussian(
                action_logits, None, action_space=self.config.action_space
            )
            output[SampleBatch.ACTION_DIST_INPUTS] = {"loc": loc, "scale": scale}
        output[SampleBatch.ACTION_DIST] = action_dist

        return output
