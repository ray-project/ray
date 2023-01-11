from dataclasses import dataclass
import gymnasium as gym
from typing import Mapping, Any, List
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule

from ray.rllib.core.rl_module.encoder_tf import FCTfConfig

from ray.rllib.utils.framework import try_import_tf, try_import_tfp

from ray.rllib.utils.annotations import override

from ray.rllib.utils.nested_dict import NestedDict

from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.policy.sample_batch import SampleBatch

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()

tf1.enable_eager_execution()


@dataclass
class PPOTfModuleConfig(RLModuleConfig):
    """Configuration for the PPO module.

    Attributes:
        pi_config: The configuration for the policy network.
        vf_config: The configuration for the value network.
    """

    pi_config: FCTfConfig = None
    vf_config: FCTfConfig = None
    observation_space: gym.Space = None
    action_space: gym.Space = None


class PPOTfModule(TfRLModule):
    def __init__(self, config: PPOTfModuleConfig):
        super().__init__()
        self.policy = config.pi_config.build()
        self.value_function = config.vf_config.build()
        self._obs_space = config.observation_space
        self._action_space = config.action_space
        self._is_discrete = isinstance(self._action_space, gym.spaces.Discrete)
        self._action_dim = (
            self._action_space.shape[0]
            if not self._is_discrete
            else self._action_space.n
        )

    @override(TfRLModule)
    def input_specs_train(self) -> SpecDict:
        if self._is_discrete:
            action_spec = TFTensorSpecs("b")
        else:
            action_spec = TFTensorSpecs("b, h", h=self._action_dim)
        spec_dict = {}
        spec_dict[SampleBatch.ACTIONS] = action_spec
        spec_dict[SampleBatch.OBS] = TFTensorSpecs("b, h", h=self._obs_space.shape[0])
        spec = SpecDict(spec_dict)
        return spec

    @override(TfRLModule)
    def output_specs_train(self) -> SpecDict:
        if self._is_discrete:
            dist_class = tfp.distributions.Categorical
        else:
            dist_class = tfp.distributions.MultivariateNormalDiag
        spec = SpecDict(
            {
                SampleBatch.ACTION_DIST: dist_class,
                SampleBatch.ACTION_LOGP: TFTensorSpecs("b"),
                SampleBatch.VF_PREDS: TFTensorSpecs("b"),
                "entropy": None,
            }
        )
        return spec

    @override(TfRLModule)
    def _forward_train(self, batch: NestedDict):
        obs = batch[SampleBatch.OBS]
        action_logits = self.policy(obs)
        vf_out = tf.squeeze(self.value_function(obs), axis=1)
        if self._is_discrete:
            action_dist = tfp.distributions.Categorical(logits=action_logits)
        else:
            action_mean, action_scale = tf.split(
                action_logits, num_or_size_splits=2, axis=1
            )
            action_dist = tfp.distributions.MultivariateNormalDiag(
                loc=action_mean, scale_diag=action_scale
            )
        logp = action_dist.log_prob(batch[SampleBatch.ACTIONS])
        entropy = -tf.math.reduce_sum(logp)
        return {
            SampleBatch.ACTION_DIST: action_dist,
            SampleBatch.ACTION_LOGP: logp,
            SampleBatch.VF_PREDS: vf_out,
            "entropy": entropy,
        }

    @override(TfRLModule)
    def input_specs_inference(self) -> SpecDict:
        spec_dict = {}
        spec_dict[SampleBatch.OBS] = TFTensorSpecs("b, h", h=self._obs_space.shape[0])
        spec = SpecDict(spec_dict)
        return spec

    @override(TfRLModule)
    def output_specs_inference(self) -> SpecDict:
        return SpecDict({SampleBatch.ACTION_DIST: tfp.distributions.Deterministic})

    @override(TfRLModule)
    def _forward_inference(self, batch) -> Mapping[str, Any]:
        action_logits = self.policy(batch[SampleBatch.OBS])

        if self._is_discrete:
            action = tf.math.argmax(action_logits, axis=-1)
        else:
            action, _ = tf.split(action_logits, num_or_size_splits=2, axis=1)
        action_dist = tfp.distributions.Deterministic(action)
        output = {SampleBatch.ACTION_DIST: action_dist}
        return output

    @override(TfRLModule)
    def input_specs_exploration(self) -> SpecDict:
        return super().input_specs_exploration()

    @override(TfRLModule)
    def output_specs_exploration(self) -> SpecDict:
        if self._is_discrete:
            dist_class = tfp.distributions.Categorical
        else:
            dist_class = tfp.distributions.MultivariateNormalDiag
        return SpecDict({SampleBatch.ACTION_DIST: dist_class})

    @override(TfRLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        obs = batch[SampleBatch.OBS]
        action_logits = self.policy(obs)

        if self._is_discrete:
            action_dist = tfp.distributions.Categorical(logits=action_logits)
        else:
            action_mean, action_scale = tf.split(
                action_logits, num_or_size_splits=2, axis=1
            )
            action_dist = tfp.distributions.MultivariateNormalDiag(
                loc=action_mean, scale_diag=action_scale
            )

        output = {SampleBatch.ACTION_DIST: action_dist}
        return output

    @classmethod
    @override(TfRLModule)
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        policy_hiddens: List[int],
        vf_hiddens: List[int],
        policy_activation: str = "ReLU",
        vf_activation: str = "ReLU",
    ) -> "PPOTfModule":
        """Create a PPOTfModule"""
        observation_dim = observation_space.shape[0]
        if isinstance(action_space, gym.spaces.Discrete):
            # the dimension should be the number of possible actions since we'll
            # be using a categorical distribution as the output
            action_dist_dim = action_space.n
        elif isinstance(action_space, gym.spaces.Box):
            # the dimension should be 2 x since half the outputs will be used
            # for the mean of the output distribution, and the other half for
            # the covariance matrix
            action_dist_dim = action_space.shape[0] * 2

        pi_config = FCTfConfig(
            input_dim=observation_dim,
            output_dim=action_dist_dim,
            hidden_layers=policy_hiddens,
            activation=policy_activation,
        )
        vf_config = FCTfConfig(
            input_dim=observation_dim,
            output_dim=1,
            hidden_layers=vf_hiddens,
            activation=vf_activation,
        )
        config = PPOTfModuleConfig(
            pi_config=pi_config,
            vf_config=vf_config,
            observation_space=observation_space,
            action_space=action_space,
        )
        return cls(config)


if __name__ == "__main__":
    env = gym.make("Pendulum-v1")

    module = PPOTfModule.from_model_config(
        observation_space=env.observation_space,
        action_space=env.action_space,
        policy_hiddens=[32, 32],
        vf_hiddens=[32, 32],
    )

    obs = [env.observation_space.sample()]
    action = [env.action_space.sample()]

    batch = SampleBatch(
        {
            SampleBatch.OBS: tf.convert_to_tensor(obs),
            SampleBatch.ACTIONS: tf.convert_to_tensor(action),
        }
    )
    print(module.forward_train(batch))
    print(module.forward_inference(batch))
    print(module.forward_exploration(batch))
