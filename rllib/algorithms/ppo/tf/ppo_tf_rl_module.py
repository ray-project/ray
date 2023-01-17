from dataclasses import dataclass
import gymnasium as gym
from typing import Mapping, Any, List
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.core.rl_module.encoder_tf import FCTfConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.gym import convert_old_gym_space_to_gymnasium_space
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.tf.tf_action_dist import Categorical, Deterministic, DiagGaussian


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
        self._is_discrete = isinstance(
            convert_old_gym_space_to_gymnasium_space(self._action_space),
            gym.spaces.Discrete,
        )
        self._action_dim = (
            self._action_space.shape[0]
            if not self._is_discrete
            else self._action_space.n
        )

    @override(TfRLModule)
    def input_specs_train(self) -> List[str]:
        return [SampleBatch.OBS, SampleBatch.ACTIONS]

    @override(TfRLModule)
    def output_specs_train(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST,
            SampleBatch.ACTION_LOGP,
            SampleBatch.VF_PREDS,
            "entropy",
            SampleBatch.ACTION_DIST_INPUTS,
        ]

    @override(TfRLModule)
    def _forward_train(self, batch: NestedDict):
        obs = batch[SampleBatch.OBS]
        action_logits = self.policy(obs)
        vf_out = tf.squeeze(self.value_function(obs), axis=1)
        if self._is_discrete:
            action_dist = Categorical(action_logits)
        else:
            action_dist = DiagGaussian(
                action_logits, None, action_space=self._action_space
            )
        logp = action_dist.logp(batch[SampleBatch.ACTIONS])
        entropy = -logp
        return {
            SampleBatch.ACTION_DIST: action_dist,
            SampleBatch.ACTION_LOGP: logp,
            SampleBatch.VF_PREDS: vf_out,
            "entropy": entropy,
            SampleBatch.ACTION_DIST_INPUTS: action_logits,
        }

    @override(TfRLModule)
    def input_specs_inference(self) -> List[str]:
        return [SampleBatch.OBS]

    @override(TfRLModule)
    def output_specs_inference(self) -> List[str]:
        return [SampleBatch.ACTION_DIST, SampleBatch.ACTION_DIST_INPUTS]

    @override(TfRLModule)
    def _forward_inference(self, batch) -> Mapping[str, Any]:
        action_logits = self.policy(batch[SampleBatch.OBS])

        if self._is_discrete:
            action = tf.math.argmax(action_logits, axis=-1)
        else:
            action, _ = tf.split(action_logits, num_or_size_splits=2, axis=1)

        action_dist = Deterministic(action, model=None)
        output = {
            SampleBatch.ACTION_DIST: action_dist,
            SampleBatch.ACTION_DIST_INPUTS: action_logits,
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
        obs = batch[SampleBatch.OBS]
        action_logits = self.policy(obs)

        if self._is_discrete:
            action_dist = Categorical(action_logits)
        else:
            action_dist = DiagGaussian(
                action_logits, None, action_space=self._action_space
            )
        vf_preds = tf.squeeze(self.value_function(obs), axis=1)
        output = {
            SampleBatch.ACTION_DIST: action_dist,
            SampleBatch.VF_PREDS: vf_preds,
            SampleBatch.ACTION_DIST_INPUTS: action_logits,
        }
        return output

    @classmethod
    @override(TfRLModule)
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config: Mapping[str, Any] = None,
    ) -> "PPOTfModule":
        """Create a PPOTfModule"""
        if model_config is None:
            model_config = {}
        model_config = model_config["custom_model_config"]
        policy_hiddens: List[int] = model_config.get("policy_hiddens", [32, 32])
        vf_hiddens: List[int] = model_config.get("vf_hiddens", [32, 32])
        policy_activation: str = model_config.get("policy_activation", "ReLU")
        vf_activation: str = model_config.get("vf_activation", "ReLU")
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
        model_config=dict(
            custom_model_config=dict(
                policy_hiddens=[256, 256, 256],
                vf_hiddens=[256, 256, 256],
            )
        ),
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
