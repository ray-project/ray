"""
This example shows how to take full control over what models and action distribution
are being built inside an RL Module. With this pattern, we can bypass a Catalog and
explicitly define our own models within a given RL Module.
"""
# __sphinx_doc_begin__
import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.examples.envs.classes.random_env import RandomEnv
from ray.rllib.models.torch.torch_distributions import TorchCategorical
from ray.rllib.examples._old_api_stack.models.mobilenet_v2_encoder import (
    MobileNetV2EncoderConfig,
    MOBILENET_INPUT_SHAPE,
)
from ray.rllib.core.models.configs import ActorCriticEncoderConfig


class MobileNetTorchPPORLModule(PPOTorchRLModule):
    """A PPORLModules with mobilenet v2 as an encoder.

    The idea behind this model is to demonstrate how we can bypass catalog to
    take full control over what models and action distribution are being built.
    In this example, we do this to modify an existing RLModule with a custom encoder.
    """

    def setup(self):
        mobilenet_v2_config = MobileNetV2EncoderConfig()
        # Since we want to use PPO, which is an actor-critic algorithm, we need to
        # use an ActorCriticEncoderConfig to wrap the base encoder config.
        actor_critic_encoder_config = ActorCriticEncoderConfig(
            base_encoder_config=mobilenet_v2_config
        )

        self.encoder = actor_critic_encoder_config.build(framework="torch")
        mobilenet_v2_output_dims = mobilenet_v2_config.output_dims

        pi_config = MLPHeadConfig(
            input_dims=mobilenet_v2_output_dims,
            output_layer_dim=2,
        )

        vf_config = MLPHeadConfig(
            input_dims=mobilenet_v2_output_dims, output_layer_dim=1
        )

        self.pi = pi_config.build(framework="torch")
        self.vf = vf_config.build(framework="torch")

        self.action_dist_cls = TorchCategorical


config = (
    PPOConfig()
    .api_stack(enable_rl_module_and_learner=True)
    .rl_module(
        rl_module_spec=SingleAgentRLModuleSpec(module_class=MobileNetTorchPPORLModule)
    )
    .environment(
        RandomEnv,
        env_config={
            "action_space": gym.spaces.Discrete(2),
            # Test a simple Image observation space.
            "observation_space": gym.spaces.Box(
                0.0,
                1.0,
                shape=MOBILENET_INPUT_SHAPE,
                dtype=np.float32,
            ),
        },
    )
    .env_runners(num_env_runners=0)
    # The following training settings make it so that a training iteration is very
    # quick. This is just for the sake of this example. PPO will not learn properly
    # with these settings!
    .training(train_batch_size=32, sgd_minibatch_size=16, num_sgd_iter=1)
)

config.build().train()
# __sphinx_doc_end__
