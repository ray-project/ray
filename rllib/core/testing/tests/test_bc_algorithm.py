import unittest

import gymnasium as gym
import ray
from ray.rllib.core.testing.torch.bc_module import (
    DiscreteBCTorchModule,
    BCTorchRLModuleWithSharedGlobalEncoder,
    BCTorchMultiAgentModuleWithSharedEncoder,
)
from ray.rllib.core.testing.tf.bc_module import (
    DiscreteBCTFModule,
    BCTfRLModuleWithSharedGlobalEncoder,
    BCTfMultiAgentModuleWithSharedEncoder,
)
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.testing.bc_algorithm import BCConfigTest
from ray.rllib.utils.test_utils import framework_iterator
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole


class TestLearner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_bc_algorithm(self):
        """Tests the Test BC algorithm in single -agent case."""

        config = (
            BCConfigTest()
            .api_stack(enable_rl_module_and_learner=True)
            .training(model={"fcnet_hiddens": [32, 32]})
        )

        # TODO (Kourosh): Add tf2 support
        for fw in framework_iterator(config, frameworks=("torch")):
            algo = config.build(env="CartPole-v1")
            policy = algo.get_policy()
            rl_module = policy.model

            if fw == "torch":
                assert isinstance(rl_module, DiscreteBCTorchModule)
            elif fw == "tf2":
                assert isinstance(rl_module, DiscreteBCTFModule)

    def test_bc_algorithm_marl(self):
        """Tests simple extension of single-agent to independent multi-agent case."""

        policies = {"policy_1", "policy_2"}
        config = (
            BCConfigTest()
            .api_stack(enable_rl_module_and_learner=True)
            .training(model={"fcnet_hiddens": [32, 32]})
            .multi_agent(
                policies=policies,
                policy_mapping_fn=lambda agent_id, **kwargs: list(policies)[agent_id],
            )
            .environment(MultiAgentCartPole, env_config={"num_agents": 2})
        )

        # TODO (Kourosh): Add tf2 support
        for fw in framework_iterator(config, frameworks=("torch")):
            algo = config.build()
            for policy_id in policies:
                policy = algo.get_policy(policy_id=policy_id)
                rl_module = policy.model

                if fw == "torch":
                    assert isinstance(rl_module, DiscreteBCTorchModule)
                elif fw == "tf2":
                    assert isinstance(rl_module, DiscreteBCTFModule)

    def test_bc_algorithm_w_custom_marl_module(self):
        """Tests the independent multi-agent case with shared encoders."""

        policies = {"policy_1", "policy_2"}

        for fw in ["torch"]:
            if fw == "torch":
                spec = MultiAgentRLModuleSpec(
                    marl_module_class=BCTorchMultiAgentModuleWithSharedEncoder,
                    module_specs=SingleAgentRLModuleSpec(
                        module_class=BCTorchRLModuleWithSharedGlobalEncoder
                    ),
                )
            else:
                spec = MultiAgentRLModuleSpec(
                    marl_module_class=BCTfMultiAgentModuleWithSharedEncoder,
                    module_specs=SingleAgentRLModuleSpec(
                        module_class=BCTfRLModuleWithSharedGlobalEncoder
                    ),
                )

            config = (
                BCConfigTest()
                .api_stack(enable_rl_module_and_learner=True)
                .framework(fw)
                .rl_module(rl_module_spec=spec)
                .training(
                    model={"fcnet_hiddens": [32, 32]},
                )
                .multi_agent(
                    policies=policies,
                    policy_mapping_fn=lambda agent_id, **kwargs: list(policies)[
                        agent_id
                    ],
                )
                # an env a weird nested obs that requires a custom model with shared
                # encoders
                .environment(
                    observation_space=gym.spaces.Dict(
                        {
                            "global": gym.spaces.Box(low=-1, high=1, shape=(10,)),
                            "local": gym.spaces.Box(low=-1, high=1, shape=(20,)),
                        }
                    ),
                    action_space=gym.spaces.Discrete(2),
                )
                .experimental(_disable_preprocessor_api=True)
            )
            algo = config.build()
            for policy_id in policies:
                policy = algo.get_policy(policy_id=policy_id)
                rl_module = policy.model

                if fw == "torch":
                    assert isinstance(rl_module, BCTorchRLModuleWithSharedGlobalEncoder)
                elif fw == "tf2":
                    assert isinstance(rl_module, BCTfRLModuleWithSharedGlobalEncoder)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
