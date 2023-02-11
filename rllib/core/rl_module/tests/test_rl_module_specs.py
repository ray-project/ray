import unittest
import gymnasium as gym
import torch

from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.core.testing.torch.bc_module import (
    DiscreteBCTorchModule,
    BCTorchRLModuleWithSharedGlobalEncoder,
    BCTorchMultiAgentSpec,
)
from ray.rllib.core.testing.tf.bc_module import (
    DiscreteBCTFModule,
    BCTfRLModuleWithSharedGlobalEncoder,
    BCTfMultiAgentSpec,
)

MODULES = [DiscreteBCTorchModule, DiscreteBCTFModule]
CUSTOM_MODULES = {
    "torch": BCTorchRLModuleWithSharedGlobalEncoder,
    "tf": BCTfRLModuleWithSharedGlobalEncoder,
}
CUSTOM_MARL_SPECS = {"torch": BCTorchMultiAgentSpec, "tf": BCTfMultiAgentSpec}


class BCRLModuleSpecCustom(SingleAgentRLModuleSpec):
    """A customized SingleAgentRLModuleSpec."""

    def build(self):
        # this handles all implementation details
        config = {
            "input_dim": self.observation_space.shape[0],
            "hidden_dim": self.model_config["hidden_dim"],
            "output_dim": self.action_space.n,
        }
        return self.module_class(**config)


class TestRLModuleSpecs(unittest.TestCase):
    def test_single_agent_spec(self):
        """Tests RLlib's default SingleAgentRLModuleSpec."""
        env = gym.make("CartPole-v1")
        for module_class in MODULES:
            spec = SingleAgentRLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config={"hidden_dim": 64},
            )

            module = spec.build()
            self.assertIsInstance(module, module_class)

    def test_customized_single_agent_spec(self):
        """Tests the a customized SingleAgentRLModuleSpec."""
        env = gym.make("CartPole-v1")
        for module_class in MODULES:

            spec = BCRLModuleSpecCustom(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config={"hidden_dim": 64},
            )
            module = spec.build()
            self.assertIsInstance(module, module_class)

    def test_multi_agent_spec(self):
        env = gym.make("CartPole-v1")
        num_agents = 2
        # make sure I use both default and cutomized single agent specs
        single_agent_spec_classes = [SingleAgentRLModuleSpec, BCRLModuleSpecCustom]
        for module_class in MODULES:
            module_specs = {}
            for i in range(num_agents):
                module_spec_cls = single_agent_spec_classes[i % num_agents]
                module_specs[f"module_{i}"] = module_spec_cls(
                    module_class=module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config={"hidden_dim": 32 * (i + 1)},
                )

            spec = MultiAgentRLModuleSpec(
                module_class=MultiAgentRLModule, module_specs=module_specs
            )
            module = spec.build()
            self.assertIsInstance(module, MultiAgentRLModule)

    def test_customized_multi_agent_spec(self):
        """Tests creating a customized MARL BC module that owns a shared encoder."""

        global_dim = 10
        local_dims = [16, 32]
        action_dims = [2, 4]

        # TODO (Kourosh): add tf support
        for fw in ["torch"]:
            spec_cls = CUSTOM_MARL_SPECS[fw]
            module_cls = CUSTOM_MODULES[fw]

            spec = spec_cls(
                module_class=MultiAgentRLModule,
                module_specs={
                    "agent_1": SingleAgentRLModuleSpec(
                        module_class=module_cls,
                        observation_space=gym.spaces.Dict(
                            {
                                "global": gym.spaces.Box(
                                    low=-1, high=1, shape=(global_dim,)
                                ),
                                "local": gym.spaces.Box(
                                    low=-1, high=1, shape=(local_dims[0],)
                                ),
                            }
                        ),
                        action_space=gym.spaces.Discrete(action_dims[0]),
                        model_config={"hidden_dim": 128},
                    ),
                    "agent_2": SingleAgentRLModuleSpec(
                        module_class=module_cls,
                        observation_space=gym.spaces.Dict(
                            {
                                "global": gym.spaces.Box(
                                    low=-1, high=1, shape=(global_dim,)
                                ),
                                "local": gym.spaces.Box(
                                    low=-1, high=1, shape=(local_dims[1],)
                                ),
                            }
                        ),
                        action_space=gym.spaces.Discrete(action_dims[1]),
                        model_config={"hidden_dim": 128},
                    ),
                },
            )

            model = spec.build()

            if fw == "torch":
                # change the parameters of the shared encoder and make sure it changes
                # across all agents
                foo = model["agent_1"].encoder[0].bias
                foo.data = torch.ones_like(foo.data)
                self.assertTrue(torch.allclose(model["agent_2"].encoder[0].bias, foo))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
