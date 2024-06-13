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
    BCTorchMultiAgentModuleWithSharedEncoder,
)
from ray.rllib.core.testing.tf.bc_module import (
    DiscreteBCTFModule,
    BCTfRLModuleWithSharedGlobalEncoder,
    BCTfMultiAgentModuleWithSharedEncoder,
)

MODULES = [DiscreteBCTorchModule, DiscreteBCTFModule]
CUSTOM_MODULES = {
    "torch": BCTorchRLModuleWithSharedGlobalEncoder,
    "tf2": BCTfRLModuleWithSharedGlobalEncoder,
}
CUSTOM_MARL_MODULES = {
    "torch": BCTorchMultiAgentModuleWithSharedEncoder,
    "tf2": BCTfMultiAgentModuleWithSharedEncoder,
}


class TestRLModuleSpecs(unittest.TestCase):
    def test_single_agent_spec(self):
        """Tests RLlib's default SingleAgentRLModuleSpec."""
        env = gym.make("CartPole-v1")
        for module_class in MODULES:
            spec = SingleAgentRLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [64]},
            )

            module = spec.build()
            self.assertIsInstance(module, module_class)

    def test_multi_agent_spec(self):
        env = gym.make("CartPole-v1")
        num_agents = 2
        for module_class in MODULES:
            module_specs = {}
            for i in range(num_agents):
                module_specs[f"module_{i}"] = SingleAgentRLModuleSpec(
                    module_class=module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict={"fcnet_hiddens": [32 * (i + 1)]},
                )

            spec = MultiAgentRLModuleSpec(module_specs=module_specs)
            module = spec.build()
            self.assertIsInstance(module, MultiAgentRLModule)

    def test_customized_multi_agent_module(self):
        """Tests creating a customized MARL BC module that owns a shared encoder."""

        global_dim = 10
        local_dims = [16, 32]
        action_dims = [2, 4]

        # TODO (Kourosh): add tf support
        for fw in ["torch"]:
            marl_module_cls = CUSTOM_MARL_MODULES[fw]
            rl_module_cls = CUSTOM_MODULES[fw]

            spec = MultiAgentRLModuleSpec(
                marl_module_class=marl_module_cls,
                module_specs={
                    "agent_1": SingleAgentRLModuleSpec(
                        module_class=rl_module_cls,
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
                        model_config_dict={"fcnet_hiddens": [128]},
                    ),
                    "agent_2": SingleAgentRLModuleSpec(
                        module_class=rl_module_cls,
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
                        model_config_dict={"fcnet_hiddens": [128]},
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

    def test_get_spec_from_module_multi_agent(self):
        """Tests wether MultiAgentRLModuleSpec.from_module() works."""
        env = gym.make("CartPole-v1")
        num_agents = 2
        for module_class in MODULES:
            module_specs = {}
            for i in range(num_agents):
                module_specs[f"module_{i}"] = SingleAgentRLModuleSpec(
                    module_class=module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict={"fcnet_hiddens": [32 * (i + 1)]},
                )

            spec = MultiAgentRLModuleSpec(module_specs=module_specs)
            module = spec.build()

            spec_from_module = MultiAgentRLModuleSpec.from_module(module)
            self.assertEqual(spec, spec_from_module)

    def test_get_spec_from_module_single_agent(self):
        """Tests wether SingleAgentRLModuleSpec.from_module() works."""
        env = gym.make("CartPole-v1")
        for module_class in MODULES:
            spec = SingleAgentRLModuleSpec(
                module_class=module_class,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config_dict={"fcnet_hiddens": [32]},
            )

            module = spec.build()
            spec_from_module = SingleAgentRLModuleSpec.from_module(module)
            self.assertEqual(spec, spec_from_module)

    def test_update_specs(self):
        """Tests wether SingleAgentRLModuleSpec.update() works."""
        env = gym.make("CartPole-v0")

        # Test if SingleAgentRLModuleSpec.update() works.
        module_spec_1 = SingleAgentRLModuleSpec(
            module_class=DiscreteBCTorchModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config_dict="Update me!",
        )
        module_spec_2 = SingleAgentRLModuleSpec(
            model_config_dict={"fcnet_hiddens": [32]}
        )
        self.assertEqual(module_spec_1.model_config_dict, "Update me!")
        module_spec_1.update(module_spec_2)
        self.assertEqual(module_spec_1.model_config_dict, {"fcnet_hiddens": [32]})

    def test_update_specs_multi_agent(self):
        """Test if updating a SingleAgentRLModuleSpec in MultiAgentRLModuleSpec works.

        This tests if we can update a `model_config_dict` field through different
        kinds of updates:
            - Create a SingleAgentRLModuleSpec and update its model_config_dict.
            - Create two MultiAgentRLModuleSpecs and update the first one with the
                second one without overwriting it.
            - Check if the updated MultiAgentRLModuleSpec does not(!) have the
                updated model_config_dict.
            - Create two MultiAgentRLModuleSpecs and update the first one with the
                second one with overwriting it.
            - Check if the updated MultiAgentRLModuleSpec has(!) the updated
                model_config_dict.

        """
        env = gym.make("CartPole-v0")

        # Test if SingleAgentRLModuleSpec.update() works.
        module_spec_1 = SingleAgentRLModuleSpec(
            module_class=DiscreteBCTorchModule,
            observation_space="Do not update me!",
            action_space=env.action_space,
            model_config_dict="Update me!",
        )
        module_spec_2 = SingleAgentRLModuleSpec(
            model_config_dict={"fcnet_hiddens": [32]},
        )

        self.assertEqual(module_spec_1.model_config_dict, "Update me!")
        module_spec_1.update(module_spec_2)
        self.assertEqual(module_spec_1.module_class, DiscreteBCTorchModule)
        self.assertEqual(module_spec_1.observation_space, "Do not update me!")
        self.assertEqual(module_spec_1.action_space, env.action_space)
        self.assertEqual(
            module_spec_1.model_config_dict, module_spec_2.model_config_dict
        )

        # Redefine module_spec_1 for following tests.
        module_spec_1 = SingleAgentRLModuleSpec(
            module_class=DiscreteBCTorchModule,
            observation_space="Do not update me!",
            action_space=env.action_space,
            model_config_dict="Update me!",
        )

        marl_spec_1 = MultiAgentRLModuleSpec(
            marl_module_class=BCTorchMultiAgentModuleWithSharedEncoder,
            module_specs={"agent_1": module_spec_1},
        )
        marl_spec_2 = MultiAgentRLModuleSpec(
            marl_module_class=BCTorchMultiAgentModuleWithSharedEncoder,
            module_specs={"agent_1": module_spec_2},
        )

        # Test if updating MultiAgentRLModuleSpec with overwriting works. This means
        # that the single agent specs should be overwritten
        self.assertEqual(
            marl_spec_1.module_specs["agent_1"].model_config_dict, "Update me!"
        )
        marl_spec_1.update(marl_spec_2, override=True)
        self.assertEqual(marl_spec_1.module_specs["agent_1"], module_spec_2)

        # Test if updating MultiAgentRLModuleSpec without overwriting works. This
        # means that the single agent specs should not be overwritten
        marl_spec_3 = MultiAgentRLModuleSpec(
            marl_module_class=BCTorchMultiAgentModuleWithSharedEncoder,
            module_specs={"agent_1": module_spec_1},
        )

        self.assertEqual(
            marl_spec_3.module_specs["agent_1"].observation_space, "Do not update me!"
        )
        marl_spec_3.update(marl_spec_2, override=False)
        # If we would overwrite, we would replace the observation space even though
        # it was None. This is not the case here.
        self.assertEqual(
            marl_spec_3.module_specs["agent_1"].observation_space, "Do not update me!"
        )

        # Test if updating with an additional SingleAgentRLModuleSpec works.
        module_spec_3 = SingleAgentRLModuleSpec(
            module_class=DiscreteBCTorchModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config_dict="I'm new!",
        )
        marl_spec_3 = MultiAgentRLModuleSpec(
            marl_module_class=BCTorchMultiAgentModuleWithSharedEncoder,
            module_specs={"agent_2": module_spec_3},
        )
        self.assertEqual(marl_spec_1.module_specs.get("agent_2"), None)
        marl_spec_1.update(marl_spec_3)
        self.assertEqual(
            marl_spec_1.module_specs["agent_2"].model_config_dict, "I'm new!"
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
