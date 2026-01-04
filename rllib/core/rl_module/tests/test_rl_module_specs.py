import unittest

import gymnasium as gym
import numpy as np

from ray.rllib.core.rl_module.multi_rl_module import (
    MultiRLModule,
    MultiRLModuleSpec,
)
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.rl_modules.classes.vpg_torch_rlm import VPGTorchRLModule
from ray.rllib.examples.rl_modules.classes.vpg_using_shared_encoder_rlm import (
    SHARED_ENCODER_ID,
    SharedEncoder,
    VPGMultiRLModuleWithSharedEncoder,
    VPGPolicyAfterSharedEncoder,
)


class TestRLModuleSpecs(unittest.TestCase):
    def test_single_agent_spec(self):
        """Tests RLlib's default RLModuleSpec."""
        env = gym.make("CartPole-v1")
        spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config={"hidden_dim": 64},
        )

        module = spec.build()
        self.assertIsInstance(module, VPGTorchRLModule)

    def test_multi_agent_spec(self):
        env = gym.make("CartPole-v1")
        num_agents = 2
        module_specs = {}
        for i in range(num_agents):
            module_specs[f"module_{i}"] = RLModuleSpec(
                module_class=VPGTorchRLModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config={"hidden_dim": 32 * (i + 1)},
            )

        spec = MultiRLModuleSpec(rl_module_specs=module_specs)
        module = spec.build()
        self.assertIsInstance(module, MultiRLModule)

    def test_customized_multi_agent_module(self):
        """Tests creating a customized MARL BC module that owns a shared encoder."""

        EMBEDDING_DIM = 10
        HIDDEN_DIM = 20
        obs_space = gym.spaces.Box(-1.0, 1.0, shape=(4,), dtype=np.float32)
        act_space = gym.spaces.Discrete(2)

        spec = MultiRLModuleSpec(
            multi_rl_module_class=VPGMultiRLModuleWithSharedEncoder,
            rl_module_specs={
                SHARED_ENCODER_ID: RLModuleSpec(
                    module_class=SharedEncoder,
                    observation_space=obs_space,
                    action_space=act_space,
                    model_config={"embedding_dim": EMBEDDING_DIM},
                ),
                "agent_1": RLModuleSpec(
                    module_class=VPGPolicyAfterSharedEncoder,
                    observation_space=obs_space,
                    action_space=act_space,
                    model_config={
                        "embedding_dim": EMBEDDING_DIM,
                        "hidden_dim": HIDDEN_DIM,
                    },
                ),
                "agent_2": RLModuleSpec(
                    module_class=VPGPolicyAfterSharedEncoder,
                    observation_space=obs_space,
                    action_space=act_space,
                    model_config={
                        "embedding_dim": EMBEDDING_DIM,
                        "hidden_dim": HIDDEN_DIM,
                    },
                ),
            },
        )
        # Make sure we can build the MultiRLModule.
        spec.build()

    def test_get_spec_from_module_multi_agent(self):
        """Tests whether MultiRLModuleSpec.from_module() works."""
        env = gym.make("CartPole-v1")
        num_agents = 2
        module_specs = {}
        for i in range(num_agents):
            module_specs[f"module_{i}"] = RLModuleSpec(
                module_class=VPGTorchRLModule,
                observation_space=env.observation_space,
                action_space=env.action_space,
                model_config={"hidden_dim": 32 * (i + 1)},
            )

        spec = MultiRLModuleSpec(rl_module_specs=module_specs)
        module = spec.build()

        spec_from_module = MultiRLModuleSpec.from_module(module)
        self.assertEqual(spec, spec_from_module)

    def test_get_spec_from_module_single_agent(self):
        """Tests whether RLModuleSpec.from_module() works."""
        env = gym.make("CartPole-v1")
        spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config={"hidden_dim": 32},
        )

        module = spec.build()
        spec_from_module = RLModuleSpec.from_module(module)
        self.assertEqual(spec, spec_from_module)

    def test_update_specs(self):
        """Tests whether RLModuleSpec.update() works."""
        env = gym.make("CartPole-v0")

        # Test if RLModuleSpec.update() works.
        module_spec_1 = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config="Update me!",
        )
        module_spec_2 = RLModuleSpec(model_config={"hidden_dim": 32})
        self.assertEqual(module_spec_1.model_config, "Update me!")
        module_spec_1.update(module_spec_2)
        self.assertEqual(module_spec_1.model_config, {"hidden_dim": 32})

    def test_update_specs_multi_agent(self):
        """Test if updating a RLModuleSpec in MultiRLModuleSpec works.

        This tests if we can update a `model_config` field through different
        kinds of updates:
            - Create a RLModuleSpec and update its model_config.
            - Create two MultiRLModuleSpecs and update the first one with the
                second one without overwriting it.
            - Check if the updated MultiRLModuleSpec does not(!) have the
                updated model_config.
            - Create two MultiRLModuleSpecs and update the first one with the
                second one with overwriting it.
            - Check if the updated MultiRLModuleSpec has(!) the updated
                model_config.

        """
        env = gym.make("CartPole-v0")

        # Test if RLModuleSpec.update() works.
        module_spec_1 = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space="Do not update me!",
            action_space=env.action_space,
            model_config="Update me!",
        )
        module_spec_2 = RLModuleSpec(
            model_config={"hidden_dim": 32},
        )

        self.assertEqual(module_spec_1.model_config, "Update me!")
        module_spec_1.update(module_spec_2)
        self.assertEqual(module_spec_1.module_class, VPGTorchRLModule)
        self.assertEqual(module_spec_1.observation_space, "Do not update me!")
        self.assertEqual(module_spec_1.action_space, env.action_space)
        self.assertEqual(module_spec_1.model_config, module_spec_2.model_config)

        # Redefine module_spec_1 for following tests.
        module_spec_1 = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space="Do not update me!",
            action_space=env.action_space,
            model_config="Update me!",
        )

        marl_spec_1 = MultiRLModuleSpec(
            multi_rl_module_class=VPGMultiRLModuleWithSharedEncoder,
            rl_module_specs={"agent_1": module_spec_1},
        )
        marl_spec_2 = MultiRLModuleSpec(
            multi_rl_module_class=VPGMultiRLModuleWithSharedEncoder,
            rl_module_specs={"agent_1": module_spec_2},
        )

        # Test if updating MultiRLModuleSpec with overwriting works. This means
        # that the single agent specs should be overwritten
        self.assertEqual(
            marl_spec_1.rl_module_specs["agent_1"].model_config, "Update me!"
        )
        marl_spec_1.update(marl_spec_2, override=True)
        self.assertEqual(marl_spec_1.rl_module_specs["agent_1"], module_spec_2)

        # Test if updating MultiRLModuleSpec without overwriting works. This
        # means that the single agent specs should not be overwritten
        marl_spec_3 = MultiRLModuleSpec(
            multi_rl_module_class=VPGMultiRLModuleWithSharedEncoder,
            rl_module_specs={"agent_1": module_spec_1},
        )

        self.assertEqual(
            marl_spec_3.rl_module_specs["agent_1"].observation_space,
            "Do not update me!",
        )
        marl_spec_3.update(marl_spec_2, override=False)
        # If we would overwrite, we would replace the observation space even though
        # it was None. This is not the case here.
        self.assertEqual(
            marl_spec_3.rl_module_specs["agent_1"].observation_space,
            "Do not update me!",
        )

        # Test if updating with an additional RLModuleSpec works.
        module_spec_3 = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config="I'm new!",
        )
        marl_spec_3 = MultiRLModuleSpec(
            multi_rl_module_class=VPGMultiRLModuleWithSharedEncoder,
            rl_module_specs={"agent_2": module_spec_3},
        )
        self.assertEqual(marl_spec_1.rl_module_specs.get("agent_2"), None)
        marl_spec_1.update(marl_spec_3)
        self.assertEqual(
            marl_spec_1.rl_module_specs["agent_2"].model_config, "I'm new!"
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
