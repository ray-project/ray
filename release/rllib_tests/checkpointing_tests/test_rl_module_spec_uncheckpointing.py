import gymnasium as gym
import tempfile
import unittest

import ray
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.utils.test_utils import check


MODULES = [DiscreteBCTorchModule, DiscreteBCTFModule]


class TestRLModuleSpecUncheckpointing(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def uncheckpoint_different_node(self):
        """Test if building a MultiAgentRLModule with heterogeneous checkpoints works.

        This tests if we can load some modules from single agent rl modules and the rest
        from a multi agent rl module checkpoint.

        """
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

            marl_spec = MultiAgentRLModuleSpec(module_specs=module_specs)
            marl_module = marl_spec.build()
            self.assertIsInstance(marl_module, MultiAgentRLModule)

            single_agent_rl_spec = module_specs["module_1"]
            single_agent_rl_module = single_agent_rl_spec.build()
            self.assertIsInstance(single_agent_rl_module, module_class)

            with tempfile.TemporaryDirectory() as single_agent_checkpoint_path:
                single_agent_rl_module.save_to_checkpoint(single_agent_checkpoint_path)

                spec_with_load_path = MultiAgentRLModuleSpec(
                    module_specs=module_specs,
                    load_state_path=single_agent_checkpoint_path,
                )
                with tempfile.TemporaryDirectory() as marl_checkpoint_path:
                    marl_module.save_to_checkpoint(marl_checkpoint_path)

                    module_specs["module_1"] = SingleAgentRLModuleSpec(
                        module_class=module_class,
                        observation_space=env.observation_space,
                        action_space=env.action_space,
                        model_config_dict={"fcnet_hiddens": [32 * (2)]},
                        load_state_path=single_agent_checkpoint_path,
                    )

                    spec_with_load_path = MultiAgentRLModuleSpec(
                        module_specs=module_specs,
                        load_state_path=marl_checkpoint_path,
                    )
                    module_from_checkpoint = spec_with_load_path.build()
                    # module 0 in the loaded marl_module should be the same as module_0
                    # in the original module
                    check(
                        marl_module.get_state()["module_0"],
                        module_from_checkpoint.get_state()["module_0"],
                    )
                    # module 1 in the loaded marl_module should have the same state as
                    # the single_agent_rl_module, not the original marl_module
                    check(
                        single_agent_rl_module.get_state(),
                        module_from_checkpoint.get_state()["module_1"],
                    )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
