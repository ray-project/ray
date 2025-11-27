import tempfile
import unittest

import gymnasium as gym

from ray.rllib.algorithms import DQNConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule, MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.multi_agent.multi_agent_cartpole import MultiAgentCartPole
from ray.rllib.examples.rl_modules.classes.vpg_torch_rlm import VPGTorchRLModule
from ray.rllib.utils.test_utils import check


class TestMultiRLModule(unittest.TestCase):
    def test_from_config(self):
        """Tests whether a MultiRLModule can be constructed from a config."""
        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})
        module1 = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=env.get_observation_space(0),
            action_space=env.get_action_space(0),
            model_config={"hidden_dim": 32},
        )

        module2 = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=env.get_observation_space(0),
            action_space=env.get_action_space(0),
            model_config={"hidden_dim": 32},
        )

        multi_rl_module = MultiRLModule(
            rl_module_specs={"module1": module1, "module2": module2},
        )

        self.assertEqual(set(multi_rl_module.keys()), {"module1", "module2"})
        self.assertIsInstance(multi_rl_module["module1"], VPGTorchRLModule)
        self.assertIsInstance(multi_rl_module["module2"], VPGTorchRLModule)

    def test_as_multi_rl_module(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        multi_rl_module = VPGTorchRLModule(
            observation_space=env.get_observation_space(0),
            action_space=env.get_action_space(0),
            model_config={"hidden_dim": 32},
        ).as_multi_rl_module()

        self.assertNotIsInstance(multi_rl_module, VPGTorchRLModule)
        self.assertIsInstance(multi_rl_module, MultiRLModule)
        self.assertEqual({DEFAULT_MODULE_ID}, set(multi_rl_module.keys()))

        # Check as_multi_rl_module() for the second time
        multi_rl_module2 = multi_rl_module.as_multi_rl_module()
        self.assertEqual(id(multi_rl_module), id(multi_rl_module2))

    def test_get_state_and_set_state(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        module = VPGTorchRLModule(
            observation_space=env.get_observation_space(0),
            action_space=env.get_action_space(0),
            model_config={"hidden_dim": 32},
        ).as_multi_rl_module()

        state = module.get_state()
        self.assertIsInstance(state, dict)
        self.assertEqual(
            set(state.keys()),
            set(module.keys()),
        )
        self.assertEqual(
            set(state[DEFAULT_MODULE_ID].keys()),
            set(module[DEFAULT_MODULE_ID].get_state().keys()),
        )

        module2 = VPGTorchRLModule(
            observation_space=env.get_observation_space(0),
            action_space=env.get_action_space(0),
            model_config={"hidden_dim": 32},
        ).as_multi_rl_module()
        state2 = module2.get_state()
        check(state[DEFAULT_MODULE_ID], state2[DEFAULT_MODULE_ID], false=True)

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)

    def test_add_remove_modules(self):
        # TODO (Avnish): Modify this test to make sure that the distributed
        # functionality won't break the add / remove.

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})
        module = VPGTorchRLModule(
            observation_space=env.get_observation_space(0),
            action_space=env.get_action_space(0),
            model_config={"hidden_dim": 32},
        ).as_multi_rl_module()

        module.add_module(
            "test",
            VPGTorchRLModule(
                observation_space=env.get_observation_space(0),
                action_space=env.get_action_space(0),
                model_config={"hidden_dim": 32},
            ),
        )
        self.assertEqual(set(module.keys()), {DEFAULT_MODULE_ID, "test"})
        module.remove_module("test")
        self.assertEqual(set(module.keys()), {DEFAULT_MODULE_ID})

        # test if add works with a conflicting name
        self.assertRaises(
            ValueError,
            lambda: module.add_module(
                DEFAULT_MODULE_ID,
                VPGTorchRLModule(
                    observation_space=env.get_observation_space(0),
                    action_space=env.get_action_space(0),
                    model_config={"hidden_dim": 32},
                ),
            ),
        )

        module.add_module(
            DEFAULT_MODULE_ID,
            VPGTorchRLModule(
                observation_space=env.get_observation_space(0),
                action_space=env.get_action_space(0),
                model_config={"hidden_dim": 32},
            ),
            override=True,
        )

    def test_save_to_path_and_from_checkpoint(self):
        """Test saving and loading from checkpoint after adding / removing modules."""
        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})
        module = VPGTorchRLModule(
            observation_space=env.get_observation_space(0),
            action_space=env.get_action_space(0),
            model_config={"hidden_dim": 32},
        ).as_multi_rl_module()

        module.add_module(
            "test",
            VPGTorchRLModule(
                observation_space=env.get_observation_space(0),
                action_space=env.get_action_space(0),
                model_config={"hidden_dim": 32},
            ),
        )
        module.add_module(
            "test2",
            VPGTorchRLModule(
                observation_space=env.get_observation_space(0),
                action_space=env.get_action_space(0),
                model_config={"hidden_dim": 128},
            ),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            module.save_to_path(tmpdir)
            module2 = MultiRLModule.from_checkpoint(tmpdir)
            check(module.get_state(), module2.get_state())
            self.assertEqual(module.keys(), module2.keys())
            self.assertEqual(module.keys(), {"test", "test2", DEFAULT_MODULE_ID})
            self.assertNotEqual(id(module), id(module2))

        module.remove_module("test")

        # Check that - after removing a module - the checkpoint is correct.
        with tempfile.TemporaryDirectory() as tmpdir:
            module.save_to_path(tmpdir)
            module2 = MultiRLModule.from_checkpoint(tmpdir)
            check(module.get_state(), module2.get_state())
            self.assertEqual(module.keys(), module2.keys())
            self.assertEqual(module.keys(), {"test2", DEFAULT_MODULE_ID})
            self.assertNotEqual(id(module), id(module2))

        # Check that - after adding a new module - the checkpoint is correct.
        module.add_module(
            "test3",
            VPGTorchRLModule(
                observation_space=env.get_observation_space(0),
                action_space=env.get_action_space(0),
                model_config={"hidden_dim": 120},
            ),
        )
        # Check that - after adding a module - the checkpoint is correct.
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = "/tmp/test_multi_rl_module"
            module.save_to_path(tmpdir)
            module2 = MultiRLModule.from_checkpoint(tmpdir)
            check(module.get_state(), module2.get_state())
            self.assertEqual(module.keys(), module2.keys())
            self.assertEqual(module.keys(), {"test2", "test3", DEFAULT_MODULE_ID})
            self.assertNotEqual(id(module), id(module2))

    def test_model_config_propagation(self):
        """Test that model_config is correctly added to a MultiRLModule"""

        class CustomMultiRLModule(MultiRLModule):
            def setup(self):
                super().setup()
                assert self.model_config is not None

        spec = MultiRLModuleSpec(
            multi_rl_module_class=CustomMultiRLModule,
            rl_module_specs={
                "agent_1": RLModuleSpec(
                    TorchRLModule,
                    observation_space=gym.spaces.Box(0, 1),
                    action_space=gym.spaces.Box(0, 1),
                )
            },
            model_config={"some_config": 1},
        )
        # Verify that model_config propagates when instantiated using MultiRLModuleSpec.build()
        spec.build()
        # Verify that model_config propagates when instantiated using an AlgorithmConfig
        algo_config = (
            DQNConfig()
            .environment(MultiAgentCartPole)
            .rl_module(rl_module_spec=spec)
            .multi_agent(
                policies={"agent_1"},
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: agent_id,
            )
        )
        MultiAgentEnvRunner(algo_config)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
