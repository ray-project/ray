import tempfile
import unittest

from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec, RLModuleConfig
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleConfig,
)
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.utils import DEFAULT_POLICY_ID
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import check


class TestMARLModule(unittest.TestCase):
    ENV = MultiAgentCartPole({"num_agents": 2})

    def test_from_config(self):
        """Tests whether a MultiAgentRLModule can be constructed from a config."""
        module1 = SingleAgentRLModuleSpec(
            module_class=DiscreteBCTorchModule,
            observation_space=self.ENV.observation_space[0],
            action_space=self.ENV.action_space[0],
            model_config_dict={"fcnet_hiddens": [32]},
        )

        module2 = SingleAgentRLModuleSpec(
            module_class=DiscreteBCTorchModule,
            observation_space=self.ENV.observation_space[0],
            action_space=self.ENV.action_space[0],
            model_config_dict={"fcnet_hiddens": [32]},
        )

        config = MultiAgentRLModuleConfig(
            modules={"module1": module1, "module2": module2}
        )
        marl_module = MultiAgentRLModule(config)

        self.assertEqual(set(marl_module.keys()), {"module1", "module2"})
        self.assertIsInstance(marl_module["module1"], DiscreteBCTorchModule)
        self.assertIsInstance(marl_module["module2"], DiscreteBCTorchModule)

    def test_as_multi_agent(self):

        marl_module = DiscreteBCTorchModule(
            config=RLModuleConfig(
                self.ENV.observation_space[0],
                self.ENV.action_space[0],
                model_config_dict={"fcnet_hiddens": [32]},
            )
        ).as_multi_agent()

        self.assertNotIsInstance(marl_module, DiscreteBCTorchModule)
        self.assertIsInstance(marl_module, MultiAgentRLModule)
        self.assertEqual({DEFAULT_POLICY_ID}, set(marl_module.keys()))

        # check as_multi_agent() for the second time
        marl_module2 = marl_module.as_multi_agent()
        self.assertEqual(id(marl_module), id(marl_module2))

    def test_get_set_state(self):

        module = DiscreteBCTorchModule(
            config=RLModuleConfig(
                self.ENV.observation_space[0],
                self.ENV.action_space[0],
                model_config_dict={"fcnet_hiddens": [32]},
            )
        ).as_multi_agent()

        state = module.get_state()
        self.assertIsInstance(state, dict)
        self.assertEqual(set(state.keys()), set(module.keys()))
        self.assertEqual(
            set(state[DEFAULT_POLICY_ID].keys()),
            set(module[DEFAULT_POLICY_ID].get_state().keys()),
        )

        module2 = DiscreteBCTorchModule(
            config=RLModuleConfig(
                self.ENV.observation_space[0],
                self.ENV.action_space[0],
                model_config_dict={"fcnet_hiddens": [32]},
            )
        ).as_multi_agent()
        state2 = module2.get_state()
        check(state, state2, false=True)

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)

    def test_add_remove_modules(self):
        # TODO (Avnish): Modify this test to make sure that the distributed
        # functionality won't break the add / remove.

        module = DiscreteBCTorchModule(
            config=RLModuleConfig(
                self.ENV.observation_space[0],
                self.ENV.action_space[0],
                model_config_dict={"fcnet_hiddens": [32]},
            )
        ).as_multi_agent()

        module.add_module(
            "test",
            DiscreteBCTorchModule(
                config=RLModuleConfig(
                    self.ENV.observation_space[0],
                    self.ENV.action_space[0],
                    model_config_dict={"fcnet_hiddens": [32]},
                )
            ),
        )
        self.assertEqual(set(module.keys()), {DEFAULT_POLICY_ID, "test"})
        module.remove_module("test")
        self.assertEqual(set(module.keys()), {DEFAULT_POLICY_ID})

        # test if add works with a conflicting name
        self.assertRaises(
            ValueError,
            lambda: module.add_module(
                DEFAULT_POLICY_ID,
                DiscreteBCTorchModule(
                    config=RLModuleConfig(
                        self.ENV.observation_space[0],
                        self.ENV.action_space[0],
                        model_config_dict={"fcnet_hiddens": [32]},
                    )
                ),
            ),
        )

        module.add_module(
            DEFAULT_POLICY_ID,
            DiscreteBCTorchModule(
                config=RLModuleConfig(
                    self.ENV.observation_space[0],
                    self.ENV.action_space[0],
                    model_config_dict={"fcnet_hiddens": [32]},
                )
            ),
            override=True,
        )

    def test_save_to_from_checkpoint(self):
        """Test saving and loading from checkpoint after adding / removing modules."""
        module = DiscreteBCTorchModule(
            config=RLModuleConfig(
                self.ENV.observation_space[0],
                self.ENV.action_space[0],
                model_config_dict={"fcnet_hiddens": [32]},
            )
        ).as_multi_agent()

        module.add_module(
            "test",
            DiscreteBCTorchModule(
                config=RLModuleConfig(
                    self.ENV.observation_space[0],
                    self.ENV.action_space[0],
                    model_config_dict={"fcnet_hiddens": [32]},
                )
            ),
        )
        module.add_module(
            "test2",
            DiscreteBCTorchModule(
                config=RLModuleConfig(
                    self.ENV.observation_space[0],
                    self.ENV.action_space[0],
                    model_config_dict={"fcnet_hiddens": [128]},
                )
            ),
        )

        # Check, whether saving and restoring from checkpoint works as expected.
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint = module.save(path=tmpdir)
            module2 = MultiAgentRLModule.from_checkpoint(checkpoint)
            check(module.get_state(), module2.get_state())
            self.assertEqual(module.keys(), module2.keys())
            self.assertEqual(module.keys(), {"test", "test2", DEFAULT_POLICY_ID})
            self.assertNotEqual(id(module), id(module2))

        # Check, whether saving/restoring partial MultiAgentRLModules works as expected.
        with tempfile.TemporaryDirectory() as tmpdir:
            # Only save "test".
            checkpoint = module.save(path=tmpdir, module_ids="test")
            module2 = MultiAgentRLModule.from_checkpoint(checkpoint)
            check(module.get_state()["test"], module2.get_state()["test"])
            self.assertTrue("test2" not in module2 and DEFAULT_POLICY_ID not in module2)
            self.assertNotEqual(module.keys(), module2.keys())
            self.assertEqual(module.keys(), {"test", "test2", DEFAULT_POLICY_ID})
            self.assertEqual(module2.keys(), {"test"})
            self.assertNotEqual(id(module), id(module2))
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save test and test2 ...
            checkpoint = module.save(path=tmpdir, module_ids={"test", "test2"})
            # But only load "test".
            module2 = MultiAgentRLModule.from_checkpoint(checkpoint, module_ids="test")
            check(module.get_state()["test"], module2.get_state()["test"])
            self.assertTrue("test2" not in module2 and DEFAULT_POLICY_ID not in module2)
            self.assertNotEqual(module.keys(), module2.keys())
            self.assertEqual(module.keys(), {"test", "test2", DEFAULT_POLICY_ID})
            self.assertEqual(module2.keys(), {"test"})
            self.assertNotEqual(id(module), id(module2))
            # Test loading a non-existent module ID, which should only produce a
            # warning.
            MultiAgentRLModule.from_checkpoint(
                checkpoint,
                module_ids=["test", "blabla_xyz"],
            )

        # Check that after removing a module, the checkpoint is correct.
        module.remove_module("test")
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint = module.save(path=tmpdir)
            module2 = MultiAgentRLModule.from_checkpoint(checkpoint)
            check(module.get_state(), module2.get_state())
            self.assertEqual(module.keys(), module2.keys())
            self.assertEqual(module.keys(), {"test2", DEFAULT_POLICY_ID})
            self.assertNotEqual(id(module), id(module2))

        # Check that after adding a new module, the checkpoint is correct.
        module.add_module(
            "test3",
            DiscreteBCTorchModule(
                config=RLModuleConfig(
                    self.ENV.observation_space[0],
                    self.ENV.action_space[0],
                    model_config_dict={"fcnet_hiddens": [120]},
                )
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = "/tmp/test_marl_module"
            checkpoint = module.save(path=tmpdir)
            module2 = MultiAgentRLModule.from_checkpoint(checkpoint)
            check(module.get_state(), module2.get_state())
            self.assertEqual(module.keys(), module2.keys())
            self.assertEqual(module.keys(), {"test2", "test3", DEFAULT_POLICY_ID})
            self.assertNotEqual(id(module), id(module2))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
