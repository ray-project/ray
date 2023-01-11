import unittest


from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule, _get_module_configs
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.utils.test_utils import check

DEFAULT_POLICY_ID = "default_policy"


class TestMARLModule(unittest.TestCase):
    def test_from_config(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})
        module1 = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )
        module2 = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        multi_agent_dict = {"module1": module1, "module2": module2}
        marl_module = MultiAgentRLModule(multi_agent_dict)

        self.assertEqual(set(marl_module.keys()), {"module1", "module2"})
        self.assertIsInstance(marl_module["module1"], DiscreteBCTorchModule)
        self.assertIsInstance(marl_module["module2"], DiscreteBCTorchModule)

    def test_from_multi_agent_config(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        multi_agent_dict = {
            "modules": {
                "module1": {
                    "module_class": DiscreteBCTorchModule,
                    "model_config": {"hidden_dim": 64},
                },
                "module2": {
                    "module_class": DiscreteBCTorchModule,
                    "model_config": {"hidden_dim": 32},
                },
            },
            "observation_space": env.observation_space,  # this is common
            "action_space": env.action_space,  # this is common
        }

        marl_module = MultiAgentRLModule.from_multi_agent_config(multi_agent_dict)

        self.assertEqual(set(marl_module.keys()), {"module1", "module2"})
        self.assertIsInstance(marl_module["module1"], DiscreteBCTorchModule)
        self.assertIsInstance(marl_module["module2"], DiscreteBCTorchModule)

    def test_as_multi_agent(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        marl_module = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        ).as_multi_agent()

        self.assertNotIsInstance(marl_module, DiscreteBCTorchModule)
        self.assertIsInstance(marl_module, MultiAgentRLModule)
        self.assertEqual({DEFAULT_POLICY_ID}, set(marl_module.keys()))

        # check as_multi_agent() for the second time
        marl_module2 = marl_module.as_multi_agent()
        self.assertEqual(id(marl_module), id(marl_module2))

    def test_get_set_state(self):

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})

        module = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        ).as_multi_agent()

        state = module.get_state()
        self.assertIsInstance(state, dict)
        self.assertEqual(set(state.keys()), set(module.keys()))
        self.assertEqual(
            set(state[DEFAULT_POLICY_ID].keys()),
            set(module[DEFAULT_POLICY_ID].get_state().keys()),
        )

        module2 = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        ).as_multi_agent()
        state2 = module2.get_state()
        check(state, state2, false=True)

        module2.set_state(state)
        state2_after = module2.get_state()
        check(state, state2_after)

    def test_add_remove_modules(self):
        # TODO (Avnish): Modify this test to make sure that the distributed
        # functionality won't break the add / remove.

        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})
        module = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        ).as_multi_agent()

        module.add_module(
            "test",
            DiscreteBCTorchModule.from_model_config(
                env.observation_space,
                env.action_space,
                model_config={"hidden_dim": 32},
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
                DiscreteBCTorchModule.from_model_config(
                    env.observation_space,
                    env.action_space,
                    model_config={"hidden_dim": 32},
                ),
            ),
        )

        module.add_module(
            DEFAULT_POLICY_ID,
            DiscreteBCTorchModule.from_model_config(
                env.observation_space,
                env.action_space,
                model_config={"hidden_dim": 32},
            ),
            override=True,
        )

    def test_get_module_configs(self):
        """Tests the method for getting the module configs from multi-agent config."""

        config = {
            "modules": {
                "1": {"module_class": "foo", "model_config": "bar"},
                "2": {"module_class": "foo2", "model_config": "bar2"},
            },
            "observation_space": "obs_space",
            "action_space": "action_space",
        }

        expected_config = {
            "1": {
                "module_class": "foo",
                "model_config": "bar",
                "observation_space": "obs_space",
                "action_space": "action_space",
            },
            "2": {
                "module_class": "foo2",
                "model_config": "bar2",
                "observation_space": "obs_space",
                "action_space": "action_space",
            },
        }

        self.assertDictEqual(_get_module_configs(config), expected_config)

        config = {
            "modules": {
                "1": {
                    "module_class": "foo",
                    "model_config": "bar",
                    "observation_space": "obs_space1",  # won't get overwritten
                    "action_space": "action_space1",  # won't get overwritten
                },
                "2": {"module_class": "foo2", "model_config": "bar2"},
            },
            "observation_space": "obs_space",
            "action_space": "action_space",
        }

        expected_config = {
            "1": {
                "module_class": "foo",
                "model_config": "bar",
                "observation_space": "obs_space1",
                "action_space": "action_space1",
            },
            "2": {
                "module_class": "foo2",
                "model_config": "bar2",
                "observation_space": "obs_space",
                "action_space": "action_space",
            },
        }

        self.assertDictEqual(_get_module_configs(config), expected_config)

    def test_serialize_deserialize(self):
        env_class = make_multi_agent("CartPole-v0")
        env = env_class({"num_agents": 2})
        module1 = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )
        module2 = DiscreteBCTorchModule.from_model_config(
            env.observation_space,
            env.action_space,
            model_config={"hidden_dim": 32},
        )

        multi_agent_dict = {"module1": module1, "module2": module2}
        marl_module = MultiAgentRLModule(multi_agent_dict)
        new_marl_module = marl_module.deserialize(marl_module.serialize())

        self.assertNotEqual(id(marl_module), id(new_marl_module))
        self.assertEqual(set(marl_module.keys()), set(new_marl_module.keys()))
        for key in marl_module.keys():
            self.assertNotEqual(id(marl_module[key]), id(new_marl_module[key]))
            check(marl_module[key].get_state(), new_marl_module[key].get_state())


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
