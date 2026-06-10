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


class TestMultiRLModuleSpecSingleSpec(unittest.TestCase):
    """Regression tests for #63616.

    ``MultiRLModuleSpec.rl_module_specs`` is documented (line 553 of
    ``multi_rl_module.py``) to accept either a dict mapping ModuleID ->
    RLModuleSpec or a single shared ``RLModuleSpec`` (applied to every
    module_id; see the shared policy net example in the RLlib RLModule
    docs). Prior to #63616 the construction path called ``.values()`` on
    ``rl_module_specs`` unconditionally and crashed with
    ``AttributeError: 'RLModuleSpec' object has no attribute 'values'``
    on the documented shared-spec usage.
    """

    def test_single_shared_spec_construction_does_not_raise(self):
        """The documented shared-spec form must construct without raising."""
        shared_spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
            model_config={"hidden_dim": 32},
        )
        multi_spec = MultiRLModuleSpec(rl_module_specs=shared_spec)
        # The shape of ``rl_module_specs`` is preserved as the single spec;
        # the dict expansion happens later in
        # ``AlgorithmConfig.get_multi_rl_module_spec``.
        self.assertIs(multi_spec.rl_module_specs, shared_spec)

    def test_single_shared_spec_inherits_inference_only_flag(self):
        """``inference_only`` defaults to the shared spec's own flag."""
        shared_spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
            inference_only=True,
        )
        multi_spec = MultiRLModuleSpec(rl_module_specs=shared_spec)
        self.assertTrue(multi_spec.inference_only)

        non_inference_spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
            inference_only=False,
        )
        multi_spec = MultiRLModuleSpec(rl_module_specs=non_inference_spec)
        self.assertFalse(multi_spec.inference_only)

    def test_explicit_inference_only_overrides_shared_spec_default(self):
        """User-provided ``inference_only`` wins over the spec-derived default."""
        shared_spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
            inference_only=True,
        )
        # User asks for inference_only=False even though the inner spec
        # is inference_only=True; explicit value must be honored.
        multi_spec = MultiRLModuleSpec(
            rl_module_specs=shared_spec,
            inference_only=False,
        )
        self.assertFalse(multi_spec.inference_only)

    def test_single_shared_spec_contains_returns_true_for_any_module_id(self):
        """A single shared spec applies to every module_id, so ``in`` is True."""
        shared_spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
        )
        multi_spec = MultiRLModuleSpec(rl_module_specs=shared_spec)
        self.assertIn("player1", multi_spec)
        self.assertIn("player2", multi_spec)
        self.assertIn(DEFAULT_MODULE_ID, multi_spec)

    def test_single_shared_spec_getitem_returns_shared_spec(self):
        """A single shared spec is returned for any module_id lookup."""
        shared_spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
        )
        multi_spec = MultiRLModuleSpec(rl_module_specs=shared_spec)
        self.assertIs(multi_spec["player1"], shared_spec)
        self.assertIs(multi_spec["player2"], shared_spec)

    def test_single_shared_spec_survives_update_with_rl_module_spec(self):
        """``MultiRLModuleSpec.update(other_RLModuleSpec)`` must not crash on
        a single-spec form.

        ``AlgorithmConfig._compute_rl_module_spec`` merges the user-provided
        spec with the algorithm's default ``RLModuleSpec`` via this exact
        path, so a single-spec ``MultiRLModuleSpec`` from the docs example
        would otherwise crash at config-merge time even after the
        construction fix.
        """
        shared_spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
            inference_only=True,
        )
        multi_spec = MultiRLModuleSpec(rl_module_specs=shared_spec)

        other = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
            inference_only=False,
        )
        # Must not raise (was crashing with
        # ``AttributeError: 'RLModuleSpec' object has no attribute 'items'``).
        multi_spec.update(other)

        # And the shared-spec ``inference_only`` flag must follow the
        # ``other`` spec's ``inference_only=False``.
        self.assertFalse(multi_spec.inference_only)

    def test_from_dict_does_not_misread_module_class_module_id(self):
        """A per-module dict with a ModuleID literally named "module_class"
        must NOT be misclassified as the single-spec form.

        The discriminator is the *value type* at the "module_class" key:
        ``RLModuleSpec.to_dict()`` writes a serialized type *string*,
        whereas a per-module dict ``{"module_class": <spec_dict>, ...}``
        carries a *dict* at that key.
        """
        per_module_spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
        )
        multi_spec = MultiRLModuleSpec(
            rl_module_specs={"module_class": per_module_spec},
        )
        serialized = multi_spec.to_dict()
        # The per-module form puts a dict at the "module_class" key.
        self.assertIsInstance(serialized["rl_module_specs"]["module_class"], dict)

        restored = MultiRLModuleSpec.from_dict(serialized)
        # Must round-trip as a dict-of-specs, NOT as a single shared spec.
        self.assertIsInstance(restored.rl_module_specs, dict)
        self.assertIn("module_class", restored.rl_module_specs)
        self.assertIsInstance(restored.rl_module_specs["module_class"], RLModuleSpec)

    def test_single_shared_spec_round_trips_through_to_dict_from_dict(self):
        """``to_dict``/``from_dict`` must round-trip the single shared-spec form.

        Required for checkpointing and for sending specs to remote workers.
        """
        shared_spec = RLModuleSpec(
            module_class=VPGTorchRLModule,
            observation_space=gym.spaces.Box(0, 1, shape=(4,)),
            action_space=gym.spaces.Discrete(2),
            inference_only=False,
        )
        multi_spec = MultiRLModuleSpec(rl_module_specs=shared_spec)

        serialized = multi_spec.to_dict()
        # The single-spec form serializes as a single RLModuleSpec dict
        # (no per-module_id wrapping); the ``module_class`` discriminator
        # key must be present so ``from_dict`` can detect this shape.
        self.assertIn("module_class", serialized["rl_module_specs"])

        restored = MultiRLModuleSpec.from_dict(serialized)
        self.assertIsInstance(restored.rl_module_specs, RLModuleSpec)
        self.assertEqual(restored.rl_module_specs.module_class, VPGTorchRLModule)
        self.assertEqual(restored.inference_only, multi_spec.inference_only)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
