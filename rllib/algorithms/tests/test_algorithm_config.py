import gymnasium as gym
from typing import Type
import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import make_multi_callbacks
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.algorithms.ppo.tf.ppo_tf_learner import PPOTfLearner
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModuleSpec, RLModule
from ray.rllib.core.rl_module.multi_rl_module import (
    MultiRLModule,
    MultiRLModuleSpec,
)
from ray.rllib.utils.test_utils import check


class TestAlgorithmConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_running_specific_algo_with_generic_config(self):
        """Tests, whether some algo can be run with the generic AlgorithmConfig."""
        config = (
            AlgorithmConfig(algo_class=PPO)
            .environment("CartPole-v0")
            .training(lr=0.12345, train_batch_size=3000, minibatch_size=300)
        )
        algo = config.build()
        self.assertTrue(algo.config.lr == 0.12345)
        self.assertTrue(algo.config.train_batch_size == 3000)
        algo.train()
        algo.stop()

    def test_update_from_dict_works_for_multi_callbacks(self):
        """Test to make sure callbacks config dict works."""
        config_dict = {"callbacks": make_multi_callbacks([])}
        config = AlgorithmConfig()
        # This should work.
        config.update_from_dict(config_dict)

        serialized = config.serialize()

        # For now, we don't support serializing make_multi_callbacks.
        # It'll turn into a classpath that's not really usable b/c the class
        # was created on-the-fly.
        self.assertEqual(
            serialized["callbacks"],
            "ray.rllib.algorithms.callbacks.make_multi_callbacks.<locals>."
            "_MultiCallbacks",
        )

    def test_freezing_of_algo_config(self):
        """Tests, whether freezing an AlgorithmConfig actually works as expected."""
        config = (
            AlgorithmConfig()
            .environment("CartPole-v0")
            .training(lr=0.12345, train_batch_size=3000)
            .multi_agent(
                policies={
                    "pol1": (None, None, None, AlgorithmConfig.overrides(lr=0.001))
                },
                policy_mapping_fn=lambda agent_id, episode, worker, **kw: "pol1",
            )
        )
        config.freeze()

        def set_lr(config):
            config.lr = 0.01

        self.assertRaisesRegex(
            AttributeError,
            "Cannot set attribute.+of an already frozen AlgorithmConfig",
            lambda: set_lr(config),
        )

        # TODO: Figure out, whether we should convert all nested structures into
        #  frozen ones (set -> frozenset; dict -> frozendict; list -> tuple).

        def set_one_policy(config):
            config.policies["pol1"] = (None, None, None, {"lr": 0.123})

        # self.assertRaisesRegex(
        #    AttributeError,
        #    "Cannot set attribute.+of an already frozen AlgorithmConfig",
        #    lambda: set_one_policy(config),
        # )

    def test_rollout_fragment_length(self):
        """Tests the proper auto-computation of the `rollout_fragment_length`."""
        config = (
            AlgorithmConfig()
            .env_runners(
                num_env_runners=4,
                num_envs_per_env_runner=3,
                rollout_fragment_length="auto",
            )
            .training(train_batch_size=2456)
        )
        # 2456 / (3 * 4) -> 204.666 -> 204 or 205 (depending on worker index).
        # Actual train batch size: 2457 (off by only 1).
        self.assertTrue(config.get_rollout_fragment_length(worker_index=0) == 205)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=1) == 205)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=2) == 205)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=3) == 205)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=4) == 204)

        config = (
            AlgorithmConfig()
            .env_runners(
                num_env_runners=3,
                num_envs_per_env_runner=2,
                rollout_fragment_length="auto",
            )
            .training(train_batch_size=4000)
        )
        # 4000 / 6 -> 666.66 -> 666 or 667 (depending on worker index)
        # Actual train batch size: 4000 (perfect match)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=0) == 667)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=1) == 667)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=2) == 667)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=3) == 666)

        config = (
            AlgorithmConfig()
            .env_runners(
                num_env_runners=12,
                rollout_fragment_length="auto",
            )
            .training(train_batch_size=1342)
        )
        # 1342 / 12 -> 111.83 -> 111 or 112 (depending on worker index)
        # Actual train batch size: 1342 (perfect match)
        for i in range(11):
            self.assertTrue(config.get_rollout_fragment_length(worker_index=i) == 112)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=11) == 111)
        self.assertTrue(config.get_rollout_fragment_length(worker_index=12) == 111)

    def test_detect_atari_env(self):
        """Tests that we can properly detect Atari envs."""
        config = AlgorithmConfig().environment(
            env="ALE/Breakout-v5", env_config={"frameskip": 1}
        )
        self.assertTrue(config.is_atari)

        config = AlgorithmConfig().environment(env="ALE/Pong-v5")
        self.assertTrue(config.is_atari)

        config = AlgorithmConfig().environment(env="CartPole-v1")
        # We do not auto-detect callable env makers for Atari envs.
        self.assertFalse(config.is_atari)

        config = AlgorithmConfig().environment(
            env=lambda ctx: gym.make(
                "ALE/Breakout-v5",
                frameskip=1,
            )
        )
        # We do not auto-detect callable env makers for Atari envs.
        self.assertFalse(config.is_atari)

        config = AlgorithmConfig().environment(env="NotAtari")
        self.assertFalse(config.is_atari)

    def test_rl_module_api(self):
        config = (
            PPOConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .environment("CartPole-v1")
            .framework("torch")
            .env_runners(enable_connectors=True)
        )

        self.assertEqual(config.rl_module_spec.module_class, PPOTorchRLModule)

        class A:
            pass

        config = config.rl_module(rl_module_spec=RLModuleSpec(A))
        self.assertEqual(config.rl_module_spec.module_class, A)

    def test_config_per_module(self):
        """Tests, whether per-module config overrides (multi-agent) work as expected."""

        # Compile individual agents' PPO configs from a config object.
        config = (
            PPOConfig()
            .training(kl_coeff=0.5)
            .multi_agent(
                policies={"module_1", "module_2", "module_3"},
                # Override config settings fro `module_1` and `module_2`.
                algorithm_config_overrides_per_module={
                    "module_1": PPOConfig.overrides(lr=0.01, kl_coeff=0.1),
                    "module_2": PPOConfig.overrides(grad_clip=100.0),
                },
            )
        )

        # Check default config.
        check(config.lr, 0.00005)
        check(config.grad_clip, None)
        check(config.grad_clip_by, "global_norm")
        check(config.kl_coeff, 0.5)

        # `module_1` overrides.
        config_1 = config.get_config_for_module("module_1")
        check(config_1.lr, 0.01)
        check(config_1.grad_clip, None)
        check(config_1.grad_clip_by, "global_norm")
        check(config_1.kl_coeff, 0.1)

        # `module_2` overrides.
        config_2 = config.get_config_for_module("module_2")
        check(config_2.lr, 0.00005)
        check(config_2.grad_clip, 100.0)
        check(config_2.grad_clip_by, "global_norm")
        check(config_2.kl_coeff, 0.5)

        # No `module_3` overrides (b/c module_3 uses the top-level config
        # object directly).
        self.assertTrue("module_3" not in config._per_module_overrides)
        config_3 = config.get_config_for_module("module_3")
        self.assertTrue(config_3 is config)

    def test_learner_api(self):
        config = (
            PPOConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .environment("CartPole-v1")
            .env_runners(enable_connectors=True)
            .framework("tf2")
        )

        self.assertEqual(config.learner_class, PPOTfLearner)

    def _assertEqualMARLSpecs(self, spec1, spec2):
        self.assertEqual(spec1.multi_rl_module_class, spec2.multi_rl_module_class)

        self.assertEqual(set(spec1.module_specs.keys()), set(spec2.module_specs.keys()))
        for k, module_spec1 in spec1.module_specs.items():
            module_spec2 = spec2.module_specs[k]

            self.assertEqual(module_spec1.module_class, module_spec2.module_class)
            self.assertEqual(
                module_spec1.observation_space, module_spec2.observation_space
            )
            self.assertEqual(module_spec1.action_space, module_spec2.action_space)
            self.assertEqual(
                module_spec1.model_config_dict, module_spec2.model_config_dict
            )

    def _get_expected_marl_spec(
        self,
        config: AlgorithmConfig,
        expected_module_class: Type[RLModule],
        passed_module_class: Type[RLModule] = None,
        expected_multi_rl_module_class: Type[MultiRLModule] = None,
    ):
        """This is a utility function that retrieves the expected marl specs.

        Args:
            config: The algorithm config.
            expected_module_class: This is the expected RLModule class that is going to
                be reference in the RLModuleSpec parts of the MultiLModuleSpec.
            passed_module_class: This is the RLModule class that is passed into the
                module_spec argument of get_multi_rl_module_spec. The function is
                designed so that it will use the passed in module_spec for the
                RLModuleSpec parts of the MultiRLModuleSpec.
            expected_multi_rl_module_class: This is the expected MultiRLModule class
                that is going to be reference in the MultiRLModuleSpec.

        Returns:
            Tuple of the returned MultiRLModuleSpec from config.
            get_multi_rl_module_spec() and the expected MultiRLModuleSpec.
        """
        from ray.rllib.policy.policy import PolicySpec

        if expected_multi_rl_module_class is None:
            expected_multi_rl_module_class = MultiRLModule

        env = gym.make("CartPole-v1")
        policy_spec_ph = PolicySpec(
            observation_space=env.observation_space,
            action_space=env.action_space,
            config=AlgorithmConfig(),
        )

        marl_spec = config.get_multi_rl_module_spec(
            policy_dict={"p1": policy_spec_ph, "p2": policy_spec_ph},
            single_agent_rl_module_spec=RLModuleSpec(module_class=passed_module_class)
            if passed_module_class
            else None,
        )

        expected_marl_spec = MultiRLModuleSpec(
            multi_rl_module_class=expected_multi_rl_module_class,
            module_specs={
                "p1": RLModuleSpec(
                    module_class=expected_module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict=AlgorithmConfig().model_config,
                ),
                "p2": RLModuleSpec(
                    module_class=expected_module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                    model_config_dict=AlgorithmConfig().model_config,
                ),
            },
        )

        return marl_spec, expected_marl_spec

    def test_get_multi_rl_module_spec(self):
        """Tests whether the get_multi_rl_module_spec() method works properly."""
        from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

        class CustomRLModule1(DiscreteBCTorchModule):
            pass

        class CustomRLModule2(DiscreteBCTorchModule):
            pass

        class CustomRLModule3(DiscreteBCTorchModule):
            pass

        class CustomMultiRLModule1(MultiRLModule):
            pass

        ########################################
        # single agent
        class SingleAgentAlgoConfig(AlgorithmConfig):
            def get_default_rl_module_spec(self):
                return RLModuleSpec(module_class=DiscreteBCTorchModule)

        # multi-agent
        class MultiAgentAlgoConfigWithNoSingleAgentSpec(AlgorithmConfig):
            def get_default_rl_module_spec(self):
                return MultiRLModuleSpec(multi_rl_module_class=CustomMultiRLModule1)

        class MultiAgentAlgoConfig(AlgorithmConfig):
            def get_default_rl_module_spec(self):
                return MultiRLModuleSpec(
                    multi_rl_module_class=CustomMultiRLModule1,
                    module_specs=RLModuleSpec(module_class=DiscreteBCTorchModule),
                )

        ########################################
        # This is the simplest case where we have to construct the MultiRLModule based
        # on the default specs only.
        config = SingleAgentAlgoConfig().api_stack(enable_rl_module_and_learner=True)

        spec, expected = self._get_expected_marl_spec(config, DiscreteBCTorchModule)
        self._assertEqualMARLSpecs(spec, expected)

        # expected module should become the passed module if we pass it in.
        spec, expected = self._get_expected_marl_spec(
            config, CustomRLModule2, passed_module_class=CustomRLModule2
        )
        self._assertEqualMARLSpecs(spec, expected)

        ########################################
        # This is the case where we pass in a `MultiRLModuleSpec` that asks the
        # algorithm to assign a specific type of RLModule class to certain module_ids.
        config = (
            SingleAgentAlgoConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(
                    module_specs={
                        "p1": RLModuleSpec(module_class=CustomRLModule1),
                        "p2": RLModuleSpec(module_class=CustomRLModule1),
                    },
                ),
            )
        )

        spec, expected = self._get_expected_marl_spec(config, CustomRLModule1)
        self._assertEqualMARLSpecs(spec, expected)

        ########################################
        # This is the case where we ask the algorithm to assign a specific type of
        # RLModule class to ALL module_ids.
        config = (
            SingleAgentAlgoConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .rl_module(
                rl_module_spec=RLModuleSpec(module_class=CustomRLModule1),
            )
        )

        spec, expected = self._get_expected_marl_spec(config, CustomRLModule1)
        self._assertEqualMARLSpecs(spec, expected)

        # expected module should become the passed module if we pass it in.
        spec, expected = self._get_expected_marl_spec(
            config, CustomRLModule2, passed_module_class=CustomRLModule2
        )
        self._assertEqualMARLSpecs(spec, expected)
        ########################################
        # This is an alternative way to ask the algorithm to assign a specific type of
        # RLModule class to ALL module_ids.
        config = (
            SingleAgentAlgoConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(
                    module_specs=RLModuleSpec(module_class=CustomRLModule1)
                ),
            )
        )

        spec, expected = self._get_expected_marl_spec(config, CustomRLModule1)
        self._assertEqualMARLSpecs(spec, expected)

        # expected module should become the passed module if we pass it in.
        spec, expected = self._get_expected_marl_spec(
            config, CustomRLModule2, passed_module_class=CustomRLModule2
        )
        self._assertEqualMARLSpecs(spec, expected)

        ########################################
        # This is not only assigning a specific type of RLModule class to EACH
        # module_id, but also defining a new custom MultiRLModule class to be used
        # in the multi-agent scenario.
        config = (
            SingleAgentAlgoConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(
                    multi_rl_module_class=CustomMultiRLModule1,
                    module_specs={
                        "p1": RLModuleSpec(module_class=CustomRLModule1),
                        "p2": RLModuleSpec(module_class=CustomRLModule1),
                    },
                ),
            )
        )

        spec, expected = self._get_expected_marl_spec(
            config, CustomRLModule1, expected_multi_rl_module_class=CustomMultiRLModule1
        )
        self._assertEqualMARLSpecs(spec, expected)

        # This is expected to return CustomRLModule1 instead of CustomRLModule3 which
        # is passed in. Because the default for p1, p2 is to use CustomRLModule1. The
        # passed module_spec only sets a default to fall back onto in case the
        # module_id is not specified in the original MultiRLModuleSpec. Since P1
        # and P2 are both assigned to CustomeRLModule1, the passed module_spec will not
        # be used. This is the expected behavior for adding a new modules to a
        # `MultiRLModule` that is not defined in the original MultiRLModuleSpec.
        spec, expected = self._get_expected_marl_spec(
            config,
            CustomRLModule1,
            passed_module_class=CustomRLModule3,
            expected_multi_rl_module_class=CustomMultiRLModule1,
        )
        self._assertEqualMARLSpecs(spec, expected)

        ########################################
        # This is the case where we ask the algorithm to use its default
        # MultiRLModuleSpec, but the MultiRLModuleSpec has not defined its
        # RLModuleSpecs.
        config = MultiAgentAlgoConfigWithNoSingleAgentSpec().api_stack(
            enable_rl_module_and_learner=True
        )

        self.assertRaisesRegex(
            ValueError,
            "Module_specs cannot be None",
            lambda: config.rl_module_spec,
        )

        ########################################
        # This is the case where we ask the algorithm to use its default
        # MultiRLModuleSpec, and the MultiRLModuleSpec has defined its
        # RLModuleSpecs.
        config = MultiAgentAlgoConfig().api_stack(enable_rl_module_and_learner=True)

        spec, expected = self._get_expected_marl_spec(
            config,
            DiscreteBCTorchModule,
            expected_multi_rl_module_class=CustomMultiRLModule1,
        )
        self._assertEqualMARLSpecs(spec, expected)

        spec, expected = self._get_expected_marl_spec(
            config,
            CustomRLModule1,
            passed_module_class=CustomRLModule1,
            expected_multi_rl_module_class=CustomMultiRLModule1,
        )
        self._assertEqualMARLSpecs(spec, expected)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
