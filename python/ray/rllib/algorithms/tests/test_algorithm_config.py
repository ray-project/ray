import copy
import unittest
from typing import Type

import gymnasium as gym

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.rl_module.multi_rl_module import (
    MultiRLModule,
    MultiRLModuleSpec,
)
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
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

    def test_multi_agent_shared_module(self):
        """Multiple agents sharing a single RLModule via a one-entry spec dict."""
        config = (
            PPOConfig()
            .environment(MultiAgentCartPole, env_config={"num_agents": 2})
            .env_runners(num_env_runners=0)
            .multi_agent(
                policies={"p0"},
                policy_mapping_fn=lambda agent_id, *args, **kwargs: "p0",
            )
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(
                    rl_module_specs={"p0": RLModuleSpec()},
                ),
            )
        )
        algo = config.build_algo()
        try:
            self.assertEqual(set(algo.env_runner.module.keys()), {"p0"})
            algo.train()
        finally:
            algo.stop()

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
            env="ale_py:ALE/Breakout-v5", env_config={"frameskip": 1}
        )
        self.assertTrue(config.is_atari)

        config = AlgorithmConfig().environment(env="ale_py:ALE/Pong-v5")
        self.assertTrue(config.is_atari)

        config = AlgorithmConfig().environment(env="CartPole-v1")
        # We do not auto-detect callable env makers for Atari envs.
        self.assertFalse(config.is_atari)

        config = AlgorithmConfig().environment(
            env=lambda ctx: gym.make(
                "ale_py:ALE/Breakout-v5",
                frameskip=1,
            )
        )
        # We do not auto-detect callable env makers for Atari envs.
        self.assertFalse(config.is_atari)

        config = AlgorithmConfig().environment(env="NotAtari")
        self.assertFalse(config.is_atari)

    def test_rl_module_api(self):
        config = PPOConfig().environment("CartPole-v1").framework("torch")

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
        config = PPOConfig().environment("CartPole-v1")

        self.assertEqual(config.learner_class, PPOTorchLearner)

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
            rl_module_specs={
                "p1": RLModuleSpec(
                    module_class=expected_module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                ),
                "p2": RLModuleSpec(
                    module_class=expected_module_class,
                    observation_space=env.observation_space,
                    action_space=env.action_space,
                ),
            },
        )

        return marl_spec, expected_marl_spec

    def test_get_multi_rl_module_spec(self):
        """Tests whether the get_multi_rl_module_spec() method works properly."""
        from ray.rllib.examples.rl_modules.classes.vpg_torch_rlm import VPGTorchRLModule

        class CustomRLModule1(VPGTorchRLModule):
            pass

        class CustomRLModule2(VPGTorchRLModule):
            pass

        class CustomRLModule3(VPGTorchRLModule):
            pass

        class CustomMultiRLModule1(MultiRLModule):
            pass

        ########################################
        # single agent
        class SingleAgentAlgoConfig(AlgorithmConfig):
            def get_default_rl_module_spec(self):
                return RLModuleSpec(module_class=VPGTorchRLModule)

        # multi-agent
        class MultiAgentAlgoConfigWithNoSingleAgentSpec(AlgorithmConfig):
            def get_default_rl_module_spec(self):
                return MultiRLModuleSpec(multi_rl_module_class=CustomMultiRLModule1)

        ########################################
        # This is the simplest case where we have to construct the MultiRLModule based
        # on the default specs only.
        config = SingleAgentAlgoConfig().api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )

        spec, expected = self._get_expected_marl_spec(config, VPGTorchRLModule)
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
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(
                    rl_module_specs={
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
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
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
        # This is not only assigning a specific type of RLModule class to EACH
        # module_id, but also defining a new custom MultiRLModule class to be used
        # in the multi-agent scenario.
        config = (
            SingleAgentAlgoConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(
                    multi_rl_module_class=CustomMultiRLModule1,
                    rl_module_specs={
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
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )

        self.assertRaisesRegex(
            ValueError,
            "must be a dict",
            lambda: config.rl_module_spec,
        )

    def test_rollout_fragment_length_with_small_batch_and_multiple_learners(self):
        """Test that get_rollout_fragment_length doesn't return 0 when train_batch_size=1 and num_learners > 1."""
        for num_env_runners in [1, 2, 3, 4]:
            config = (
                AlgorithmConfig()
                .env_runners(
                    rollout_fragment_length="auto",
                    num_env_runners=num_env_runners,
                )
                .learners(
                    num_learners=2
                )  # Multiple learners with train_batch_size=1 causes the issue
                .training(
                    train_batch_size=1
                )  # Small batch size with multiple learners causes integer division to 0
            )

            # This should not return 0
            rollout_fragment_length = config.get_rollout_fragment_length(0)
            self.assertEqual(
                rollout_fragment_length,
                1,
            )

    def test_to_dict_roundtrip_new_api_stack(self):
        """Tests that to_dict() round-trips New API stack batch sizes.

        `to_dict()` does NOT eagerly resolve the effective batch size (that stays
        lazy via the `total_train_batch_size` property). It only serializes the raw
        fields, which is what makes it safe to call on an as-yet-unresolved config
        (e.g. one carrying Tune search spaces).
        """
        from ray.rllib.algorithms.ppo import PPOConfig

        # 1. Create a config on the New API Stack
        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .training(train_batch_size_per_learner=123)
        )

        # 2. Export to dictionary
        config_dict = config.to_dict()

        # to_dict() does not inject computed properties (would break round-trip).
        self.assertNotIn("total_train_batch_size", config_dict)
        self.assertNotIn("train_batch_size_per_learner", config_dict)

        # 3. Roundtrip: Create a new config and update from the dictionary, and
        # verify the per-learner batch size (and the total derived from it) survives.
        new_config = PPOConfig().update_from_dict(config_dict)
        self.assertEqual(new_config.train_batch_size_per_learner, 123)
        self.assertEqual(new_config.total_train_batch_size, 123)

    def test_to_dict_with_tune_search_space(self):
        """to_dict() must not eagerly resolve batch size when it's a Tune search space.

        Regression test: passing an AlgorithmConfig with a search-space
        `train_batch_size_per_learner` as Tune's `param_space` calls `to_dict()` on
        an unresolved config. Computing `total_train_batch_size` (`Domain * int`)
        would raise TypeError, so `to_dict()` must not attempt it.
        """
        from ray import tune
        from ray.rllib.algorithms.ppo import PPOConfig

        config = (
            PPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .training(train_batch_size_per_learner=tune.qrandint(256, 2048, 64))
        )

        # Must not raise (this is the bug: TypeError from `Domain * int`).
        config_dict = config.to_dict()

        # The unresolved search space survives serialization so Tune can sample it.
        self.assertIsInstance(
            config_dict["_train_batch_size_per_learner"], tune.search.sample.Domain
        )

    def test_connector_pipeline_builder(self):
        """Tests the `*_connector_builder` hooks for all three pipelines.

        A builder receives the fully-built ConnectorPipelineV2 - including RLlib's
        default pieces - and may modify it in place, e.g. insert a custom piece
        between two default ones or remove a default piece.
        """
        from ray.rllib.connectors.common import BatchIndividualItems
        from ray.rllib.connectors.connector_v2 import ConnectorV2
        from ray.rllib.connectors.learner import AddColumnsFromEpisodesToTrainBatch
        from ray.rllib.connectors.module_to_env import GetActions

        class Marker(ConnectorV2):
            def __call__(self, *, rl_module, batch, episodes, shared_data=None):
                return batch

        def connector_names(pipeline):
            return [type(c).__name__ for c in pipeline.connectors]

        env = gym.make("CartPole-v1")
        obs_space, act_space = env.observation_space, env.action_space

        # Env-to-module: Insert a custom piece between two default pieces.
        def env_to_module_builder(pipeline, device=None):
            pipeline.insert_before(BatchIndividualItems, Marker())
            return pipeline

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(env_to_module_connector_builder=env_to_module_builder)
        )
        self.assertEqual(
            connector_names(config.build_env_to_module_connector(env=env)),
            [
                "AddObservationsFromEpisodesToBatch",
                "AddTimeDimToBatchAndZeroPad",
                "AddStatesFromEpisodesToBatch",
                "Marker",
                "BatchIndividualItems",
                "NumpyToTensor",
            ],
        )

        # Module-to-env: remove a default piece (verify device).
        seen_device = []

        def module_to_env_builder(pipeline, device=None):
            seen_device.append(device)
            pipeline.remove(GetActions)
            return pipeline

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(module_to_env_connector_builder=module_to_env_builder)
        )
        names = connector_names(
            config.build_module_to_env_connector(env=env, device="cpu")
        )
        self.assertNotIn("GetActions", names)
        self.assertIn("NormalizeAndClipActions", names)
        self.assertEqual(seen_device, ["cpu"])

        # Learner: Replace a default piece with a custom one.
        def learner_builder(pipeline, device=None):
            pipeline.remove(AddColumnsFromEpisodesToTrainBatch)
            pipeline.insert_after("AddObservationsFromEpisodesToBatch", Marker())
            return pipeline

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .learners(learner_connector_builder=learner_builder)
        )
        names = connector_names(config.build_learner_connector(obs_space, act_space))
        self.assertNotIn("AddColumnsFromEpisodesToTrainBatch", names)
        self.assertEqual(
            names.index("Marker"),
            names.index("AddObservationsFromEpisodesToBatch") + 1,
        )

        # Legacy `*_connector` pieces keep their historical placement.

        # Env-to-module: prepended.
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(env_to_module_connector=lambda env, spaces, device: Marker())
        )
        names = connector_names(config.build_env_to_module_connector(env=env))
        self.assertEqual(names.index("Marker"), 0)

        # Module-to-env: in the middle.
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(module_to_env_connector=lambda env, spaces: Marker())
        )
        names = connector_names(config.build_module_to_env_connector(env=env))
        self.assertLess(
            names.index("RemoveSingleTsTimeRankFromBatch"),
            names.index("Marker"),
        )
        self.assertLess(
            names.index("Marker"),
            names.index("NormalizeAndClipActions"),
        )

        # Learner: prepended.
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .learners(learner_connector=lambda obs, act: Marker())
        )
        names = connector_names(config.build_learner_connector(obs_space, act_space))
        self.assertEqual(names.index("Marker"), 0)

        # Legacy `*_connector` + builder can be combined.
        seen = []

        def combined_builder(pipeline, device=None):
            seen.append(connector_names(pipeline))
            pipeline.insert_after("Marker", Marker())
            return pipeline

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                env_to_module_connector=lambda env, spaces, device: Marker(),
                env_to_module_connector_builder=combined_builder,
            )
        )
        names = connector_names(config.build_env_to_module_connector(env=env))
        # Builder saw legacy piece + defaults.
        self.assertEqual(seen[0][0], "Marker")
        self.assertIn("AddObservationsFromEpisodesToBatch", seen[0])
        # Builder's change applied.
        self.assertEqual(names.count("Marker"), 2)

    def test_connector_pipeline_insert_missing_raises(self):
        """`insert_before`/`insert_after` raise if the target piece is absent."""
        from ray.rllib.connectors.common import NumpyToTensor
        from ray.rllib.connectors.connector_pipeline_v2 import ConnectorPipelineV2
        from ray.rllib.connectors.connector_v2 import ConnectorV2

        class Marker(ConnectorV2):
            def __call__(self, *, rl_module, batch, episodes, shared_data=None):
                return batch

        pipeline = ConnectorPipelineV2(connectors=[Marker()])
        with self.assertRaisesRegex(ValueError, "Can not find connector"):
            pipeline.insert_before(NumpyToTensor, Marker())
        with self.assertRaisesRegex(ValueError, "Can not find connector"):
            pipeline.insert_after(NumpyToTensor, Marker())
        # A real target still works and lands in the right spot.
        pipeline.insert_before(Marker, NumpyToTensor())
        self.assertEqual(
            [type(c).__name__ for c in pipeline.connectors],
            ["NumpyToTensor", "Marker"],
        )

    def test_differentiable_learner_connector_builder(self):
        """`DifferentiableLearnerConfig` forwards the builder + device."""
        from ray.rllib.connectors.common import BatchIndividualItems
        from ray.rllib.connectors.connector_v2 import ConnectorV2
        from ray.rllib.core.learner.differentiable_learner import DifferentiableLearner
        from ray.rllib.core.learner.differentiable_learner_config import (
            DifferentiableLearnerConfig,
        )

        class DummyDiffLearner(DifferentiableLearner):
            pass

        class Marker(ConnectorV2):
            def __call__(self, *, rl_module, batch, episodes, shared_data=None):
                return batch

        seen_device = []

        def builder(pipeline, device=None):
            seen_device.append(device)
            pipeline.insert_before(BatchIndividualItems, Marker())
            return pipeline

        learner_cfg = DifferentiableLearnerConfig(
            learner_class=DummyDiffLearner,
            learner_connector_builder=builder,
        )
        env = gym.make("CartPole-v1")
        pipeline = learner_cfg.build_learner_connector(
            env.observation_space, env.action_space, device="cpu"
        )
        names = [type(c).__name__ for c in pipeline.connectors]
        self.assertIn("Marker", names)
        self.assertEqual(seen_device, ["cpu"])

    def test_connector_builder_receives_device(self):
        """All three builder hooks receive the `device` kwarg."""
        env = gym.make("CartPole-v1")
        seen = {}

        def make(name):
            def builder(pipeline, device=None):
                seen[name] = device
                return pipeline

            return builder

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                env_to_module_connector_builder=make("env_to_module"),
                module_to_env_connector_builder=make("module_to_env"),
            )
            .learners(learner_connector_builder=make("learner"))
        )
        config.build_env_to_module_connector(env=env, device="cpu")
        config.build_module_to_env_connector(env=env, device="cpu")
        config.build_learner_connector(
            env.observation_space, env.action_space, device="cpu"
        )
        self.assertEqual(
            seen,
            {"env_to_module": "cpu", "module_to_env": "cpu", "learner": "cpu"},
        )

    def test_connector_builder_must_return_pipeline(self):
        """A builder that doesn't return the specific pipeline type raises."""
        env = gym.make("CartPole-v1")
        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(env_to_module_connector_builder=lambda p, device: None)
        )
        with self.assertRaisesRegex(ValueError, "must return an `EnvToModulePipeline`"):
            config.build_env_to_module_connector(env=env)

    def test_connector_builder_with_defaults_disabled(self):
        """With default connectors disabled, the builder runs on the bare pipeline."""
        from ray.rllib.connectors.connector_v2 import ConnectorV2

        class Marker(ConnectorV2):
            def __call__(self, *, rl_module, batch, episodes, shared_data=None):
                return batch

        env = gym.make("CartPole-v1")

        def builder(pipeline, device=None):
            pipeline.prepend(Marker())
            return pipeline

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(
                add_default_connectors_to_env_to_module_pipeline=False,
                env_to_module_connector_builder=builder,
            )
        )
        names = [
            type(c).__name__
            for c in config.build_env_to_module_connector(env=env).connectors
        ]
        self.assertEqual(names, ["Marker"])

    def test_connector_builder_config_roundtrip(self):
        """`to_dict`/`from_dict` and `deepcopy` preserve the builder callable."""

        def builder(pipeline, device=None):
            return pipeline

        config = (
            PPOConfig()
            .environment("CartPole-v1")
            .env_runners(env_to_module_connector_builder=builder)
        )
        self.assertIs(
            PPOConfig.from_dict(config.to_dict())._env_to_module_connector_builder,
            builder,
        )
        self.assertIs(
            copy.deepcopy(config)._env_to_module_connector_builder,
            builder,
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
