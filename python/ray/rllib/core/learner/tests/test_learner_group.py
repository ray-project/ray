import tempfile
import unittest

import gymnasium as gym
import numpy as np
import pytest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import (
    COMPONENT_LEARNER,
    COMPONENT_RL_MODULE,
    DEFAULT_MODULE_ID,
    Columns,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule, MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.core.testing.testing_learner import BaseTestingAlgorithmConfig
from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    LEARNER_CONNECTOR,
    LEARNER_MODULE_STEPS_DROPPED_FOR_PEER_LIFETIME,
    LEARNER_MODULE_STEPS_DROPPED_ON_SKIP_LIFETIME,
    LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME,
    LEARNER_UPDATE_SKIPPED_FOR_PEER_LIFETIME,
    NUM_MODULE_STEPS_TRAINED,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check
from ray.util.timer import _Timer

REMOTE_CONFIGS = {
    "remote-cpu": AlgorithmConfig.overrides(num_learners=1),
    "remote-gpu": AlgorithmConfig.overrides(num_learners=1, num_gpus_per_learner=1),
    "multi-gpu-ddp": AlgorithmConfig.overrides(num_learners=2, num_gpus_per_learner=1),
    "multi-cpu-ddp": AlgorithmConfig.overrides(num_learners=2, num_cpus_per_learner=2),
    # "multi-gpu-ddp-pipeline": AlgorithmConfig.overrides(
    #     num_learners=2, num_gpus_per_learner=2
    # ),
}


LOCAL_CONFIGS = {
    "local-cpu": AlgorithmConfig.overrides(num_learners=0, num_gpus_per_learner=0),
    "local-gpu": AlgorithmConfig.overrides(num_learners=0, num_gpus_per_learner=1),
}


FAKE_EPISODES = [
    SingleAgentEpisode(
        observation_space=gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
        observations=[
            np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
            np.array([0.5, 0.6, 0.7, 0.8], dtype=np.float32),
            np.array([0.9, 1.0, 1.1, 1.2], dtype=np.float32),
            np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
            np.array([-0.1, -0.2, -0.3, -0.4], dtype=np.float32),
        ],
        action_space=gym.spaces.Discrete(2),
        actions=[0, 1, 1, 0],
        rewards=[1.0, -1.0, 0.5, 0.3],
        terminated=True,
        len_lookback_buffer=0,  # all data part of actual episode
    ),
]

FAKE_MA_EPISODES = [
    MultiAgentEpisode(
        agent_module_ids={
            0: "p0",
            1: "p1",
        },
        observation_space=gym.spaces.Dict(
            {
                0: FAKE_EPISODES[0].observation_space,
                1: FAKE_EPISODES[0].observation_space,
            }
        ),
        observations=[
            {
                0: FAKE_EPISODES[0].get_observations(i),
                1: FAKE_EPISODES[0].get_observations(i),
            }
            for i in range(5)
        ],
        action_space=gym.spaces.Dict(
            {
                0: FAKE_EPISODES[0].action_space,
                1: FAKE_EPISODES[0].action_space,
            }
        ),
        actions=[
            {
                0: FAKE_EPISODES[0].get_actions(i),
                1: FAKE_EPISODES[0].get_actions(i),
            }
            for i in range(4)
        ],
        rewards=[
            {
                0: FAKE_EPISODES[0].get_rewards(i),
                1: FAKE_EPISODES[0].get_rewards(i),
            }
            for i in range(4)
        ],
        len_lookback_buffer=0,  # all data part of actual episode
    ),
]
FAKE_MA_EPISODES[0].to_numpy()

FAKE_MA_EPISODES_WO_P1 = [
    MultiAgentEpisode(
        agent_module_ids={0: "p0"},
        observation_space=gym.spaces.Dict({0: FAKE_EPISODES[0].observation_space}),
        observations=[{0: FAKE_EPISODES[0].get_observations(i)} for i in range(5)],
        action_space=gym.spaces.Dict({0: FAKE_EPISODES[0].action_space}),
        actions=[{0: FAKE_EPISODES[0].get_actions(i)} for i in range(4)],
        rewards=[{0: FAKE_EPISODES[0].get_rewards(i)} for i in range(4)],
        len_lookback_buffer=0,  # all data part of actual episode
    ),
]
FAKE_MA_EPISODES_WO_P1[0].to_numpy()

# We use this to emulate situations where some learners receive empty batches.
NO_DATA = MultiAgentBatch(policy_batches={}, env_steps=0)


def fake_batch(num_timesteps, *, env_steps=None, seq_lens=None):
    rng = np.random.default_rng(0)
    columns = {
        Columns.OBS: rng.standard_normal((num_timesteps, 4), dtype=np.float32),
        Columns.ACTIONS: rng.integers(0, 2, size=(num_timesteps,)),
    }
    if seq_lens is not None:
        # Cut the timesteps into sequences, as for a recurrent module. The Learner
        # then slices the batch, and counts its minibatches, in sequences.
        columns[SampleBatch.SEQ_LENS] = np.array(seq_lens)
    return MultiAgentBatch(
        {DEFAULT_MODULE_ID: SampleBatch(columns)},
        env_steps=num_timesteps if env_steps is None else env_steps,
    )


def fake_batch_with_p1(num_timesteps):
    """A `fake_batch` that also holds `num_timesteps` rows for a module "p1"."""
    rows = fake_batch(num_timesteps).policy_batches[DEFAULT_MODULE_ID]
    return MultiAgentBatch(
        {DEFAULT_MODULE_ID: rows, "p1": rows.copy()}, env_steps=num_timesteps
    )


class TestLearnerGroupSyncUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_learner_group_build_from_algorithm_config(self):
        """Tests whether we can build a learner_groupobject from algorithm_config."""

        env = gym.make("CartPole-v1")

        # Config that has its own learner class and RLModule spec.
        config = BaseTestingAlgorithmConfig()
        learner_group = config.build_learner_group(env=env)
        print(learner_group)
        learner_group.shutdown()

        # Config for which user defines custom learner class and RLModule spec.
        config = (
            BaseTestingAlgorithmConfig()
            .training(learner_class=BCTorchLearner)
            .rl_module(
                rl_module_spec=RLModuleSpec(
                    module_class=DiscreteBCTorchModule,
                    model_config={"fcnet_hiddens": [32]},
                )
            )
        )
        learner_group = config.build_learner_group(env=env)
        print(learner_group)
        learner_group.shutdown()

    def test_update_multi_gpu(self):
        return

        scaling_modes = ["multi-gpu-ddp", "remote-gpu"]

        for scaling_mode in scaling_modes:
            print(f"Testing scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")

            config_overrides = REMOTE_CONFIGS[scaling_mode]
            config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)
            learner_group = config.build_learner_group(env=env)

            min_loss = float("inf")
            for iter_i in range(1000):
                results = learner_group.update(episodes=FAKE_EPISODES)

                loss = np.mean(
                    [res[ALL_MODULES][Learner.TOTAL_LOSS_KEY] for res in results]
                )
                min_loss = min(loss, min_loss)
                print(f"[iter = {iter_i}] Loss: {loss:.3f}, Min Loss: {min_loss:.3f}")
                # The loss is initially around 0.69 (ln2). When it gets to around
                # 0.57 the return of the policy gets to around 100.
                if min_loss < 0.57:
                    break

                for res1, res2 in zip(results, results[1:]):
                    self.assertEqual(
                        res1[DEFAULT_MODULE_ID]["mean_weight"],
                        res2[DEFAULT_MODULE_ID]["mean_weight"],
                    )

            self.assertLess(min_loss, 0.57)

            # Make sure the learner_group resources are freed up so that we don't
            # autoscale.
            learner_group.shutdown()
            del learner_group

    def test_add_module_and_remove_module(self):
        scaling_modes = ["local-cpu", "multi-cpu-ddp"]

        for scaling_mode in scaling_modes:
            print(f"Testing scaling mode: {scaling_mode}.")
            ma_env = MultiAgentCartPole({"num_agents": 2})
            config_overrides = REMOTE_CONFIGS.get(scaling_mode) or LOCAL_CONFIGS.get(
                scaling_mode
            )
            config = (
                BCConfig()
                .update_from_dict(config_overrides)
                .multi_agent(
                    policies={"p0"},
                    policy_mapping_fn=lambda aid, *ar, **kw: f"p{aid}",
                )
                .rl_module(
                    rl_module_spec=MultiRLModuleSpec(
                        rl_module_specs={"p0": RLModuleSpec()},
                    )
                )
            )
            learner_group = config.build_learner_group(env=ma_env)

            # Update once with the default policy.
            learner_group.update(episodes=FAKE_MA_EPISODES_WO_P1)

            # Add a test_module.
            learner_group.add_module(
                module_id="p1",
                module_spec=config.get_multi_rl_module_spec(env=ma_env).module_specs[
                    "p0"
                ],
            )
            # Do training that includes the test_module.
            results = learner_group.update(episodes=FAKE_MA_EPISODES)

            # check that module ids are updated to include the new module
            module_ids_after_add = {"p0", "p1"}
            # Compare module IDs in results with expected ones.
            self.assertEqual(
                set(results[0].keys()) - {ALL_MODULES}, module_ids_after_add
            )

            # Remove the test_module.
            learner_group.remove_module(module_id="p1")

            # Run training without the test_module.
            results = learner_group.update(episodes=FAKE_MA_EPISODES_WO_P1)

            # check that module ids are updated after remove operation to not
            # include the new module
            # remove the total_loss key since its not a module key
            self.assertEqual(set(results[0].keys()) - {ALL_MODULES}, {"p0"})

            # make sure the learner_group resources are freed up so that we don't
            # autoscale
            learner_group.shutdown()
            del learner_group


class TestLearnerGroupUpdatePlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_learners_agree_on_the_update_plan(self):
        """Tests that the Learners of a group carry out every update the same way.

        Learners in a DDP group run one all-reduce per minibatch, so one of them
        skipping an update that its peer takes, or stepping through a different
        number of minibatches, makes their collectives mismatch. This test would
        therefore hang rather than fail if the Learners stopped agreeing.
        """
        config = BaseTestingAlgorithmConfig().update_from_dict(
            REMOTE_CONFIGS["multi-cpu-ddp"]
        )
        learner_group = config.build_learner_group(env=gym.make("CartPole-v1"))

        def weights():
            """The module weights of each Learner in the group."""
            return [
                result.get()
                for result in learner_group.foreach_learner(
                    lambda learner: convert_to_numpy(
                        learner.module[DEFAULT_MODULE_ID].get_state()
                    )
                )
            ]

        try:
            # Act as if the EnvRunners feeding the second Learner were lost, so it is handed a
            # batch without any data while its peer has a full one. Both must skip:
            # nobody trains, and each Learner reports why it skipped.
            before = weights()
            with_data, starved = MetricsLogger.peek_results(
                learner_group.update(batches=[fake_batch(128, env_steps=1), NO_DATA])
            )
            check(before, weights())
            self.assertEqual(
                1, starved[ALL_MODULES][LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME]
            )
            self.assertEqual(
                1, with_data[ALL_MODULES][LEARNER_UPDATE_SKIPPED_FOR_PEER_LIFETIME]
            )
            # Module steps, not env steps: a shard from `ShardBatchIterator` carries
            # the row count of whichever module it sliced last (`env_steps=1` here),
            # so counting env steps would report 1 instead of the 128 rows dropped.
            self.assertEqual(
                128,
                with_data[ALL_MODULES][LEARNER_MODULE_STEPS_DROPPED_ON_SKIP_LIFETIME],
            )

            # An empty batch handed to the group as a whole is sharded into empty
            # shards, one per Learner, and every Learner skips it as its own.
            before = weights()
            results = MetricsLogger.peek_results(learner_group.update(batch=NO_DATA))
            check(before, weights())
            self.assertEqual(
                [1, 1],
                [
                    result[ALL_MODULES][LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME]
                    for result in results
                ],
            )

            # The same, but with `minibatch_size` set and the starved Learner handed a
            # batch that still carries its ModuleIDs with no rows under them -- what
            # `ShardBatchIterator` produces when there is not enough data to give every
            # Learner a row.
            before = weights()
            with_data, starved = MetricsLogger.peek_results(
                learner_group.update(
                    batches=[fake_batch(128), fake_batch(0)],
                    minibatch_size=32,
                    num_epochs=1,
                )
            )
            check(before, weights())
            self.assertEqual(
                1, starved[ALL_MODULES][LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME]
            )
            self.assertEqual(
                1, with_data[ALL_MODULES][LEARNER_UPDATE_SKIPPED_FOR_PEER_LIFETIME]
            )

            # Both Learners have data, but unequal amounts of it. On their own they
            # would step through ceil(256/32) = 8 and ceil(64/32) = 2 minibatches.
            # The group settles on the LARGER of the two, 8, not on their average, 5:
            # 5 would leave the bigger shard short of the epochs it was configured
            # for (see `test_minibatch_coverage_across_unequal_shards`). Both
            # Learners step 8 times and stay in sync.
            results = MetricsLogger.peek_results(
                learner_group.update(
                    batches=[fake_batch(256), fake_batch(64)],
                    minibatch_size=32,
                    num_epochs=1,
                )
            )
            self.assertEqual(
                [8 * 32, 8 * 32],
                [result[ALL_MODULES][NUM_MODULE_STEPS_TRAINED] for result in results],
            )
            learner_0_weights, learner_1_weights = weights()
            check(learner_0_weights, learner_1_weights)

            # A minibatch count passed in by the caller is used by both as-is.
            results = MetricsLogger.peek_results(
                learner_group.update(
                    batches=[fake_batch(256), fake_batch(64)],
                    minibatch_size=32,
                    num_epochs=1,
                    num_total_minibatches=3,
                )
            )
            self.assertEqual(
                [3 * 32, 3 * 32],
                [result[ALL_MODULES][NUM_MODULE_STEPS_TRAINED] for result in results],
            )
        finally:
            learner_group.shutdown()

    def test_learners_drop_modules_only_some_of_them_have_data_for(self):
        """A module only some Learners have data for is dropped (hangs if not)."""
        env = gym.make("CartPole-v1")
        config = BaseTestingAlgorithmConfig().update_from_dict(
            REMOTE_CONFIGS["multi-cpu-ddp"]
        )
        learner_group = config.build_learner_group(env=env)
        learner_group.add_module(
            module_id="p1", module_spec=config.get_rl_module_spec(env=env)
        )

        def weights(module_id):
            return [
                result.get()
                for result in learner_group.foreach_learner(
                    lambda learner: convert_to_numpy(
                        learner.module[module_id].get_state()
                    )
                )
            ]

        try:
            p1_before = weights("p1")
            with_p1, without_p1 = MetricsLogger.peek_results(
                learner_group.update(batches=[fake_batch_with_p1(64), fake_batch(64)])
            )
            # p1 isn't trained, its dropped rows are logged, and p0 trains in sync.
            check(p1_before, weights("p1"))
            self.assertEqual(
                64, with_p1["p1"][LEARNER_MODULE_STEPS_DROPPED_FOR_PEER_LIFETIME]
            )
            self.assertEqual(
                [64, 64],
                [
                    result[ALL_MODULES][NUM_MODULE_STEPS_TRAINED]
                    for result in (with_p1, without_p1)
                ],
            )
            learner_0_weights, learner_1_weights = weights(DEFAULT_MODULE_ID)
            check(learner_0_weights, learner_1_weights)
        finally:
            learner_group.shutdown()

    def test_learners_update_on_a_multi_agent_episode_cut_between_them(self):
        """A cut that leaves an agent without timesteps still updates (hangs if not)."""
        space = gym.spaces.Box(-1.0, 1.0, (4,), np.float32)
        obs = np.zeros(4, np.float32)
        episode = MultiAgentEpisode(
            observation_space={0: space, 1: space},
            action_space={0: gym.spaces.Discrete(2), 1: gym.spaces.Discrete(2)},
            agent_module_ids={0: "p0", 1: "p1"},
        )
        episode.add_env_reset(observations={0: obs, 1: obs})
        # Agent 1 ends at env step 3, where the episode gets cut.
        for t in range(6):
            agents = [0, 1] if t < 3 else [0]
            episode.add_env_step(
                observations={a: obs for a in agents},
                actions={a: 0 for a in agents},
                rewards={a: 1.0 for a in agents},
                terminateds={1: True} if t == 2 else {},
                extra_model_outputs={
                    a: {
                        Columns.ACTION_LOGP: np.float32(np.log(0.5)),
                        Columns.ACTION_DIST_INPUTS: np.zeros(2, np.float32),
                    }
                    for a in agents
                },
            )
        episode.to_numpy()
        config = (
            PPOConfig()
            .update_from_dict(REMOTE_CONFIGS["multi-cpu-ddp"])
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda aid, *args, **kwargs: f"p{aid}",
            )
            .rl_module(
                rl_module_spec=MultiRLModuleSpec(
                    rl_module_specs={"p0": RLModuleSpec(), "p1": RLModuleSpec()}
                )
            )
        )
        learner_group = config.build_learner_group(
            env=MultiAgentCartPole({"num_agents": 2})
        )
        try:
            results = MetricsLogger.peek_results(
                learner_group.update(episodes=[episode])
            )
            with_p1 = results[0]
            # p0 trains on both Learners, p1 (data on one only) on neither.
            self.assertEqual(
                [True, True],
                [result["p0"][NUM_MODULE_STEPS_TRAINED] > 0 for result in results],
            )
            for result in results:
                self.assertNotIn(NUM_MODULE_STEPS_TRAINED, result["p1"])
            self.assertGreater(
                with_p1["p1"][LEARNER_MODULE_STEPS_DROPPED_FOR_PEER_LIFETIME], 0
            )
        finally:
            learner_group.shutdown()

    def test_never_skip_update_in_a_group(self):
        """`never_skip_update` turns a skip into an error, raised on every Learner.

        The Learners must still settle on one number of minibatches: on their own,
        shards of 256 and 64 rows would step 8 and 2 times over 32-row minibatches,
        and the group takes the larger, 8.
        And a Learner handed no data must not raise before the group agreement, or
        its peer waits in that collective forever. Either way the group would hang
        -- as this test then does, rather than fail.
        """
        config = (
            BaseTestingAlgorithmConfig()
            .update_from_dict(REMOTE_CONFIGS["multi-cpu-ddp"])
            .learners(never_skip_update=True)
        )
        learner_group = config.build_learner_group(env=gym.make("CartPole-v1"))
        try:
            results = MetricsLogger.peek_results(
                learner_group.update(
                    batches=[fake_batch(256), fake_batch(64)],
                    minibatch_size=32,
                    num_epochs=1,
                )
            )
            self.assertEqual(
                [8 * 32, 8 * 32],
                [result[ALL_MODULES][NUM_MODULE_STEPS_TRAINED] for result in results],
            )

            # A Learner without data raises, and so does its peer: both leave
            # `update()` right after the agreement, ...
            with self.assertRaisesRegex(Exception, "never_skip_update"):
                learner_group.update(batches=[fake_batch(128), NO_DATA])
            # ... so their collectives still line up, and the next update trains. (The
            # actor manager takes a Learner that raised out of service, as for any
            # error; put both back first.)
            learner_group.foreach_learner(
                lambda learner: None, healthy_only=False, mark_healthy=True
            )
            learner_group.update(batches=[fake_batch(64), fake_batch(64)])
            learner_0_weights, learner_1_weights = [
                result.get()
                for result in learner_group.foreach_learner(
                    lambda learner: convert_to_numpy(
                        learner.module[DEFAULT_MODULE_ID].get_state()
                    )
                )
            ]
            check(learner_0_weights, learner_1_weights)

            # A module only one Learner has data for raises, too.
            learner_group.add_module(
                module_id="p1",
                module_spec=config.get_rl_module_spec(env=gym.make("CartPole-v1")),
            )
            with self.assertRaisesRegex(Exception, "never_skip_update"):
                learner_group.update(batches=[fake_batch_with_p1(64), fake_batch(64)])
        finally:
            learner_group.shutdown()

    def test_a_learner_that_cannot_form_minibatches_fails_the_group(self):
        """A batch that cannot be split into minibatches fails every Learner.

        A minibatch of a sequence batch must hold at least one sequence, so a
        `minibatch_size` below the batch's average sequence length is an error.
        That can hold for one shard and not for another: here, sequences of 20
        timesteps against 5. The first Learner must not raise before the group
        agreement, or its peer waits in that collective forever -- as this test
        then does, rather than fail.
        """
        config = BaseTestingAlgorithmConfig().update_from_dict(
            REMOTE_CONFIGS["multi-cpu-ddp"]
        )
        learner_group = config.build_learner_group(env=gym.make("CartPole-v1"))
        try:
            with self.assertRaisesRegex(Exception, "minibatch"):
                learner_group.update(
                    batches=[
                        fake_batch(40, seq_lens=[20, 20]),
                        fake_batch(20, seq_lens=[5, 5, 5, 5]),
                    ],
                    minibatch_size=10,
                    num_epochs=1,
                )
            # Both Learners left `update()` right after the agreement, so their
            # collectives still line up, and the next update trains. (The actor
            # manager takes a Learner that raised out of service, as for any error;
            # put both back first.)
            learner_group.foreach_learner(
                lambda learner: None, healthy_only=False, mark_healthy=True
            )
            learner_group.update(batches=[fake_batch(64), fake_batch(64)])
            learner_0_weights, learner_1_weights = [
                result.get()
                for result in learner_group.foreach_learner(
                    lambda learner: convert_to_numpy(
                        learner.module[DEFAULT_MODULE_ID].get_state()
                    )
                )
            ]
            check(learner_0_weights, learner_1_weights)
        finally:
            learner_group.shutdown()


class TestLearnerGroupCheckpointRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_restore_from_path_multi_rl_module_and_individual_modules(self):
        """Tests whether MultiRLModule- and single RLModule states can be restored."""
        # this is expanded to more scaling modes on the release ci.
        scaling_modes = ["local-cpu", "multi-gpu-ddp"]

        for scaling_mode in scaling_modes:
            print(f"Testing scaling mode: {scaling_mode}.")
            # env will have agent ids 0 and 1
            env = MultiAgentCartPole({"num_agents": 2})

            config_overrides = REMOTE_CONFIGS.get(scaling_mode) or LOCAL_CONFIGS.get(
                scaling_mode
            )
            config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)
            learner_group = config.build_learner_group(env=env)
            spec = config.get_multi_rl_module_spec(env=env).module_specs[
                DEFAULT_MODULE_ID
            ]
            learner_group.add_module(module_id="0", module_spec=spec)
            learner_group.add_module(module_id="1", module_spec=spec)
            learner_group.remove_module(DEFAULT_MODULE_ID)

            module_0 = spec.build()
            module_1 = spec.build()
            multi_rl_module = MultiRLModule()
            multi_rl_module.add_module(module_id="0", module=module_0)
            multi_rl_module.add_module(module_id="1", module=module_1)

            # Check if we can load just the MultiRLModule.
            with tempfile.TemporaryDirectory() as tmpdir:
                multi_rl_module.save_to_path(tmpdir)
                old_learner_weights = learner_group.get_weights()
                learner_group.restore_from_path(
                    tmpdir,
                    component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE,
                )
                # Check the weights of the module in the learner group are the
                # same as the weights of the newly created MultiRLModule
                check(learner_group.get_weights(), multi_rl_module.get_state())
                learner_group.set_state(
                    {
                        COMPONENT_LEARNER: {COMPONENT_RL_MODULE: old_learner_weights},
                    }
                )
                check(learner_group.get_weights(), old_learner_weights)

            # Check if we can load just single agent RL Modules.
            with tempfile.TemporaryDirectory() as tmpdir:
                module_0.save_to_path(tmpdir)
                with tempfile.TemporaryDirectory() as tmpdir2:
                    temp_module = spec.build()
                    temp_module.save_to_path(tmpdir2)

                    old_learner_weights = learner_group.get_weights()
                    learner_group.restore_from_path(
                        tmpdir,
                        component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/0",
                    )
                    learner_group.restore_from_path(
                        tmpdir2,
                        component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/1",
                    )
                    # check the weights of the module in the learner group are the
                    # same as the weights of the newly created MultiRLModule
                    new_multi_rl_module = MultiRLModule()
                    new_multi_rl_module.add_module(module_id="0", module=module_0)
                    new_multi_rl_module.add_module(module_id="1", module=temp_module)
                    check(learner_group.get_weights(), new_multi_rl_module.get_state())
                    learner_group.set_weights(old_learner_weights)

            # Check if we can first load a MultiRLModule, then a single agent RLModule
            # (within that MultiRLModule). Check that the single agent RL Module is
            # loaded over the matching submodule in the MultiRLModule.
            with tempfile.TemporaryDirectory() as tmpdir:
                module_0 = spec.build()
                multi_rl_module = MultiRLModule()
                multi_rl_module.add_module(module_id="0", module=module_0)
                multi_rl_module.add_module(module_id="1", module=spec.build())
                multi_rl_module.save_to_path(tmpdir)
                with tempfile.TemporaryDirectory() as tmpdir2:
                    module_1 = spec.build()
                    module_1.save_to_path(tmpdir2)
                    learner_group.restore_from_path(
                        tmpdir,
                        component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE,
                    )
                    learner_group.restore_from_path(
                        tmpdir2,
                        component=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/1",
                    )
                    new_multi_rl_module = MultiRLModule()
                    new_multi_rl_module.add_module(module_id="0", module=module_0)
                    new_multi_rl_module.add_module(module_id="1", module=module_1)
                    check(learner_group.get_weights(), new_multi_rl_module.get_state())
            del learner_group


class TestLearnerGroupSaveAndRestoreState(unittest.TestCase):

    FAKE_BATCH = {
        Columns.OBS: np.array(
            [
                [0.1, 0.2, 0.3, 0.4],
                [0.5, 0.6, 0.7, 0.8],
                [0.9, 1.0, 1.1, 1.2],
                [1.3, 1.4, 1.5, 1.6],
            ],
            dtype=np.float32,
        ),
        Columns.NEXT_OBS: np.array(
            [
                [0.1, 0.2, 0.3, 0.4],
                [0.5, 0.6, 0.7, 0.8],
                [0.9, 1.0, 1.1, 1.2],
                [1.3, 1.4, 1.5, 1.6],
            ],
            dtype=np.float32,
        ),
        Columns.ACTIONS: np.array([0, 1, 1, 0]),
        Columns.REWARDS: np.array([1.0, -1.0, 0.5, 0.6], dtype=np.float32),
        Columns.TERMINATEDS: np.array([False, False, True, False]),
        Columns.TRUNCATEDS: np.array([False, False, False, False]),
        Columns.VF_PREDS: np.array([0.5, 0.6, 0.7, 0.8], dtype=np.float32),
        Columns.ACTION_DIST_INPUTS: np.array(
            [[-2.0, 0.5], [-3.0, -0.3], [-0.1, 2.5], [-0.2, 3.5]], dtype=np.float32
        ),
        Columns.ACTION_LOGP: np.array([-0.5, -0.1, -0.2, -0.3], dtype=np.float32),
        Columns.EPS_ID: np.array([0, 0, 0, 0]),
    }

    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_save_to_path_and_restore_from_path(self):
        """Check that saving and loading learner group state works."""
        # this is expanded to more scaling modes on the release ci.
        scaling_modes = ["local-cpu"]  # , "multi-gpu-ddp"]

        for scaling_mode in scaling_modes:
            print(f"Testing scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")

            config_overrides = REMOTE_CONFIGS.get(scaling_mode) or LOCAL_CONFIGS.get(
                scaling_mode
            )
            config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)
            learner_group = config.build_learner_group(env=env)

            # Checkpoint the initial learner state for later comparison.
            initial_learner_checkpoint_dir = tempfile.TemporaryDirectory().name
            learner_group.save_to_path(initial_learner_checkpoint_dir)
            # Test the convenience method `.get_weights()`.
            initial_weights = learner_group.get_weights()

            # Do a single update.
            learner_group.update(episodes=FAKE_EPISODES)
            weights_after_update = learner_group.get_state(
                components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE
            )[COMPONENT_LEARNER][COMPONENT_RL_MODULE]
            # Weights after the update must be different from original ones.
            check(initial_weights, weights_after_update, false=True)

            # Checkpoint the learner state after 1 update for later comparison.
            learner_after_1_update_checkpoint_dir = tempfile.TemporaryDirectory().name
            learner_group.save_to_path(learner_after_1_update_checkpoint_dir)

            # Remove that learner, construct a new one, and load the state of the old
            # learner into the new one.
            learner_group.shutdown()
            del learner_group

            learner_group = config.build_learner_group(env=env)
            learner_group.restore_from_path(learner_after_1_update_checkpoint_dir)

            # Do another update.
            results_2nd_update_with_break = learner_group.update(episodes=FAKE_EPISODES)
            weights_after_2_updates_with_break = learner_group.get_state(
                components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE
            )[COMPONENT_LEARNER][COMPONENT_RL_MODULE]
            learner_group.shutdown()
            del learner_group

            # Construct a new learner group and load the initial state of the learner.
            learner_group = config.build_learner_group(env=env)
            learner_group.restore_from_path(initial_learner_checkpoint_dir)
            weights_after_restore = learner_group.get_state(
                components=COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE
            )[COMPONENT_LEARNER][COMPONENT_RL_MODULE]
            check(initial_weights, weights_after_restore)
            # Perform 2 updates to get to the same state as the previous learners.
            learner_group.update(episodes=FAKE_EPISODES)
            results_2nd_update_without_break = learner_group.update(
                episodes=FAKE_EPISODES
            )
            weights_after_2_updates_without_break = learner_group.get_weights()
            learner_group.shutdown()
            del learner_group

            # Compare the results of the two updates.
            for r1, r2 in zip(
                results_2nd_update_with_break,
                results_2nd_update_without_break,
            ):
                r1[ALL_MODULES].pop(LEARNER_CONNECTOR)
                r2[ALL_MODULES].pop(LEARNER_CONNECTOR)
            check(
                MetricsLogger.peek_results(results_2nd_update_with_break),
                MetricsLogger.peek_results(results_2nd_update_without_break),
                rtol=0.05,
            )
            check(
                weights_after_2_updates_with_break,
                weights_after_2_updates_without_break,
                rtol=0.05,
            )


class TestLearnerGroupAsyncUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_async_update(self):
        """Test that async style updates converge to the same result as sync."""
        scaling_modes = ["multi-gpu-ddp", "multi-cpu-ddp", "remote-gpu"]

        for scaling_mode in scaling_modes:
            print(f"Testing scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")
            config_overrides = REMOTE_CONFIGS[scaling_mode]
            config = BaseTestingAlgorithmConfig().update_from_dict(config_overrides)
            learner_group = config.build_learner_group(env=env)
            timer_sync = _Timer()
            timer_async = _Timer()
            with timer_sync:
                learner_group.update(episodes=FAKE_EPISODES, async_update=False)
            with timer_async:
                result_async = learner_group.update(
                    episodes=FAKE_EPISODES, async_update=True
                )
            # Ideally the first async update will return nothing, and an easy
            # way to check that is if the time for an async update call is faster
            # than the time for a sync update call.
            self.assertLess(timer_async.mean, timer_sync.mean)
            self.assertIsInstance(result_async, list)
            loss = float("inf")
            iter_i = 0
            while True:
                result_async = learner_group.update(
                    episodes=FAKE_EPISODES, async_update=True
                )
                if not result_async:
                    continue
                self.assertIsInstance(result_async, list)
                self.assertIsInstance(result_async[0], dict)
                # Check one async Learner result.
                loss = result_async[0][DEFAULT_MODULE_ID][Learner.TOTAL_LOSS_KEY]
                # The loss is initially around 0.69 (ln2). When it gets to around
                # 0.57 the return of the policy gets to around 100.
                if loss < 0.57:
                    break
                # Compare reported "mean_weight" with actual ones.
                _check_multi_worker_weights(learner_group, result_async)
                iter_i += 1
            learner_group.shutdown()
            self.assertLess(loss, 0.57)


def _check_multi_worker_weights(learner_group, results):
    # Check that module weights are updated across workers and synchronized.
    # for i in range(1, len(results)):

    learner_1_results = results[0]
    for module_id, mod_result in learner_1_results.items():
        if module_id == ALL_MODULES:
            continue
        results = MetricsLogger.peek_results(results)
        reported_mean_weights = np.mean([r[module_id]["mean_weight"] for r in results])

        # Compare the reported mean weights (merged across all Learner workers,
        # which all should have the same weights after updating) with the actual
        # current mean weights.
        parameters = learner_group.get_state(
            components=(
                COMPONENT_LEARNER + "/" + COMPONENT_RL_MODULE + "/" + module_id
            ),
        )[COMPONENT_LEARNER][COMPONENT_RL_MODULE][module_id]
        actual_mean_weights = np.mean([w.mean() for w in parameters.values()])
        check(reported_mean_weights, actual_mean_weights, rtol=0.02)


if __name__ == "__main__":
    import sys

    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
