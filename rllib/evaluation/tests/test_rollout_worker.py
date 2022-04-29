from collections import Counter
import gym
from gym.spaces import Box, Discrete
import numpy as np
import os
import random
import tempfile
import time
import unittest

import ray
from ray.rllib.agents.pg import PGTrainer
from ray.rllib.agents.a3c import A2CTrainer
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.utils import VideoMonitor
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.examples.env.mock_env import (
    MockEnv,
    MockEnv2,
    MockVectorEnv,
    VectorizedMockEnv,
)
from ray.rllib.examples.env.multi_agent import BasicMultiAgent, MultiAgentCartPole
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    MultiAgentBatch,
    SampleBatch,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import NUM_AGENT_STEPS_SAMPLED, NUM_AGENT_STEPS_TRAINED
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.tune.registry import register_env


class MockPolicy(RandomPolicy):
    @override(RandomPolicy)
    def compute_actions(
        self,
        obs_batch,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
        episodes=None,
        explore=None,
        timestep=None,
        **kwargs
    ):
        return np.array([random.choice([0, 1])] * len(obs_batch)), [], {}

    @override(Policy)
    def postprocess_trajectory(self, batch, other_agent_batches=None, episode=None):
        assert episode is not None
        super().postprocess_trajectory(batch, other_agent_batches, episode)
        return compute_advantages(batch, 100.0, 0.9, use_gae=False, use_critic=False)


class BadPolicy(RandomPolicy):
    @override(RandomPolicy)
    def compute_actions(
        self,
        obs_batch,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
        episodes=None,
        explore=None,
        timestep=None,
        **kwargs
    ):
        raise Exception("intentional error")


class FailOnStepEnv(gym.Env):
    def __init__(self):
        self.observation_space = gym.spaces.Discrete(1)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        raise ValueError("kaboom")

    def step(self, action):
        raise ValueError("kaboom")


class TestRolloutWorker(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=5)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_basic(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"), policy_spec=MockPolicy
        )
        batch = ev.sample()
        for key in [
            "obs",
            "actions",
            "rewards",
            "dones",
            "advantages",
            "prev_rewards",
            "prev_actions",
        ]:
            self.assertIn(key, batch)
            self.assertGreater(np.abs(np.mean(batch[key])), 0)

        def to_prev(vec):
            out = np.zeros_like(vec)
            for i, v in enumerate(vec):
                if i + 1 < len(out) and not batch["dones"][i]:
                    out[i + 1] = v
            return out.tolist()

        self.assertEqual(batch["prev_rewards"].tolist(), to_prev(batch["rewards"]))
        self.assertEqual(batch["prev_actions"].tolist(), to_prev(batch["actions"]))
        self.assertGreater(batch["advantages"][0], 1)
        ev.stop()

    def test_batch_ids(self):
        fragment_len = 100
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_spec=MockPolicy,
            rollout_fragment_length=fragment_len,
        )
        batch1 = ev.sample()
        batch2 = ev.sample()
        unroll_ids_1 = set(batch1["unroll_id"])
        unroll_ids_2 = set(batch2["unroll_id"])
        # Assert no overlap of unroll IDs between sample() calls.
        self.assertTrue(not any(uid in unroll_ids_2 for uid in unroll_ids_1))
        # CartPole episodes should be short initially: Expect more than one
        # unroll ID in each batch.
        self.assertTrue(len(unroll_ids_1) > 1)
        self.assertTrue(len(unroll_ids_2) > 1)
        ev.stop()

    def test_global_vars_update(self):
        for fw in framework_iterator(frameworks=("tf2", "tf")):
            agent = A2CTrainer(
                env="CartPole-v0",
                config={
                    "num_workers": 1,
                    # lr = 0.1 - [(0.1 - 0.000001) / 100000] * ts
                    "lr_schedule": [[0, 0.1], [100000, 0.000001]],
                    "framework": fw,
                },
            )
            policy = agent.get_policy()
            for i in range(3):
                result = agent.train()
                print(
                    "{}={}".format(
                        NUM_AGENT_STEPS_TRAINED, result["info"][NUM_AGENT_STEPS_TRAINED]
                    )
                )
                print(
                    "{}={}".format(
                        NUM_AGENT_STEPS_SAMPLED, result["info"][NUM_AGENT_STEPS_SAMPLED]
                    )
                )
                global_timesteps = (
                    policy.global_timestep
                    if fw == "tf"
                    else policy.global_timestep.numpy()
                )
                print("global_timesteps={}".format(global_timesteps))
                expected_lr = 0.1 - ((0.1 - 0.000001) / 100000) * global_timesteps
                lr = policy.cur_lr
                if fw == "tf":
                    lr = policy.get_session().run(lr)
                check(lr, expected_lr, rtol=0.05)
            agent.stop()

    def test_no_step_on_init(self):
        register_env("fail", lambda _: FailOnStepEnv())
        for fw in framework_iterator():
            # We expect this to fail already on Trainer init due
            # to the env sanity check right after env creation (inside
            # RolloutWorker).
            self.assertRaises(
                Exception,
                lambda: PGTrainer(
                    env="fail",
                    config={
                        "num_workers": 2,
                        "framework": fw,
                    },
                ),
            )

    def test_callbacks(self):
        for fw in framework_iterator(frameworks=("torch", "tf")):
            counts = Counter()
            pg = PGTrainer(
                env="CartPole-v0",
                config={
                    "num_workers": 0,
                    "rollout_fragment_length": 50,
                    "train_batch_size": 50,
                    "callbacks": {
                        "on_episode_start": lambda x: counts.update({"start": 1}),
                        "on_episode_step": lambda x: counts.update({"step": 1}),
                        "on_episode_end": lambda x: counts.update({"end": 1}),
                        "on_sample_end": lambda x: counts.update({"sample": 1}),
                    },
                    "framework": fw,
                },
            )
            pg.train()
            pg.train()
            self.assertGreater(counts["sample"], 0)
            self.assertGreater(counts["start"], 0)
            self.assertGreater(counts["end"], 0)
            self.assertGreater(counts["step"], 0)
            pg.stop()

    def test_query_evaluators(self):
        register_env("test", lambda _: gym.make("CartPole-v0"))
        for fw in framework_iterator(frameworks=("torch", "tf")):
            pg = PGTrainer(
                env="test",
                config={
                    "num_workers": 2,
                    "rollout_fragment_length": 5,
                    "num_envs_per_worker": 2,
                    "framework": fw,
                    "create_env_on_driver": True,
                },
            )
            results = pg.workers.foreach_worker(lambda ev: ev.rollout_fragment_length)
            results2 = pg.workers.foreach_worker_with_index(
                lambda ev, i: (i, ev.rollout_fragment_length)
            )
            results3 = pg.workers.foreach_worker(
                lambda ev: ev.foreach_env(lambda env: 1)
            )
            self.assertEqual(results, [10, 10, 10])
            self.assertEqual(results2, [(0, 10), (1, 10), (2, 10)])
            self.assertEqual(results3, [[1, 1], [1, 1], [1, 1]])
            pg.stop()

    def test_action_clipping(self):
        from ray.rllib.examples.env.random_env import RandomEnv

        action_space = gym.spaces.Box(-2.0, 1.0, (3,))

        # Clipping: True (clip between Policy's action_space.low/high).
        ev = RolloutWorker(
            env_creator=lambda _: RandomEnv(
                config=dict(
                    action_space=action_space,
                    max_episode_len=10,
                    p_done=0.0,
                    check_action_bounds=True,
                )
            ),
            policy_spec=RandomPolicy,
            policy_config=dict(
                action_space=action_space,
                ignore_action_bounds=True,
            ),
            normalize_actions=False,
            clip_actions=True,
            batch_mode="complete_episodes",
        )
        sample = ev.sample()
        # Check, whether the action bounds have been breached (expected).
        # We still arrived here b/c we clipped according to the Env's action
        # space.
        self.assertGreater(np.max(sample["actions"]), action_space.high[0])
        self.assertLess(np.min(sample["actions"]), action_space.low[0])
        ev.stop()

        # Clipping: False and RandomPolicy produces invalid actions.
        # Expect Env to complain.
        ev2 = RolloutWorker(
            env_creator=lambda _: RandomEnv(
                config=dict(
                    action_space=action_space,
                    max_episode_len=10,
                    p_done=0.0,
                    check_action_bounds=True,
                )
            ),
            policy_spec=RandomPolicy,
            policy_config=dict(
                action_space=action_space,
                ignore_action_bounds=True,
            ),
            # No normalization (+clipping) and no clipping ->
            # Should lead to Env complaining.
            normalize_actions=False,
            clip_actions=False,
            batch_mode="complete_episodes",
        )
        self.assertRaisesRegex(ValueError, r"Illegal action", ev2.sample)
        ev2.stop()

        # Clipping: False and RandomPolicy produces valid (bounded) actions.
        # Expect "actions" in SampleBatch to be unclipped.
        ev3 = RolloutWorker(
            env_creator=lambda _: RandomEnv(
                config=dict(
                    action_space=action_space,
                    max_episode_len=10,
                    p_done=0.0,
                    check_action_bounds=True,
                )
            ),
            policy_spec=RandomPolicy,
            policy_config=dict(action_space=action_space),
            # Should not be a problem as RandomPolicy abides to bounds.
            normalize_actions=False,
            clip_actions=False,
            batch_mode="complete_episodes",
        )
        sample = ev3.sample()
        self.assertGreater(np.min(sample["actions"]), action_space.low[0])
        self.assertLess(np.max(sample["actions"]), action_space.high[0])
        ev3.stop()

    def test_action_normalization(self):
        from ray.rllib.examples.env.random_env import RandomEnv

        action_space = gym.spaces.Box(0.0001, 0.0002, (5,))

        # Normalize: True (unsquash between Policy's action_space.low/high).
        ev = RolloutWorker(
            env_creator=lambda _: RandomEnv(
                config=dict(
                    action_space=action_space,
                    max_episode_len=10,
                    p_done=0.0,
                    check_action_bounds=True,
                )
            ),
            policy_spec=RandomPolicy,
            policy_config=dict(
                action_space=action_space,
                ignore_action_bounds=True,
            ),
            normalize_actions=True,
            clip_actions=False,
            batch_mode="complete_episodes",
        )
        sample = ev.sample()
        # Check, whether the action bounds have been breached (expected).
        # We still arrived here b/c we unsquashed according to the Env's action
        # space.
        self.assertGreater(np.max(sample["actions"]), action_space.high[0])
        self.assertLess(np.min(sample["actions"]), action_space.low[0])
        ev.stop()

    def test_action_immutability(self):
        from ray.rllib.examples.env.random_env import RandomEnv

        action_space = gym.spaces.Box(0.0001, 0.0002, (5,))

        class ActionMutationEnv(RandomEnv):
            def init(self, config):
                self.test_case = config["test_case"]
                super().__init__(config=config)

            def step(self, action):
                # Ensure that it is called from inside the sampling process.
                import inspect

                curframe = inspect.currentframe()
                called_from_check = any(
                    [
                        frame[3] == "check_gym_environments"
                        for frame in inspect.getouterframes(curframe, 2)
                    ]
                )
                # Check, whether the action is immutable.
                if action.flags.writeable and not called_from_check:
                    self.test_case.assertFalse(
                        action.flags.writeable, "Action is mutable"
                    )
                return super().step(action)

        ev = RolloutWorker(
            env_creator=lambda _: ActionMutationEnv(
                config=dict(
                    test_case=self,
                    action_space=action_space,
                    max_episode_len=10,
                    p_done=0.0,
                    check_action_bounds=True,
                )
            ),
            policy_spec=RandomPolicy,
            policy_config=dict(
                action_space=action_space,
                ignore_action_bounds=True,
            ),
            clip_actions=False,
            batch_mode="complete_episodes",
        )
        ev.sample()
        ev.stop()

    def test_reward_clipping(self):
        # Clipping: True (clip between -1.0 and 1.0).
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv2(episode_length=10),
            policy_spec=MockPolicy,
            clip_rewards=True,
            batch_mode="complete_episodes",
        )
        self.assertEqual(max(ev.sample()["rewards"]), 1)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episode_reward_mean"], 1000)
        ev.stop()

        from ray.rllib.examples.env.random_env import RandomEnv

        # Clipping in certain range (-2.0, 2.0).
        ev2 = RolloutWorker(
            env_creator=lambda _: RandomEnv(
                dict(
                    reward_space=gym.spaces.Box(low=-10, high=10, shape=()),
                    p_done=0.0,
                    max_episode_len=10,
                )
            ),
            policy_spec=MockPolicy,
            clip_rewards=2.0,
            batch_mode="complete_episodes",
        )
        sample = ev2.sample()
        self.assertEqual(max(sample["rewards"]), 2.0)
        self.assertEqual(min(sample["rewards"]), -2.0)
        self.assertLess(np.mean(sample["rewards"]), 0.5)
        self.assertGreater(np.mean(sample["rewards"]), -0.5)
        ev2.stop()

        # Clipping: Off.
        ev2 = RolloutWorker(
            env_creator=lambda _: MockEnv2(episode_length=10),
            policy_spec=MockPolicy,
            clip_rewards=False,
            batch_mode="complete_episodes",
        )
        self.assertEqual(max(ev2.sample()["rewards"]), 100)
        result2 = collect_metrics(ev2, [])
        self.assertEqual(result2["episode_reward_mean"], 1000)
        ev2.stop()

    def test_hard_horizon(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv2(episode_length=10),
            policy_spec=MockPolicy,
            batch_mode="complete_episodes",
            rollout_fragment_length=10,
            episode_horizon=4,
            soft_horizon=False,
        )
        samples = ev.sample()
        # Three logical episodes and correct episode resets (always after 4
        # steps).
        self.assertEqual(len(set(samples["eps_id"])), 3)
        for i in range(4):
            self.assertEqual(np.argmax(samples["obs"][i]), i)
        self.assertEqual(np.argmax(samples["obs"][4]), 0)
        # 3 done values.
        self.assertEqual(sum(samples["dones"]), 3)
        ev.stop()

        # A gym env's max_episode_steps is smaller than Trainer's horizon.
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_spec=MockPolicy,
            batch_mode="complete_episodes",
            rollout_fragment_length=10,
            episode_horizon=6,
            soft_horizon=False,
        )
        samples = ev.sample()
        # 12 steps due to `complete_episodes` batch_mode.
        self.assertEqual(len(samples["eps_id"]), 12)
        # Two logical episodes and correct episode resets (always after 6(!)
        # steps).
        self.assertEqual(len(set(samples["eps_id"])), 2)
        # 2 done values after 6 and 12 steps.
        check(
            samples["dones"],
            [
                False,
                False,
                False,
                False,
                False,
                True,
                False,
                False,
                False,
                False,
                False,
                True,
            ],
        )
        ev.stop()

    def test_soft_horizon(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(episode_length=10),
            policy_spec=MockPolicy,
            batch_mode="complete_episodes",
            rollout_fragment_length=10,
            episode_horizon=4,
            soft_horizon=True,
        )
        samples = ev.sample()
        # three logical episodes
        self.assertEqual(len(set(samples["eps_id"])), 3)
        # only 1 hard done value
        self.assertEqual(sum(samples["dones"]), 1)
        ev.stop()

    def test_metrics(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(episode_length=10),
            policy_spec=MockPolicy,
            batch_mode="complete_episodes",
        )
        remote_ev = RolloutWorker.as_remote().remote(
            env_creator=lambda _: MockEnv(episode_length=10),
            policy_spec=MockPolicy,
            batch_mode="complete_episodes",
        )
        ev.sample()
        ray.get(remote_ev.sample.remote())
        result = collect_metrics(ev, [remote_ev])
        self.assertEqual(result["episodes_this_iter"], 20)
        self.assertEqual(result["episode_reward_mean"], 10)
        ev.stop()

    def test_async(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            sample_async=True,
            policy_spec=MockPolicy,
        )
        batch = ev.sample()
        for key in ["obs", "actions", "rewards", "dones", "advantages"]:
            self.assertIn(key, batch)
        self.assertGreater(batch["advantages"][0], 1)
        ev.stop()

    def test_auto_vectorization(self):
        ev = RolloutWorker(
            env_creator=lambda cfg: MockEnv(episode_length=20, config=cfg),
            policy_spec=MockPolicy,
            batch_mode="truncate_episodes",
            rollout_fragment_length=2,
            num_envs=8,
        )
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 16)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 0)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 16)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 8)
        indices = []
        for env in ev.async_env.vector_env.envs:
            self.assertEqual(env.unwrapped.config.worker_index, 0)
            indices.append(env.unwrapped.config.vector_index)
        self.assertEqual(indices, [0, 1, 2, 3, 4, 5, 6, 7])
        ev.stop()

    def test_batches_larger_when_vectorized(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(episode_length=8),
            policy_spec=MockPolicy,
            batch_mode="truncate_episodes",
            rollout_fragment_length=4,
            num_envs=4,
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 16)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 0)
        batch = ev.sample()
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 4)
        ev.stop()

    def test_vector_env_support(self):
        # Test a vector env that contains 8 actual envs
        # (MockEnv instances).
        ev = RolloutWorker(
            env_creator=(lambda _: VectorizedMockEnv(episode_length=20, num_envs=8)),
            policy_spec=MockPolicy,
            batch_mode="truncate_episodes",
            rollout_fragment_length=10,
        )
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 0)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 8)
        ev.stop()

        # Test a vector env that pretends(!) to contain 4 envs, but actually
        # only has 1 (CartPole).
        ev = RolloutWorker(
            env_creator=(lambda _: MockVectorEnv(20, mocked_num_envs=4)),
            policy_spec=MockPolicy,
            batch_mode="truncate_episodes",
            rollout_fragment_length=10,
        )
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertGreater(result["episodes_this_iter"], 3)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertGreater(result["episodes_this_iter"], 6)
        ev.stop()

    def test_truncate_episodes(self):
        ev_env_steps = RolloutWorker(
            env_creator=lambda _: MockEnv(10),
            policy_spec=MockPolicy,
            rollout_fragment_length=15,
            batch_mode="truncate_episodes",
        )
        batch = ev_env_steps.sample()
        self.assertEqual(batch.count, 15)
        self.assertTrue(isinstance(batch, SampleBatch))
        ev_env_steps.stop()

        action_space = Discrete(2)
        obs_space = Box(float("-inf"), float("inf"), (4,), dtype=np.float32)
        ev_agent_steps = RolloutWorker(
            env_creator=lambda _: MultiAgentCartPole({"num_agents": 4}),
            policy_spec={
                "pol0": (MockPolicy, obs_space, action_space, {}),
                "pol1": (MockPolicy, obs_space, action_space, {}),
            },
            policy_mapping_fn=lambda agent_id, episode, **kwargs: "pol0"
            if agent_id == 0
            else "pol1",
            rollout_fragment_length=301,
            count_steps_by="env_steps",
            batch_mode="truncate_episodes",
        )
        batch = ev_agent_steps.sample()
        self.assertTrue(isinstance(batch, MultiAgentBatch))
        self.assertGreater(batch.agent_steps(), 301)
        self.assertEqual(batch.env_steps(), 301)
        ev_agent_steps.stop()

        ev_agent_steps = RolloutWorker(
            env_creator=lambda _: MultiAgentCartPole({"num_agents": 4}),
            policy_spec={
                "pol0": (MockPolicy, obs_space, action_space, {}),
                "pol1": (MockPolicy, obs_space, action_space, {}),
            },
            policy_mapping_fn=lambda agent_id, episode, **kwargs: "pol0"
            if agent_id == 0
            else "pol1",
            rollout_fragment_length=301,
            count_steps_by="agent_steps",
            batch_mode="truncate_episodes",
        )
        batch = ev_agent_steps.sample()
        self.assertTrue(isinstance(batch, MultiAgentBatch))
        self.assertLess(batch.env_steps(), 301)
        # When counting agent steps, the count may be slightly larger than
        # rollout_fragment_length, b/c we have up to N agents stepping in each
        # env step and we only check, whether we should build after each env
        # step.
        self.assertGreaterEqual(batch.agent_steps(), 301)
        ev_agent_steps.stop()

    def test_complete_episodes(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(10),
            policy_spec=MockPolicy,
            rollout_fragment_length=5,
            batch_mode="complete_episodes",
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 10)
        ev.stop()

    def test_complete_episodes_packing(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(10),
            policy_spec=MockPolicy,
            rollout_fragment_length=15,
            batch_mode="complete_episodes",
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 20)
        self.assertEqual(
            batch["t"].tolist(),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        )
        ev.stop()

    def test_filter_sync(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_spec=MockPolicy,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter",
        )
        time.sleep(2)
        ev.sample()
        filters = ev.get_filters(flush_after=True)
        obs_f = filters[DEFAULT_POLICY_ID]
        self.assertNotEqual(obs_f.rs.n, 0)
        self.assertNotEqual(obs_f.buffer.n, 0)
        ev.stop()

    def test_get_filters(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_spec=MockPolicy,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter",
        )
        self.sample_and_flush(ev)
        filters = ev.get_filters(flush_after=False)
        time.sleep(2)
        filters2 = ev.get_filters(flush_after=False)
        obs_f = filters[DEFAULT_POLICY_ID]
        obs_f2 = filters2[DEFAULT_POLICY_ID]
        self.assertGreaterEqual(obs_f2.rs.n, obs_f.rs.n)
        self.assertGreaterEqual(obs_f2.buffer.n, obs_f.buffer.n)
        ev.stop()

    def test_sync_filter(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_spec=MockPolicy,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter",
        )
        obs_f = self.sample_and_flush(ev)

        # Current State
        filters = ev.get_filters(flush_after=False)
        obs_f = filters[DEFAULT_POLICY_ID]

        self.assertLessEqual(obs_f.buffer.n, 20)

        new_obsf = obs_f.copy()
        new_obsf.rs._n = 100
        ev.sync_filters({DEFAULT_POLICY_ID: new_obsf})
        filters = ev.get_filters(flush_after=False)
        obs_f = filters[DEFAULT_POLICY_ID]
        self.assertGreaterEqual(obs_f.rs.n, 100)
        self.assertLessEqual(obs_f.buffer.n, 20)
        ev.stop()

    def test_extra_python_envs(self):
        extra_envs = {"env_key_1": "env_value_1", "env_key_2": "env_value_2"}
        self.assertFalse("env_key_1" in os.environ)
        self.assertFalse("env_key_2" in os.environ)
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(10),
            policy_spec=MockPolicy,
            extra_python_environs=extra_envs,
        )
        self.assertTrue("env_key_1" in os.environ)
        self.assertTrue("env_key_2" in os.environ)
        ev.stop()

        # reset to original
        del os.environ["env_key_1"]
        del os.environ["env_key_2"]

    def test_no_env_seed(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockVectorEnv(20, mocked_num_envs=8),
            policy_spec=MockPolicy,
            seed=1,
        )
        assert not hasattr(ev.env, "seed")
        ev.stop()

    def test_multi_env_seed(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv2(100),
            num_envs=3,
            policy_spec=MockPolicy,
            seed=1,
        )
        # Make sure we can properly sample from the wrapped env.
        ev.sample()
        # Make sure all environments got a different deterministic seed.
        seeds = ev.foreach_env(lambda env: env.rng_seed)
        self.assertEqual(seeds, [1, 2, 3])
        ev.stop()

    def test_wrap_multi_agent_env(self):
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(10),
            policy_spec=MockPolicy,
            policy_config={
                "in_evaluation": False,
            },
            record_env=tempfile.gettempdir(),
        )
        # Make sure we can properly sample from the wrapped env.
        ev.sample()
        # Make sure the resulting environment is indeed still an
        # instance of MultiAgentEnv and VideoMonitor.
        self.assertTrue(isinstance(ev.env.unwrapped, MultiAgentEnv))
        self.assertTrue(isinstance(ev.env, gym.Env))
        self.assertTrue(isinstance(ev.env, VideoMonitor))
        ev.stop()

    def test_no_training(self):
        class NoTrainingEnv(MockEnv):
            def __init__(self, episode_length, training_enabled):
                super(NoTrainingEnv, self).__init__(episode_length)
                self.training_enabled = training_enabled

            def step(self, action):
                obs, rew, done, info = super(NoTrainingEnv, self).step(action)
                return (
                    obs,
                    rew,
                    done,
                    {**info, "training_enabled": self.training_enabled},
                )

        ev = RolloutWorker(
            env_creator=lambda _: NoTrainingEnv(10, True),
            policy_spec=MockPolicy,
            rollout_fragment_length=5,
            batch_mode="complete_episodes",
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 10)
        self.assertEqual(len(batch["obs"]), 10)
        ev.stop()

        ev = RolloutWorker(
            env_creator=lambda _: NoTrainingEnv(10, False),
            policy_spec=MockPolicy,
            rollout_fragment_length=5,
            batch_mode="complete_episodes",
        )
        batch = ev.sample()
        self.assertTrue(isinstance(batch, MultiAgentBatch))
        self.assertEqual(len(batch.policy_batches), 0)
        ev.stop()

    def sample_and_flush(self, ev):
        time.sleep(2)
        ev.sample()
        filters = ev.get_filters(flush_after=True)
        obs_f = filters[DEFAULT_POLICY_ID]
        self.assertNotEqual(obs_f.rs.n, 0)
        self.assertNotEqual(obs_f.buffer.n, 0)
        return obs_f


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
