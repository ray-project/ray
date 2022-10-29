import gym
import numpy as np
import random
import unittest

import ray
from ray.tune.registry import register_env
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn.dqn_tf_policy import DQNTFPolicy
from ray.rllib.algorithms.pg import PG
from ray.rllib.env.multi_agent_env import make_multi_agent, MultiAgentEnvWrapper
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.tests.test_rollout_worker import MockPolicy
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.examples.env.multi_agent import (
    MultiAgentCartPole,
    BasicMultiAgent,
    EarlyDoneMultiAgent,
    FlexAgentsMultiAgent,
    RoundRobinMultiAgent,
)
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.tests.test_nested_observation_spaces import NestedMultiAgentEnv
from ray.rllib.utils.numpy import one_hot
from ray.rllib.utils.test_utils import check


class TestMultiAgentEnv(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_basic_mock(self):
        env = BasicMultiAgent(4)
        obs = env.reset()
        self.assertEqual(obs, {0: 0, 1: 0, 2: 0, 3: 0})
        for _ in range(24):
            obs, rew, done, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
            self.assertEqual(obs, {0: 0, 1: 0, 2: 0, 3: 0})
            self.assertEqual(rew, {0: 1, 1: 1, 2: 1, 3: 1})
            self.assertEqual(
                done, {0: False, 1: False, 2: False, 3: False, "__all__": False}
            )
        obs, rew, done, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
        self.assertEqual(done, {0: True, 1: True, 2: True, 3: True, "__all__": True})

    def test_round_robin_mock(self):
        env = RoundRobinMultiAgent(2)
        obs = env.reset()
        self.assertEqual(obs, {0: 0})
        for _ in range(5):
            obs, rew, done, info = env.step({0: 0})
            self.assertEqual(obs, {1: 0})
            self.assertEqual(done["__all__"], False)
            obs, rew, done, info = env.step({1: 0})
            self.assertEqual(obs, {0: 0})
            self.assertEqual(done["__all__"], False)
        obs, rew, done, info = env.step({0: 0})
        self.assertEqual(done["__all__"], True)

    def test_no_reset_until_poll(self):
        env = MultiAgentEnvWrapper(lambda v: BasicMultiAgent(2), [], 1)
        self.assertFalse(env.get_sub_environments()[0].resetted)
        env.poll()
        self.assertTrue(env.get_sub_environments()[0].resetted)

    def test_vectorize_basic(self):
        env = MultiAgentEnvWrapper(lambda v: BasicMultiAgent(2), [], 2)
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        self.assertEqual(rew, {0: {}, 1: {}})
        self.assertEqual(
            dones,
            {
                0: {"__all__": False},
                1: {"__all__": False},
            },
        )
        for _ in range(24):
            env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            obs, rew, dones, _, _ = env.poll()
            self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            self.assertEqual(rew, {0: {0: 1, 1: 1}, 1: {0: 1, 1: 1}})
            self.assertEqual(
                dones,
                {
                    0: {0: False, 1: False, "__all__": False},
                    1: {0: False, 1: False, "__all__": False},
                },
            )
        env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(
            dones,
            {
                0: {0: True, 1: True, "__all__": True},
                1: {0: True, 1: True, "__all__": True},
            },
        )

        # Reset processing
        self.assertRaises(
            ValueError, lambda: env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        )
        self.assertEqual(env.try_reset(0), {0: {0: 0, 1: 0}})
        self.assertEqual(env.try_reset(1), {1: {0: 0, 1: 0}})
        env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        self.assertEqual(rew, {0: {0: 1, 1: 1}, 1: {0: 1, 1: 1}})
        self.assertEqual(
            dones,
            {
                0: {0: False, 1: False, "__all__": False},
                1: {0: False, 1: False, "__all__": False},
            },
        )

    def test_vectorize_round_robin(self):
        env = MultiAgentEnvWrapper(lambda v: RoundRobinMultiAgent(2), [], 2)
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0}, 1: {0: 0}})
        self.assertEqual(rew, {0: {}, 1: {}})
        env.send_actions({0: {0: 0}, 1: {0: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {1: 0}, 1: {1: 0}})
        env.send_actions({0: {1: 0}, 1: {1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0}, 1: {0: 0}})

    def test_multi_agent_sample(self):
        def policy_mapping_fn(agent_id, episode, worker, **kwargs):
            return "p{}".format(agent_id % 2)

        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .rollouts(rollout_fragment_length=50, num_rollout_workers=0)
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=policy_mapping_fn,
            ),
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 50)
        self.assertEqual(batch.policy_batches["p0"].count, 150)
        self.assertEqual(batch.policy_batches["p1"].count, 100)
        self.assertEqual(batch.policy_batches["p0"]["t"].tolist(), list(range(25)) * 6)

    def test_multi_agent_sample_sync_remote(self):
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            default_policy_class=MockPolicy,
            # This signature will raise a soft-deprecation warning due
            # to the new signature we are using (agent_id, episode, **kwargs),
            # but should not break this test.
            config=AlgorithmConfig()
            .rollouts(
                rollout_fragment_length=50,
                num_rollout_workers=0,
                num_envs_per_worker=4,
                remote_worker_envs=True,
                remote_env_batch_wait_ms=99999999,
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=(lambda agent_id: "p{}".format(agent_id % 2)),
            ),
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 200)

    def test_multi_agent_sample_async_remote(self):
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .rollouts(
                rollout_fragment_length=50,
                num_rollout_workers=0,
                num_envs_per_worker=4,
                remote_worker_envs=True,
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=(lambda aid, **kwargs: "p{}".format(aid % 2)),
            ),
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 200)

    def test_multi_agent_sample_with_horizon(self):
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .rollouts(
                rollout_fragment_length=50,
                num_rollout_workers=0,
                horizon=10,  # test with episode horizon set
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=(lambda aid, **kwarg: "p{}".format(aid % 2)),
            ),
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 50)

    def test_sample_from_early_done_env(self):
        ev = RolloutWorker(
            env_creator=lambda _: EarlyDoneMultiAgent(),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .rollouts(
                rollout_fragment_length=1,
                num_rollout_workers=0,
                batch_mode="complete_episodes",
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=(lambda aid, **kwargs: "p{}".format(aid % 2)),
            ),
        )
        # This used to raise an Error due to the EarlyDoneMultiAgent
        # terminating at e.g. agent0 w/o publishing the observation for
        # agent1 anymore. This limitation is fixed and an env may
        # terminate at any time (as well as return rewards for any agent
        # at any time, even when that agent doesn't have an obs returned
        # in the same call to `step()`).
        ma_batch = ev.sample()
        # Make sure that agents took the correct (alternating timesteps)
        # path. Except for the last timestep, where both agents got
        # terminated.
        ag0_ts = ma_batch.policy_batches["p0"]["t"]
        ag1_ts = ma_batch.policy_batches["p1"]["t"]
        self.assertTrue(np.all(np.abs(ag0_ts[:-1] - ag1_ts[:-1]) == 1.0))
        self.assertTrue(ag0_ts[-1] == ag1_ts[-1])

    def test_multi_agent_with_flex_agents(self):
        register_env(
            "flex_agents_multi_agent_cartpole", lambda _: FlexAgentsMultiAgent()
        )
        pg = PG(
            env="flex_agents_multi_agent_cartpole",
            config={
                "num_workers": 0,
                "framework": "tf",
            },
        )
        for i in range(10):
            result = pg.train()
            print(
                "Iteration {}, reward {}, timesteps {}".format(
                    i, result["episode_reward_mean"], result["timesteps_total"]
                )
            )

    def test_multi_agent_sample_round_robin(self):
        ev = RolloutWorker(
            env_creator=lambda _: RoundRobinMultiAgent(5, increment_obs=True),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .rollouts(
                rollout_fragment_length=50,
                num_rollout_workers=0,
            )
            .multi_agent(
                policies={"p0"},
                policy_mapping_fn=lambda agent_id, episode, **kwargs: "p0",
            ),
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 50)
        # since we round robin introduce agents into the env, some of the env
        # steps don't count as proper transitions
        self.assertEqual(batch.policy_batches["p0"].count, 42)
        check(
            batch.policy_batches["p0"]["obs"][:10],
            one_hot(np.array([0, 1, 2, 3, 4] * 2), 10),
        )
        check(
            batch.policy_batches["p0"]["new_obs"][:10],
            one_hot(np.array([1, 2, 3, 4, 5] * 2), 10),
        )
        self.assertEqual(
            batch.policy_batches["p0"]["rewards"].tolist()[:10],
            [100, 100, 100, 100, 0] * 2,
        )
        self.assertEqual(
            batch.policy_batches["p0"]["dones"].tolist()[:10],
            [False, False, False, False, True] * 2,
        )
        self.assertEqual(
            batch.policy_batches["p0"]["t"].tolist()[:10],
            [4, 9, 14, 19, 24, 5, 10, 15, 20, 25],
        )

    def test_custom_rnn_state_values(self):
        h = {"some": {"arbitrary": "structure", "here": [1, 2, 3]}}

        class StatefulPolicy(RandomPolicy):
            def compute_actions(
                self,
                obs_batch,
                state_batches=None,
                prev_action_batch=None,
                prev_reward_batch=None,
                episodes=None,
                explore=True,
                timestep=None,
                **kwargs
            ):
                return [0] * len(obs_batch), [[h] * len(obs_batch)], {}

            def get_initial_state(self):
                return [{}]  # empty dict

        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v1"),
            default_policy_class=StatefulPolicy,
            config=(
                AlgorithmConfig().rollouts(
                    rollout_fragment_length=5, num_rollout_workers=0
                )
                # Force `state_in_0` to be repeated every ts in the collected batch
                # (even though we don't even have a model that would care about this).
                .training(model={"max_seq_len": 1})
            ),
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 5)
        self.assertEqual(batch["state_in_0"][0], {})
        self.assertEqual(batch["state_out_0"][0], h)
        for i in range(1, 5):
            self.assertEqual(batch["state_in_0"][i], h)
            self.assertEqual(batch["state_out_0"][i], h)

    def test_returning_model_based_rollouts_data(self):
        class ModelBasedPolicy(DQNTFPolicy):
            def compute_actions_from_input_dict(
                self, input_dict, explore=None, timestep=None, episodes=None, **kwargs
            ):
                obs_batch = input_dict["obs"]
                # In policy loss initialization phase, no episodes are passed
                # in.
                if episodes is not None:
                    # Pretend we did a model-based rollout and want to return
                    # the extra trajectory.
                    env_id = episodes[0].env_id
                    fake_eps = Episode(
                        episodes[0].policy_map,
                        episodes[0].policy_mapping_fn,
                        lambda: None,
                        lambda x: None,
                        env_id,
                    )
                    builder = get_global_worker().sampler.sample_collector
                    agent_id = "extra_0"
                    policy_id = "p1"  # use p1 so we can easily check it
                    builder.add_init_obs(
                        episode=fake_eps,
                        agent_id=agent_id,
                        policy_id=policy_id,
                        env_id=env_id,
                        init_obs=obs_batch[0],
                    )
                    for t in range(4):
                        builder.add_action_reward_next_obs(
                            episode_id=fake_eps.episode_id,
                            agent_id=agent_id,
                            env_id=env_id,
                            policy_id=policy_id,
                            agent_done=t == 3,
                            values=dict(
                                t=t,
                                actions=0,
                                rewards=0,
                                dones=t == 3,
                                infos={},
                                new_obs=obs_batch[0],
                            ),
                        )
                    batch = builder.postprocess_episode(episode=fake_eps, build=True)
                    episodes[0].add_extra_batch(batch)

                # Just return zeros for actions
                return [0] * len(obs_batch), [], {}

        ev = RolloutWorker(
            env_creator=lambda _: MultiAgentCartPole({"num_agents": 2}),
            default_policy_class=ModelBasedPolicy,
            config=AlgorithmConfig()
            .rollouts(
                rollout_fragment_length=5,
                num_rollout_workers=0,
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda agent_id, episode, **kwargs: "p0",
            ),
        )
        batch = ev.sample()
        # 5 environment steps (rollout_fragment_length).
        self.assertEqual(batch.count, 5)
        # 10 agent steps for p0: 2 agents, both using p0 as their policy.
        self.assertEqual(batch.policy_batches["p0"].count, 10)
        # 20 agent steps for p1: Each time both(!) agents takes 1 step,
        # p1 takes 4: 5 (rollout-fragment length) * 4 = 20
        self.assertEqual(batch.policy_batches["p1"].count, 20)

    def test_train_multi_agent_cartpole_single_policy(self):
        n = 10
        register_env(
            "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": n})
        )
        pg = PG(
            env="multi_agent_cartpole",
            config={
                "num_workers": 0,
                "framework": "tf",
            },
        )
        for i in range(50):
            result = pg.train()
            print(
                "Iteration {}, reward {}, timesteps {}".format(
                    i, result["episode_reward_mean"], result["timesteps_total"]
                )
            )
            if result["episode_reward_mean"] >= 50 * n:
                return
        raise Exception("failed to improve reward")

    def test_train_multi_agent_cartpole_multi_policy(self):
        n = 10
        register_env(
            "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": n})
        )

        def gen_policy():
            config = {
                "gamma": random.choice([0.5, 0.8, 0.9, 0.95, 0.99]),
                "n_step": random.choice([1, 2, 3, 4, 5]),
            }
            return PolicySpec(config=config)

        pg = PG(
            env="multi_agent_cartpole",
            config={
                "num_workers": 0,
                "multiagent": {
                    "policies": {
                        "policy_1": gen_policy(),
                        "policy_2": gen_policy(),
                    },
                    "policy_mapping_fn": lambda aid, **kwargs: "policy_1",
                },
                "framework": "tf",
            },
        )

        # Just check that it runs without crashing
        for i in range(10):
            result = pg.train()
            print(
                "Iteration {}, reward {}, timesteps {}".format(
                    i, result["episode_reward_mean"], result["timesteps_total"]
                )
            )
        self.assertTrue(
            pg.compute_single_action([0, 0, 0, 0], policy_id="policy_1") in [0, 1]
        )
        self.assertTrue(
            pg.compute_single_action([0, 0, 0, 0], policy_id="policy_2") in [0, 1]
        )
        self.assertRaisesRegex(
            KeyError,
            "not found in PolicyMap",
            lambda: pg.compute_single_action([0, 0, 0, 0], policy_id="policy_3"),
        )

    def test_space_in_preferred_format(self):
        env = NestedMultiAgentEnv()
        action_spaces_in_preferred_format = (
            env._check_if_action_space_maps_agent_id_to_sub_space()
        )
        obs_space_in_preferred_format = (
            env._check_if_obs_space_maps_agent_id_to_sub_space()
        )
        spaces_in_preferred_format = env._check_if_space_maps_agent_id_to_sub_space()
        assert action_spaces_in_preferred_format, "Space is not in preferred format."
        assert obs_space_in_preferred_format, "Space is not in preferred format."
        assert spaces_in_preferred_format, "Space is not in preferred format."
        env2 = make_multi_agent("CartPole-v1")()
        spaces_in_preferred_format = env2._check_if_space_maps_agent_id_to_sub_space()
        action_spaces_in_preferred_format = (
            env2._check_if_action_space_maps_agent_id_to_sub_space()
        )
        obs_space_in_preferred_format = (
            env2._check_if_obs_space_maps_agent_id_to_sub_space()
        )
        assert (
            not action_spaces_in_preferred_format
        ), "Space should not be in preferred format but is."
        assert (
            not obs_space_in_preferred_format
        ), "Space should not be in preferred format but is."
        assert (
            not spaces_in_preferred_format
        ), "Space should not be in preferred format but is."

    def test_spaces_sample_contain_in_preferred_format(self):
        env = NestedMultiAgentEnv()
        # this environment has spaces that are in the preferred format
        # for multi-agent environments where the spaces are dict spaces
        # mapping agent-ids to sub-spaces
        obs = env.observation_space_sample()
        assert env.observation_space_contains(
            obs
        ), "Observation space does not contain obs"

        action = env.action_space_sample()
        assert env.action_space_contains(action), "Action space does not contain action"

    def test_spaces_sample_contain_not_in_preferred_format(self):
        env = make_multi_agent("CartPole-v1")({"num_agents": 2})
        # this environment has spaces that are not in the preferred format
        # for multi-agent environments where the spaces not in the preferred
        # format, users must override the observation_space_contains,
        # action_space_contains observation_space_sample,
        # and action_space_sample methods in order to do proper checks
        obs = env.observation_space_sample()
        assert env.observation_space_contains(
            obs
        ), "Observation space does not contain obs"
        action = env.action_space_sample()
        assert env.action_space_contains(action), "Action space does not contain action"


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
