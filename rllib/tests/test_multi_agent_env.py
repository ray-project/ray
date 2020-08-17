import gym
import random
import unittest

import ray
from ray.tune.registry import register_env
from ray.rllib.agents.pg import PGTrainer
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole, \
    BasicMultiAgent, EarlyDoneMultiAgent, RoundRobinMultiAgent
from ray.rllib.tests.test_rollout_worker import MockPolicy
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.env.base_env import _MultiAgentEnvToBaseEnv


def one_hot(i, n):
    out = [0.0] * n
    out[i] = 1.0
    return out


class TestMultiAgentEnv(unittest.TestCase):
    def setUp(self) -> None:
        ray.init(num_cpus=4)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_basic_mock(self):
        env = BasicMultiAgent(4)
        obs = env.reset()
        self.assertEqual(obs, {0: 0, 1: 0, 2: 0, 3: 0})
        for _ in range(24):
            obs, rew, done, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
            self.assertEqual(obs, {0: 0, 1: 0, 2: 0, 3: 0})
            self.assertEqual(rew, {0: 1, 1: 1, 2: 1, 3: 1})
            self.assertEqual(done, {
                0: False,
                1: False,
                2: False,
                3: False,
                "__all__": False
            })
        obs, rew, done, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
        self.assertEqual(done, {
            0: True,
            1: True,
            2: True,
            3: True,
            "__all__": True
        })

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
        env = _MultiAgentEnvToBaseEnv(lambda v: BasicMultiAgent(2), [], 1)
        self.assertFalse(env.get_unwrapped()[0].resetted)
        env.poll()
        self.assertTrue(env.get_unwrapped()[0].resetted)

    def test_vectorize_basic(self):
        env = _MultiAgentEnvToBaseEnv(lambda v: BasicMultiAgent(2), [], 2)
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        self.assertEqual(rew, {0: {0: None, 1: None}, 1: {0: None, 1: None}})
        self.assertEqual(
            dones, {
                0: {
                    0: False,
                    1: False,
                    "__all__": False
                },
                1: {
                    0: False,
                    1: False,
                    "__all__": False
                }
            })
        for _ in range(24):
            env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            obs, rew, dones, _, _ = env.poll()
            self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            self.assertEqual(rew, {0: {0: 1, 1: 1}, 1: {0: 1, 1: 1}})
            self.assertEqual(
                dones, {
                    0: {
                        0: False,
                        1: False,
                        "__all__": False
                    },
                    1: {
                        0: False,
                        1: False,
                        "__all__": False
                    }
                })
        env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(
            dones, {
                0: {
                    0: True,
                    1: True,
                    "__all__": True
                },
                1: {
                    0: True,
                    1: True,
                    "__all__": True
                }
            })

        # Reset processing
        self.assertRaises(
            ValueError, lambda: env.send_actions({
                0: {
                    0: 0,
                    1: 0
                },
                1: {
                    0: 0,
                    1: 0
                }
            }))
        self.assertEqual(env.try_reset(0), {0: 0, 1: 0})
        self.assertEqual(env.try_reset(1), {0: 0, 1: 0})
        env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        self.assertEqual(rew, {0: {0: 1, 1: 1}, 1: {0: 1, 1: 1}})
        self.assertEqual(
            dones, {
                0: {
                    0: False,
                    1: False,
                    "__all__": False
                },
                1: {
                    0: False,
                    1: False,
                    "__all__": False
                }
            })

    def test_vectorize_round_robin(self):
        env = _MultiAgentEnvToBaseEnv(lambda v: RoundRobinMultiAgent(2), [], 2)
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0}, 1: {0: 0}})
        self.assertEqual(rew, {0: {0: None}, 1: {0: None}})
        env.send_actions({0: {0: 0}, 1: {0: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {1: 0}, 1: {1: 0}})
        env.send_actions({0: {1: 0}, 1: {1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0}, 1: {0: 0}})

    def test_multi_agent_sample(self):
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            policy={
                "p0": (MockPolicy, obs_space, act_space, {}),
                "p1": (MockPolicy, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p{}".format(agent_id % 2),
            rollout_fragment_length=50)
        batch = ev.sample()
        self.assertEqual(batch.count, 50)
        self.assertEqual(batch.policy_batches["p0"].count, 150)
        self.assertEqual(batch.policy_batches["p1"].count, 100)
        self.assertEqual(batch.policy_batches["p0"]["t"].tolist(),
                         list(range(25)) * 6)

    def test_multi_agent_sample_sync_remote(self):
        # Allow to be run via Unittest.
        ray.init(num_cpus=4, ignore_reinit_error=True)
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            policy={
                "p0": (MockPolicy, obs_space, act_space, {}),
                "p1": (MockPolicy, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p{}".format(agent_id % 2),
            rollout_fragment_length=50,
            num_envs=4,
            remote_worker_envs=True,
            remote_env_batch_wait_ms=99999999)
        batch = ev.sample()
        self.assertEqual(batch.count, 200)

    def test_multi_agent_sample_async_remote(self):
        # Allow to be run via Unittest.
        ray.init(num_cpus=4, ignore_reinit_error=True)
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            policy={
                "p0": (MockPolicy, obs_space, act_space, {}),
                "p1": (MockPolicy, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p{}".format(agent_id % 2),
            rollout_fragment_length=50,
            num_envs=4,
            remote_worker_envs=True)
        batch = ev.sample()
        self.assertEqual(batch.count, 200)

    def test_multi_agent_sample_with_horizon(self):
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = RolloutWorker(
            env_creator=lambda _: BasicMultiAgent(5),
            policy={
                "p0": (MockPolicy, obs_space, act_space, {}),
                "p1": (MockPolicy, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p{}".format(agent_id % 2),
            episode_horizon=10,  # test with episode horizon set
            rollout_fragment_length=50)
        batch = ev.sample()
        self.assertEqual(batch.count, 50)

    def test_sample_from_early_done_env(self):
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = RolloutWorker(
            env_creator=lambda _: EarlyDoneMultiAgent(),
            policy={
                "p0": (MockPolicy, obs_space, act_space, {}),
                "p1": (MockPolicy, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p{}".format(agent_id % 2),
            batch_mode="complete_episodes",
            rollout_fragment_length=1)
        self.assertRaisesRegexp(ValueError,
                                ".*don't have a last observation.*",
                                lambda: ev.sample())

    def test_multi_agent_sample_round_robin(self):
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(10)
        ev = RolloutWorker(
            env_creator=lambda _: RoundRobinMultiAgent(5, increment_obs=True),
            policy={
                "p0": (MockPolicy, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p0",
            rollout_fragment_length=50)
        batch = ev.sample()
        self.assertEqual(batch.count, 50)
        # since we round robin introduce agents into the env, some of the env
        # steps don't count as proper transitions
        self.assertEqual(batch.policy_batches["p0"].count, 42)
        self.assertEqual(batch.policy_batches["p0"]["obs"].tolist()[:10], [
            one_hot(0, 10),
            one_hot(1, 10),
            one_hot(2, 10),
            one_hot(3, 10),
            one_hot(4, 10),
        ] * 2)
        self.assertEqual(batch.policy_batches["p0"]["new_obs"].tolist()[:10], [
            one_hot(1, 10),
            one_hot(2, 10),
            one_hot(3, 10),
            one_hot(4, 10),
            one_hot(5, 10),
        ] * 2)
        self.assertEqual(batch.policy_batches["p0"]["rewards"].tolist()[:10],
                         [100, 100, 100, 100, 0] * 2)
        self.assertEqual(batch.policy_batches["p0"]["dones"].tolist()[:10],
                         [False, False, False, False, True] * 2)
        self.assertEqual(batch.policy_batches["p0"]["t"].tolist()[:10],
                         [4, 9, 14, 19, 24, 5, 10, 15, 20, 25])

    def test_custom_rnn_state_values(self):
        h = {"some": {"arbitrary": "structure", "here": [1, 2, 3]}}

        class StatefulPolicy(RandomPolicy):
            def compute_actions(self,
                                obs_batch,
                                state_batches=None,
                                prev_action_batch=None,
                                prev_reward_batch=None,
                                episodes=None,
                                explore=True,
                                timestep=None,
                                **kwargs):
                return [0] * len(obs_batch), [[h] * len(obs_batch)], {}

            def get_initial_state(self):
                return [{}]  # empty dict

        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy=StatefulPolicy,
            rollout_fragment_length=5)
        batch = ev.sample()
        self.assertEqual(batch.count, 5)
        self.assertEqual(batch["state_in_0"][0], {})
        self.assertEqual(batch["state_out_0"][0], h)
        self.assertEqual(batch["state_in_0"][1], h)
        self.assertEqual(batch["state_out_0"][1], h)

    def test_returning_model_based_rollouts_data(self):
        class ModelBasedPolicy(PGTFPolicy):
            def compute_actions(self,
                                obs_batch,
                                state_batches,
                                prev_action_batch=None,
                                prev_reward_batch=None,
                                episodes=None,
                                **kwargs):
                # Pretend we did a model-based rollout and want to return
                # the extra trajectory.
                builder = episodes[0].new_batch_builder()
                rollout_id = random.randint(0, 10000)
                for t in range(5):
                    builder.add_values(
                        agent_id="extra_0",
                        policy_id="p1",  # use p1 so we can easily check it
                        t=t,
                        eps_id=rollout_id,  # new id for each rollout
                        obs=obs_batch[0],
                        actions=0,
                        rewards=0,
                        dones=t == 4,
                        infos={},
                        new_obs=obs_batch[0])
                batch = builder.build_and_reset(episode=None)
                episodes[0].add_extra_batch(batch)

                # Just return zeros for actions
                return [0] * len(obs_batch), [], {}

        single_env = gym.make("CartPole-v0")
        obs_space = single_env.observation_space
        act_space = single_env.action_space
        ev = RolloutWorker(
            env_creator=lambda _: MultiAgentCartPole({"num_agents": 2}),
            policy={
                "p0": (ModelBasedPolicy, obs_space, act_space, {}),
                "p1": (ModelBasedPolicy, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p0",
            rollout_fragment_length=5)
        batch = ev.sample()
        self.assertEqual(batch.count, 5)
        self.assertEqual(batch.policy_batches["p0"].count, 10)
        self.assertEqual(batch.policy_batches["p1"].count, 25)

    def test_train_multi_agent_cartpole_single_policy(self):
        n = 10
        register_env("multi_agent_cartpole",
                     lambda _: MultiAgentCartPole({"num_agents": n}))
        pg = PGTrainer(
            env="multi_agent_cartpole",
            config={
                "num_workers": 0,
                "framework": "tf",
            })
        for i in range(50):
            result = pg.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result["episode_reward_mean"], result["timesteps_total"]))
            if result["episode_reward_mean"] >= 50 * n:
                return
        raise Exception("failed to improve reward")

    def test_train_multi_agent_cartpole_multi_policy(self):
        n = 10
        register_env("multi_agent_cartpole",
                     lambda _: MultiAgentCartPole({"num_agents": n}))
        single_env = gym.make("CartPole-v0")

        def gen_policy():
            config = {
                "gamma": random.choice([0.5, 0.8, 0.9, 0.95, 0.99]),
                "n_step": random.choice([1, 2, 3, 4, 5]),
            }
            obs_space = single_env.observation_space
            act_space = single_env.action_space
            return (None, obs_space, act_space, config)

        pg = PGTrainer(
            env="multi_agent_cartpole",
            config={
                "num_workers": 0,
                "multiagent": {
                    "policies": {
                        "policy_1": gen_policy(),
                        "policy_2": gen_policy(),
                    },
                    "policy_mapping_fn": lambda agent_id: "policy_1",
                },
                "framework": "tf",
            })

        # Just check that it runs without crashing
        for i in range(10):
            result = pg.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result["episode_reward_mean"], result["timesteps_total"]))
        self.assertTrue(
            pg.compute_action([0, 0, 0, 0], policy_id="policy_1") in [0, 1])
        self.assertTrue(
            pg.compute_action([0, 0, 0, 0], policy_id="policy_2") in [0, 1])
        self.assertRaises(
            KeyError,
            lambda: pg.compute_action([0, 0, 0, 0], policy_id="policy_3"))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
