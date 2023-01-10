import gymnasium as gym
import numpy as np
import unittest

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.connectors.connector import ActionConnector, ConnectorContext
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.examples.env.debug_counter_env import DebugCounterEnv
from ray.rllib.examples.env.multi_agent import BasicMultiAgent, GuessTheNumberGame
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.policy.sample_batch import convert_ma_batch_to_sample_batch

from ray.rllib.utils.test_utils import check


gym.register("basic_multiagent", lambda: BasicMultiAgent(2))


class TestEnvRunnerV2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

        # When dealing with two policies in these tests, simply alternate between the 2
        # policies to make sure we have data for inference for both policies for each
        # step.
        class AlternatePolicyMapper:
            def __init__(self):
                self.policies = ["one", "two"]
                self.next = 0

            def map(self):
                p = self.policies[self.next]
                self.next = 1 - self.next
                return p

        cls.mapper = AlternatePolicyMapper()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_sample_batch_rollout_single_agent_env(self):
        config = (
            PPOConfig()
            .framework("torch")
            .training(
                # Specifically ask for a batch of 200 samples.
                train_batch_size=200,
            )
            .rollouts(
                num_envs_per_worker=1,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
            )
        )

        algo = PPO(config, env=DebugCounterEnv)

        rollout_worker = algo.workers.local_worker()
        sample_batch = rollout_worker.sample()
        sample_batch = convert_ma_batch_to_sample_batch(sample_batch)

        self.assertEqual(sample_batch["t"][0], 0)
        self.assertEqual(sample_batch.env_steps(), 200)
        self.assertEqual(sample_batch.agent_steps(), 200)

    def test_sample_batch_rollout_multi_agent_env(self):
        config = (
            PPOConfig()
            .framework("torch")
            .training(
                # Specifically ask for a batch of 200 samples.
                train_batch_size=200,
            )
            .rollouts(
                num_envs_per_worker=1,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
            )
        )

        algo = PPO(config, env="basic_multiagent")

        rollout_worker = algo.workers.local_worker()
        sample_batch = rollout_worker.sample()

        # 2 agents. So the multi-agent SampleBatch should have
        # 200 env steps, and 400 agent steps.
        self.assertEqual(sample_batch.env_steps(), 200)
        self.assertEqual(sample_batch.agent_steps(), 400)

    def test_guess_the_number_multi_agent(self):
        """This test will test env runner in the game of GuessTheNumberGame.

        The policies are chosen to be deterministic, so that we can test for an
        expected reward. Agent 1 will always pick 1, and agent 2 will always guess that
        the picked number is higher than 1. The game will end when the picked number is
        1, and agent 1 will win. The reward will be 100 for winning, and 1 for each
        step that the game is dragged on for. So the expected reward for agent 1 is 100
        + 19 = 119. 19 is the number of steps that the game will last for agent 1
        before it wins or loses.
        """

        gym.register("env_under_test", lambda: GuessTheNumberGame(config={}))

        def mapping_fn(agent_id, *args, **kwargs):
            return "pol1" if agent_id == 0 else "pol2"

        class PickOne(RandomPolicy):
            """This policy will always pick 1."""

            def compute_actions(
                self,
                obs_batch,
                state_batches=None,
                prev_action_batch=None,
                prev_reward_batch=None,
                **kwargs
            ):
                return [np.array([2, 1])] * len(obs_batch), [], {}

        class GuessHigherThanOne(RandomPolicy):
            """This policy will guess that the picked number is higher than 1."""

            def compute_actions(
                self,
                obs_batch,
                state_batches=None,
                prev_action_batch=None,
                prev_reward_batch=None,
                **kwargs
            ):
                return [np.array([1, 1])] * len(obs_batch), [], {}

        config = (
            PPOConfig()
            .framework("torch")
            .environment(disable_env_checking=True, env="env_under_test")
            .rollouts(
                num_envs_per_worker=1,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
                rollout_fragment_length=100,
            )
            .multi_agent(
                # this makes it independent of neural networks
                policies={
                    "pol1": PolicySpec(policy_class=PickOne),
                    "pol2": PolicySpec(policy_class=GuessHigherThanOne),
                },
                policy_mapping_fn=mapping_fn,
            )
            .debugging(seed=42)
        )

        algo = PPO(config, env="env_under_test")

        rollout_worker = algo.workers.local_worker()
        sample_batch = rollout_worker.sample()
        pol1_batch = sample_batch.policy_batches["pol1"]

        # reward should be 100 (for winning) + 19 (for dragging the game for 19 steps)
        check(pol1_batch["rewards"], 119 * np.ones_like(pol1_batch["rewards"]))
        # check if pol1 only has one timestep of transition informatio per each episode
        check(len(set(pol1_batch["eps_id"])), len(pol1_batch["eps_id"]))
        # check if pol2 has 19 timesteps of transition information per each episode
        pol2_batch = sample_batch.policy_batches["pol2"]
        check(len(set(pol2_batch["eps_id"])) * 19, len(pol2_batch["eps_id"]))

    def test_inference_batches_are_grouped_by_policy(self):
        # Create 2 policies that have different inference batch shapes.
        class RandomPolicyOne(RandomPolicy):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.view_requirements["rewards"].used_for_compute_actions = True
                self.view_requirements["terminateds"].used_for_compute_actions = True

        # Create 2 policies that have different inference batch shapes.
        class RandomPolicyTwo(RandomPolicy):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.view_requirements["rewards"].used_for_compute_actions = False
                self.view_requirements["terminateds"].used_for_compute_actions = False

        config = (
            PPOConfig()
            .framework("torch")
            .training(
                # Specifically ask for a batch of 200 samples.
                train_batch_size=200,
            )
            .rollouts(
                num_envs_per_worker=1,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
            )
            .multi_agent(
                policies={
                    "one": PolicySpec(
                        policy_class=RandomPolicyOne,
                    ),
                    "two": PolicySpec(
                        policy_class=RandomPolicyTwo,
                    ),
                },
                policy_mapping_fn=lambda *args, **kwargs: self.mapper.map(),
                policies_to_train=["one"],
                count_steps_by="agent_steps",
            )
        )

        algo = PPO(config, env="basic_multiagent")
        local_worker = algo.workers.local_worker()
        env = local_worker.env
        env.reset()
        obs, rewards, terminateds, truncateds, infos = local_worker.env.step(
            {0: env.action_space.sample(), 1: env.action_space.sample()}
        )

        env_id = 0
        env_runner = local_worker.sampler._env_runner_obj
        env_runner.create_episode(env_id)
        _, to_eval, _ = env_runner._process_observations(
            {0: obs}, {0: rewards}, {0: terminateds}, {0: truncateds}, {0: infos}
        )

        # We should have 2 separate batches for both policies.
        # Each batch has 1 samples.
        self.assertTrue("one" in to_eval)
        self.assertEqual(len(to_eval["one"]), 1)
        self.assertTrue("two" in to_eval)
        self.assertEqual(len(to_eval["two"]), 1)

    def test_action_connector_gets_raw_input_dict(self):
        class CheckInputDictActionConnector(ActionConnector):
            def __call__(self, ac_data):
                assert ac_data.input_dict, "raw input dict should be available"
                return ac_data

        class AddActionConnectorCallbacks(DefaultCallbacks):
            def on_create_policy(self, *, policy_id, policy) -> None:
                policy.action_connectors.append(
                    CheckInputDictActionConnector(ConnectorContext.from_policy(policy))
                )

        config = (
            PPOConfig()
            .framework("torch")
            .training(
                # Specifically ask for a batch of 200 samples.
                train_batch_size=200,
            )
            .callbacks(
                callbacks_class=AddActionConnectorCallbacks,
            )
            .rollouts(
                num_envs_per_worker=1,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
            )
        )

        algo = PPO(config, env="basic_multiagent")

        rollout_worker = algo.workers.local_worker()
        # As long as we can successfully sample(), things should be good.
        _ = rollout_worker.sample()

    def test_start_episode(self):
        config = (
            PPOConfig()
            .framework("torch")
            .training(
                # Specifically ask for a batch of 200 samples.
                train_batch_size=200,
            )
            .rollouts(
                num_envs_per_worker=1,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
            )
            .multi_agent(
                policies={
                    "one": PolicySpec(
                        policy_class=RandomPolicy,
                    ),
                    "two": PolicySpec(
                        policy_class=RandomPolicy,
                    ),
                },
                policy_mapping_fn=lambda *args, **kwargs: self.mapper.map(),
                policies_to_train=["one"],
                count_steps_by="agent_steps",
            )
        )

        algo = PPO(config, env="basic_multiagent")

        local_worker = algo.workers.local_worker()

        env_runner = local_worker.sampler._env_runner_obj

        # No episodes present
        self.assertEqual(env_runner._active_episodes.get(0), None)
        env_runner.step()
        # Only initial observation collected, add_init_obs called on episode
        self.assertEqual(env_runner._active_episodes[0].total_env_steps, 0)
        self.assertEqual(env_runner._active_episodes[0].total_agent_steps, 0)
        env_runner.step()
        # First recorded step, add_action_reward_done_next_obs called
        self.assertEqual(env_runner._active_episodes[0].total_env_steps, 1)
        self.assertEqual(env_runner._active_episodes[0].total_agent_steps, 2)

    def test_env_runner_output(self):
        # Test if we can produce RolloutMetrics just by stepping
        config = (
            PPOConfig()
            .framework("torch")
            .training(
                # Specifically ask for a batch of 200 samples.
                train_batch_size=200,
            )
            .rollouts(
                num_envs_per_worker=1,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
            )
            .multi_agent(
                policies={
                    "one": PolicySpec(
                        policy_class=RandomPolicy,
                    ),
                    "two": PolicySpec(
                        policy_class=RandomPolicy,
                    ),
                },
                policy_mapping_fn=lambda *args, **kwargs: self.mapper.map(),
                policies_to_train=["one"],
                count_steps_by="agent_steps",
            )
        )

        algo = PPO(config, env="basic_multiagent")

        local_worker = algo.workers.local_worker()

        env_runner = local_worker.sampler._env_runner_obj

        outputs = []
        while not outputs:
            outputs = env_runner.step()

        self.assertEqual(len(outputs), 1)
        self.assertTrue(len(list(outputs[0].agent_rewards.keys())) == 2)

    def test_env_error(self):
        class CheckErrorCallbacks(DefaultCallbacks):
            def on_episode_end(
                self, *, worker, base_env, policies, episode, env_index=None, **kwargs
            ) -> None:
                # We should see an error episode.
                assert isinstance(episode, Exception)

        # Test if we can produce RolloutMetrics just by stepping
        config = (
            PPOConfig()
            .framework("torch")
            .training(
                # Specifically ask for a batch of 200 samples.
                train_batch_size=200,
            )
            .rollouts(
                num_envs_per_worker=1,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
            )
            .multi_agent(
                policies={
                    "one": PolicySpec(
                        policy_class=RandomPolicy,
                    ),
                    "two": PolicySpec(
                        policy_class=RandomPolicy,
                    ),
                },
                policy_mapping_fn=lambda *args, **kwargs: self.mapper.map(),
                policies_to_train=["one"],
                count_steps_by="agent_steps",
            )
            .callbacks(
                callbacks_class=CheckErrorCallbacks,
            )
        )

        algo = PPO(config, env="basic_multiagent")

        local_worker = algo.workers.local_worker()

        env_runner = local_worker.sampler._env_runner_obj

        # Run a couple of steps.
        env_runner.step()
        env_runner.step()

        active_envs, to_eval, outputs = env_runner._process_observations(
            unfiltered_obs={0: AttributeError("mock error")},
            rewards={0: {}},
            terminateds={0: {"__all__": True}},
            truncateds={0: {"__all__": False}},
            infos={0: {}},
        )

        self.assertEqual(active_envs, {0})
        self.assertTrue(to_eval)  # to_eval contains data for the resetted new episode.
        self.assertEqual(len(outputs), 1)
        self.assertTrue(isinstance(outputs[0], RolloutMetrics))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
