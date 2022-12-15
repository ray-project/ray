import unittest

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.connectors.connector import ActionConnector, ConnectorContext
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.examples.env.debug_counter_env import DebugCounterEnv
from ray.rllib.examples.env.multi_agent import BasicMultiAgent
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.tune import register_env
from ray.rllib.policy.sample_batch import convert_ma_batch_to_sample_batch


register_env("basic_multiagent", lambda _: BasicMultiAgent(2))


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

    def test_inference_batches_are_grouped_by_policy(self):
        # Create 2 policies that have different inference batch shapes.
        class RandomPolicyOne(RandomPolicy):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.view_requirements["rewards"].used_for_compute_actions = True
                self.view_requirements["dones"].used_for_compute_actions = True

        # Create 2 policies that have different inference batch shapes.
        class RandomPolicyTwo(RandomPolicy):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.view_requirements["rewards"].used_for_compute_actions = False
                self.view_requirements["dones"].used_for_compute_actions = False

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

        obs, rewards, dones, infos = local_worker.env.step(
            {0: env.action_space.sample(), 1: env.action_space.sample()}
        )

        env_id = 0
        env_runner = local_worker.sampler._env_runner_obj
        env_runner.create_episode(env_id)
        _, to_eval, _ = env_runner._process_observations(
            {0: obs}, {0: rewards}, {0: dones}, {0: infos}
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
            dones={0: {"__all__": True}},
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
