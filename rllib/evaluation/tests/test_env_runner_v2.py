import unittest

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.connectors.connector import ActionConnector, ConnectorContext
from ray.rllib.examples.env.debug_counter_env import DebugCounterEnv
from ray.rllib.examples.env.multi_agent import BasicMultiAgent
from ray.tune import register_env


register_env("basic_multiagent", lambda _: BasicMultiAgent(2))


class TestEnvRunnerV2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

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
                horizon=4,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
            )
        )

        algo = PPO(config, env=DebugCounterEnv)

        rollout_worker = algo.workers.local_worker()
        sample_batch = rollout_worker.sample()

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
                horizon=4,
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
                horizon=4,
                num_rollout_workers=0,
                # Enable EnvRunnerV2.
                enable_connectors=True,
            )
        )

        algo = PPO(config, env="basic_multiagent")

        rollout_worker = algo.workers.local_worker()
        # As long as we can successfully sample(), things should be good.
        _ = rollout_worker.sample()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
