import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole, MultiAgentMountainCar
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.test_utils import check_train_results, framework_iterator
from ray.tune.registry import register_env


def check_support_multiagent(alg: str, config: AlgorithmConfig):
    register_env(
        "multi_agent_mountaincar", lambda _: MultiAgentMountainCar({"num_agents": 2})
    )
    register_env(
        "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": 2})
    )

    # Simulate a simple multi-agent setup.
    policies = {
        "policy_0": PolicySpec(config={"gamma": 0.99}),
        "policy_1": PolicySpec(config={"gamma": 0.95}),
    }
    policy_ids = list(policies.keys())

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        pol_id = policy_ids[agent_id]
        return pol_id

    config.multi_agent(policies=policies, policy_mapping_fn=policy_mapping_fn)

    for fw in framework_iterator(config):
        if fw == "tf2" and alg == "IMPALA":
            continue
        if alg == "SAC":
            a = config.build(env="multi_agent_mountaincar")
        else:
            a = config.build(env="multi_agent_cartpole")

        results = a.train()
        check_train_results(results)
        print(results)
        a.stop()


class TestSupportedMultiAgentPolicyGradient(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_multiagent(self):
        check_support_multiagent("IMPALA", ImpalaConfig().resources(num_gpus=0))

    def test_ppo_multiagent(self):
        check_support_multiagent(
            "PPO",
            (
                PPOConfig()
                .rollouts(num_rollout_workers=1, rollout_fragment_length=10)
                .training(num_sgd_iter=1, train_batch_size=10, sgd_minibatch_size=1)
            ),
        )


class TestSupportedMultiAgentOffPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=6)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_dqn_multiagent(self):
        check_support_multiagent(
            "DQN",
            (
                DQNConfig()
                .reporting(min_sample_timesteps_per_iteration=1)
                .training(replay_buffer_config={"capacity": 1000})
            ),
        )

    def test_sac_multiagent(self):
        check_support_multiagent(
            "SAC",
            (
                SACConfig()
                .environment(normalize_actions=False)
                .rollouts(num_rollout_workers=0)
                .training(replay_buffer_config={"capacity": 1000})
            ),
        )


class TestSupportedMultiAgentMultiGPU(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_multiagent_multi_gpu(self):
        check_support_multiagent("IMPALA", ImpalaConfig().resources(num_gpus=2))


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
