import unittest

import ray
from ray.rllib.algorithms.registry import get_algorithm_class
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole, MultiAgentMountainCar
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.test_utils import check_train_results, framework_iterator
from ray.tune import register_env


def check_support_multiagent(alg, config):
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

    config["multiagent"] = {
        "policies": policies,
        "policy_mapping_fn": policy_mapping_fn,
    }

    for fw in framework_iterator(config):
        if fw == "tf2" and alg in ["A3C", "APEX", "APEX_DDPG", "IMPALA"]:
            continue
        if alg in ["DDPG", "APEX_DDPG", "SAC"]:
            a = get_algorithm_class(alg)(config=config, env="multi_agent_mountaincar")
        else:
            a = get_algorithm_class(alg)(config=config, env="multi_agent_cartpole")

        results = a.train()
        check_train_results(results)
        print(results)
        a.stop()


class TestSupportedMultiAgentPG(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_a3c_multiagent(self):
        check_support_multiagent(
            "A3C", {"num_workers": 1, "optimizer": {"grads_per_step": 1}}
        )

    def test_impala_multiagent(self):
        check_support_multiagent("IMPALA", {"num_gpus": 0})

    def test_pg_multiagent(self):
        check_support_multiagent("PG", {"num_workers": 1, "optimizer": {}})

    def test_ppo_multiagent(self):
        check_support_multiagent(
            "PPO",
            {
                "num_workers": 1,
                "num_sgd_iter": 1,
                "train_batch_size": 10,
                "rollout_fragment_length": 10,
                "sgd_minibatch_size": 1,
            },
        )


class TestSupportedMultiAgentOffPolicy(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(num_cpus=6)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_apex_multiagent(self):
        check_support_multiagent(
            "APEX",
            {
                "num_workers": 2,
                "min_sample_timesteps_per_iteration": 100,
                "num_gpus": 0,
                "replay_buffer_config": {
                    "capacity": 1000,
                },
                "num_steps_sampled_before_learning_starts": 10,
                "min_time_s_per_iteration": 1,
                "target_network_update_freq": 100,
                "optimizer": {
                    "num_replay_buffer_shards": 1,
                },
            },
        )

    def test_apex_ddpg_multiagent(self):
        check_support_multiagent(
            "APEX_DDPG",
            {
                "num_workers": 2,
                "min_sample_timesteps_per_iteration": 100,
                "replay_buffer_config": {
                    "capacity": 1000,
                },
                "num_steps_sampled_before_learning_starts": 10,
                "num_gpus": 0,
                "min_time_s_per_iteration": 1,
                "target_network_update_freq": 100,
                "use_state_preprocessor": True,
            },
        )

    def test_ddpg_multiagent(self):
        check_support_multiagent(
            "DDPG",
            {
                "min_sample_timesteps_per_iteration": 1,
                "replay_buffer_config": {
                    "capacity": 1000,
                },
                "num_steps_sampled_before_learning_starts": 10,
                "use_state_preprocessor": True,
            },
        )

    def test_dqn_multiagent(self):
        check_support_multiagent(
            "DQN",
            {
                "min_sample_timesteps_per_iteration": 1,
                "replay_buffer_config": {
                    "capacity": 1000,
                },
            },
        )

    def test_sac_multiagent(self):
        check_support_multiagent(
            "SAC",
            {
                "num_workers": 0,
                "replay_buffer_config": {
                    "capacity": 1000,
                },
                "normalize_actions": False,
            },
        )


if __name__ == "__main__":
    import pytest
    import sys

    # One can specify the specific TestCase class to run.
    # None for all unittest.TestCase classes in this file.
    class_ = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(pytest.main(["-v", __file__ + ("" if class_ is None else "::" + class_)]))
