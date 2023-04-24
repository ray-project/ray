import tempfile
import unittest

import ray
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY


algorithms_and_configs = {
    "PPO": (PPOConfig().training(train_batch_size=2, sgd_minibatch_size=2))
}


@ray.remote
def save_and_train(algo_cfg, env, tmpdir):
    algo_cfg = (
        algo_cfg.training(_enable_learner_api=True)
        .rl_module(_enable_rl_module_api=True)
        .rollouts(num_rollout_workers=0)
        .reporting(min_time_s_per_iteration=0, min_sample_timesteps_per_iteration=1)
        .debugging(seed=10)
    )
    algo1 = algo_cfg.environment(env).build()

    print(tmpdir)
    tmpdir = str(tmpdir)
    algo1.save_checkpoint(tmpdir)
    print("======= training algo 1 =======")
    algo1.train()
    results_algo1 = algo1.train()
    return results_algo1["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY]


@ray.remote
def load_and_train(algo_cfg, env, tmpdir):
    algo_cfg = (
        algo_cfg.training(_enable_learner_api=True)
        .rl_module(_enable_rl_module_api=True)
        .rollouts(num_rollout_workers=0)
        .reporting(min_time_s_per_iteration=0, min_sample_timesteps_per_iteration=1)
        .debugging(seed=10)
    )
    algo = algo_cfg.environment(env).build()
    print(tmpdir)
    tmpdir = str(tmpdir)
    algo.load_checkpoint(tmpdir)
    print("======= training algo 2 =======")
    algo.train()
    results_algo2 = algo.train()
    return results_algo2["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY]


class TestAlgorithmWithLearnerSaveAndRestore(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_save_and_restore(self):
        for algo_name in algorithms_and_configs:
            config = algorithms_and_configs[algo_name]
            for _ in framework_iterator(config, frameworks=["torch", "tf2"]):
                with tempfile.TemporaryDirectory() as tmpdir:
                    results_algo_1 = ray.get(
                        save_and_train.remote(config, "CartPole-v1", tmpdir)
                    )
                    results_algo_2 = ray.get(
                        load_and_train.remote(config, "CartPole-v1", tmpdir)
                    )

                    results_algo_3 = ray.get(
                        load_and_train.remote(config, "CartPole-v1", tmpdir)
                    )

                    print(results_algo_1)
                    print(results_algo_2)
                    print(results_algo_3)

                    # check that the results are the same across loaded algorithms
                    check(results_algo_3, results_algo_2)


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
