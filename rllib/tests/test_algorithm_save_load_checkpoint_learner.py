import tempfile
import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY


algorithms_and_configs = {
    "PPO": (PPOConfig().training(train_batch_size=2, sgd_minibatch_size=2))
}


@ray.remote
def save_and_train(algo_cfg: AlgorithmConfig, env: str, tmpdir):
    """Create an algo, checkpoint it, then train for 2 iterations.

    Note: This function uses a seeded algorithm that can modify the global random state.
        Running it multiple times in the same process can affect other algorithms.
        Making it a Ray task runs it in a separate process and prevents it from
        affecting other algorithms' random state.

    Args:
        algo_cfg: The algorithm config to build the algo from.
        env: The gym genvironment to train on.
        tmpdir: The temporary directory to save the checkpoint to.

    Returns:
        The learner stats after 2 iterations of training.
    """
    algo_cfg = (
        algo_cfg.training(_enable_learner_api=True)
        .rl_module(_enable_rl_module_api=True)
        .rollouts(num_rollout_workers=0)
        # setting min_time_s_per_iteration=0 and min_sample_timesteps_per_iteration=1
        # to make sure that we get results as soon as sampling/training is done at
        # least once
        .reporting(min_time_s_per_iteration=0, min_sample_timesteps_per_iteration=1)
        .debugging(seed=10)
    )
    algo = algo_cfg.environment(env).build()

    tmpdir = str(tmpdir)
    algo.save_checkpoint(tmpdir)
    for _ in range(2):
        results = algo.train()
    return results["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY]


@ray.remote
def load_and_train(algo_cfg: AlgorithmConfig, env: str, tmpdir):
    """Loads the checkpoint saved by save_and_train and trains for 2 iterations.

    Note: This function uses a seeded algorithm that can modify the global random state.
        Running it multiple times in the same process can affect other algorithms.
        Making it a Ray task runs it in a separate process and prevents it from
        affecting other algorithms' random state.

    Args:
        algo_cfg: The algorithm config to build the algo from.
        env: The gym genvironment to train on.
        tmpdir: The temporary directory to save the checkpoint to.

    Returns:
        The learner stats after 2 iterations of training.

    """
    algo_cfg = (
        algo_cfg.training(_enable_learner_api=True)
        .rl_module(_enable_rl_module_api=True)
        .rollouts(num_rollout_workers=0)
        # setting min_time_s_per_iteration=0 and min_sample_timesteps_per_iteration=1
        # to make sure that we get results as soon as sampling/training is done at
        # least once
        .reporting(min_time_s_per_iteration=0, min_sample_timesteps_per_iteration=1)
        .debugging(seed=10)
    )
    algo = algo_cfg.environment(env).build()
    tmpdir = str(tmpdir)
    algo.load_checkpoint(tmpdir)
    for _ in range(2):
        results = algo.train()
    return results["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY]


class TestAlgorithmWithLearnerSaveAndRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDowClass(cls) -> None:
        ray.shutdown()

    def test_save_and_restore(self):
        for algo_name in algorithms_and_configs:
            config = algorithms_and_configs[algo_name]
            for _ in framework_iterator(config, frameworks=["torch", "tf2"]):
                with tempfile.TemporaryDirectory() as tmpdir:
                    # create an algorithm, checkpoint it, then train for 2 iterations
                    ray.get(save_and_train.remote(config, "CartPole-v1", tmpdir))
                    # load that checkpoint into a new algorithm and train for 2
                    # iterations
                    results_algo_2 = ray.get(
                        load_and_train.remote(config, "CartPole-v1", tmpdir)
                    )

                    # load that checkpoint into another new algorithm and train for 2
                    # iterations
                    results_algo_3 = ray.get(
                        load_and_train.remote(config, "CartPole-v1", tmpdir)
                    )

                    # check that the results are the same across loaded algorithms
                    # they won't be the same as the first algorithm since the random
                    # state that is used for each algorithm is not preserved across
                    # checkpoints.
                    check(results_algo_3, results_algo_2)


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
