import tempfile
import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.core import COMPONENT_ENV_TO_MODULE_CONNECTOR
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.test_utils import check

algorithms_and_configs = {
    "PPO": (PPOConfig().training(train_batch_size=2, minibatch_size=2))
}


@ray.remote
def save_train_and_get_states(
    algo_cfg: AlgorithmConfig, num_env_runners: int, env: str, tmpdir
):
    """Create an algo, train for 10 iterations, then checkpoint it.

    Note: This function uses a seeded algorithm that can modify the global random state.
        Running it multiple times in the same process can affect other algorithms.
        Making it a Ray task runs it in a separate process and prevents it from
        affecting other algorithms' random state.

    Args:
        algo_cfg: The algorithm config to build the algo from.
        num_env_runners: Number of environment runners to use.
        env: The gym genvironment to train on.
        tmpdir: The temporary directory to save the checkpoint to.

    Returns:
        The env-runner states after 10 iterations of training.
    """
    algo_cfg = (
        algo_cfg.api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment(env)
        .env_runners(
            num_env_runners=num_env_runners,
            env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
        )
        # setting min_time_s_per_iteration=0 and min_sample_timesteps_per_iteration=1
        # to make sure that we get results as soon as sampling/training is done at
        # least once
        .reporting(min_time_s_per_iteration=0, min_sample_timesteps_per_iteration=1)
        .debugging(seed=10)
    )
    algo = algo_cfg.build()
    for _ in range(10):
        algo.train()
    algo.save_to_path(tmpdir)
    states = algo.env_runner_group.foreach_env_runner(
        "get_state",
        local_env_runner=False,
    )
    return states


@ray.remote
def load_and_get_states(
    algo_cfg: AlgorithmConfig, num_env_runners: int, env: str, tmpdir
):
    """Loads the checkpoint saved by save_train_and_get_states and returns connector states.

    Note: This function uses a seeded algorithm that can modify the global random state.
        Running it multiple times in the same process can affect other algorithms.
        Making it a Ray task runs it in a separate process and prevents it from
        affecting other algorithms' random state.

    Args:
        algo_cfg: The algorithm config to build the algo from.
        num_env_runners: Number of env-runners to use.
        env: The gym genvironment to train on.
        tmpdir: The temporary directory to save the checkpoint to.

    Returns:
        The connector states of remote env-runners after 10 iterations of training.

    """
    algo_cfg = (
        algo_cfg.api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment(env)
        .env_runners(
            num_env_runners=num_env_runners,
            env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
        )
        # setting min_time_s_per_iteration=0 and min_sample_timesteps_per_iteration=1
        # to make sure that we get results as soon as sampling/training is done at
        # least once
        .reporting(min_time_s_per_iteration=0, min_sample_timesteps_per_iteration=1)
        .debugging(seed=10)
    )
    algo = algo_cfg.build()
    algo.restore_from_path(tmpdir)
    states = algo.env_runner_group.foreach_env_runner(
        "get_state",
        local_env_runner=False,
    )

    return states


class TestAlgorithmWithConnectorsSaveAndRestore(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_save_and_restore_w_remote_env_runners(self):
        num_env_runners = 2
        for algo_name in algorithms_and_configs:
            config = algorithms_and_configs[algo_name]
            with tempfile.TemporaryDirectory() as tmpdir:
                # create an algorithm, checkpoint it, then train for 2 iterations
                connector_states_algo_1 = ray.get(
                    save_train_and_get_states.remote(
                        config, num_env_runners, "CartPole-v1", tmpdir
                    )
                )
                # load that checkpoint into a new algorithm and check the states.
                connector_states_algo_2 = ray.get(  # noqa
                    load_and_get_states.remote(
                        config, num_env_runners, "CartPole-v1", tmpdir
                    )
                )

                # Assert that all running stats are the same.
                self._assert_running_stats_consistency(
                    connector_states_algo_1, connector_states_algo_2
                )

    def test_save_and_restore_w_remote_env_runners_and_wo_local_env_runner(self):
        num_env_runners = 2
        for algo_name in algorithms_and_configs:
            config = algorithms_and_configs[algo_name].env_runners(
                create_local_env_runner=False
            )
            with tempfile.TemporaryDirectory() as tmpdir:
                # create an algorithm, checkpoint it, then train for 2 iterations
                connector_states_algo_1 = ray.get(
                    save_train_and_get_states.remote(
                        config, num_env_runners, "CartPole-v1", tmpdir
                    )
                )
                # load that checkpoint into a new algorithm and check the states.
                connector_states_algo_2 = ray.get(  # noqa
                    load_and_get_states.remote(
                        config, num_env_runners, "CartPole-v1", tmpdir
                    )
                )
                # Assert that all running stats are the same.
                self._assert_running_stats_consistency(
                    connector_states_algo_1, connector_states_algo_2
                )

    def _assert_running_stats_consistency(
        self, connector_states_algo_1: list, connector_states_algo_2: list
    ):
        """
        Asserts consistency of running stats within and between algorithms.
        """

        running_stats_states_algo_1 = [
            state[COMPONENT_ENV_TO_MODULE_CONNECTOR]["MeanStdFilter"][None][
                "running_stats"
            ]
            for state in connector_states_algo_1
        ]
        running_stats_states_algo_2 = [
            state[COMPONENT_ENV_TO_MODULE_CONNECTOR]["MeanStdFilter"][None][
                "running_stats"
            ]
            for state in connector_states_algo_2
        ]

        running_stats_states_algo_1 = [
            [RunningStat.from_state(s) for s in running_stats_state]
            for running_stats_state in running_stats_states_algo_1
        ]
        running_stats_states_algo_2 = [
            [RunningStat.from_state(s) for s in running_stats_state]
            for running_stats_state in running_stats_states_algo_2
        ]

        running_stats_states_algo_1 = [
            (
                running_stat[0].n,
                running_stat[0].mean_array,
                running_stat[0].sum_sq_diff_array,
            )
            for running_stat in running_stats_states_algo_1
        ]
        running_stats_states_algo_2 = [
            (
                running_stat[0].n,
                running_stat[0].mean_array,
                running_stat[0].sum_sq_diff_array,
            )
            for running_stat in running_stats_states_algo_2
        ]

        # The number of env-runners must be two for the following checks to make sense.
        self.assertEqual(len(running_stats_states_algo_1), 2)
        self.assertEqual(len(running_stats_states_algo_2), 2)

        # Assert that all running stats in algo-1 are the same (for consistency).
        check(running_stats_states_algo_1[0][0], running_stats_states_algo_1[1][0])

        # Now ensure that the connector states on remote `EnvRunner`s were restored.
        check(running_stats_states_algo_1[0][0], running_stats_states_algo_2[0][0])

        # Ensure also that all states are the same in algo-2 (for consistency).
        check(running_stats_states_algo_2[0][0], running_stats_states_algo_2[1][0])


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
