import unittest
from pathlib import Path

import ray
from ray.rllib.algorithms.bc import BCConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
)


class TestBC(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_bc_compilation_and_learning_from_offline_file(self):
        # Define the data paths.
        data_path = "tests/data/cartpole/cartpole-v1_large"
        base_path = Path(__file__).parents[3]
        print(f"base_path={base_path}")
        data_path = "local://" / base_path / data_path

        print(f"data_path={data_path}")

        # Define the BC config.
        config = (
            BCConfig()
            .environment(env="CartPole-v1")
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .learners(
                num_learners=0,
            )
            .evaluation(
                evaluation_interval=3,
                evaluation_num_env_runners=1,
                evaluation_duration=5,
                evaluation_parallel_to_training=True,
            )
            # Note, the `input_` argument is the major argument for the
            # new offline API.
            .offline_data(
                input_=[data_path.as_posix()],
                dataset_num_iters_per_learner=1,
            )
            .training(
                lr=0.0008,
                train_batch_size_per_learner=2000,
            )
        )

        num_iterations = 350
        min_return_to_reach = 120.0

        # TODO (simon): Add support for recurrent modules.
        algo = config.build()
        learnt = False
        for i in range(num_iterations):
            results = algo.train()
            print(results)

            eval_results = results.get(EVALUATION_RESULTS, {})
            if eval_results:
                episode_return_mean = eval_results[ENV_RUNNER_RESULTS][
                    EPISODE_RETURN_MEAN
                ]
                print(f"iter={i}, R={episode_return_mean}")
                if episode_return_mean > min_return_to_reach:
                    print("BC has learnt the task!")
                    learnt = True
                    break

        if not learnt:
            raise ValueError(
                f"`BC` did not reach {min_return_to_reach} reward from "
                "expert offline data!"
            )

        algo.stop()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
