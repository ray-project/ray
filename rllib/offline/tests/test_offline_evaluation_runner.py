import unittest
import gymnasium as gym
import ray
from pathlib import Path

from ray.rllib.algorithms.bc.bc import BCConfig
from ray.rllib.offline.offline_evaluation_runner import OfflineEvaluationRunner


class TestOfflineData(unittest.TestCase):
    def setUp(self) -> None:
        data_path = "tests/data/cartpole/cartpole-v1_large"
        self.base_path = Path(__file__).parents[2]
        self.data_path = "local://" + self.base_path.joinpath(data_path).as_posix()
        # Assign the observation and action spaces.
        env = gym.make("CartPole-v1")
        self.observation_space = env.observation_space
        self.action_space = env.action_space

        # Create a simple config.
        self.config = (
            BCConfig()
            .environment(
                observation_space=self.observation_space,
                action_space=self.action_space,
            )
            .api_stack(
                enable_env_runner_and_connector_v2=True,
                enable_rl_module_and_learner=True,
            )
            .offline_data(
                input_=[self.data_path],
                dataset_num_iters_per_learner=1,
            )
            .learners(
                num_learners=0,
            )
            .training(
                train_batch_size_per_learner=256,
            )
            .evaluation(
                num_offline_eval_runners=2,
                offline_eval_batch_size_per_runner=256,
            )
        )
        # Start ray.
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_offline_evaluation_runner_setup(self):

        # Create an algorithm from the config.
        # algo = self.config.build()

        offline_eval_runner = OfflineEvaluationRunner(config=self.config)

        # Ensure that the runner has a config.
        self.assertIsInstance(offline_eval_runner.config, BCConfig)
        # Ensure that the runner has an `MultiRLModule`.
        from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule

        self.assertIsInstance(offline_eval_runner.module, MultiRLModule)
        # Make sure the runner has a callable loss function.
        from typing import Callable

        self.assertIsInstance(offline_eval_runner._loss_for_module_fn, Callable)

    def test_offline_evaluation_runner_dataset_iterator(self):

        # Create an algorithm from the config.
        algo = self.config.build()

        iterators = algo.offline_data.sample(
            num_samples=self.config.offline_eval_batch_size_per_runner,
            return_iterator=True,
            num_shards=0,
        )

        offline_eval_runner = OfflineEvaluationRunner(config=self.config)

        offline_eval_runner.set_dataset_iterator(iterator=iterators[0])

        # Ensure the dataset iterator is set.
        self.assertIsNotNone(offline_eval_runner._dataset_iterator)

    def test_offline_evaluation_runner_run(self):

        algo = self.config.build()

        offline_eval_runner = OfflineEvaluationRunner(config=self.config)

        iterators = algo.offline_data.sample(
            num_samples=self.config.offline_eval_batch_size_per_runner,
            return_iterator=True,
            num_shards=0,
        )

        offline_eval_runner.set_dataset_iterator(iterator=iterators[0])

        metrics = offline_eval_runner.run(num_samples=1)

        from ray.rllib.core import DEFAULT_MODULE_ID
        from ray.rllib.offline.offline_evaluation_runner import TOTAL_EVAL_LOSS_KEY
        from ray.rllib.utils.metrics.stats import Stats
        from ray.rllib.utils.typing import ResultDict

        self.assertIsInstance(metrics, ResultDict)
        self.assertIsInstance(metrics[DEFAULT_MODULE_ID], ResultDict)
        self.assertIsInstance(metrics[DEFAULT_MODULE_ID][TOTAL_EVAL_LOSS_KEY], Stats)
        from ray.rllib.utils.minibatch_utils import MiniBatchRayDataIterator

        self.assertIsInstance(
            offline_eval_runner._batch_iterator, MiniBatchRayDataIterator
        )


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
