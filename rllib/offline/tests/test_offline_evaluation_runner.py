import unittest
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

import gymnasium as gym

from ray.rllib.algorithms.bc.bc import BCConfig
from ray.rllib.core import ALL_MODULES, DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.offline.offline_evaluation_runner import (
    TOTAL_EVAL_LOSS_KEY,
    OfflineEvaluationRunner,
)
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED
from ray.rllib.utils.typing import ModuleID, ResultDict, TensorType

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig


class TestOfflineEvaluationRunner(unittest.TestCase):
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

    def test_offline_evaluation_runner_setup(self):

        # Create an `OfflineEvalautionRunner` instance.
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

        # Create an `OfflineEvaluationRunner`.
        offline_eval_runner = OfflineEvaluationRunner(config=self.config)

        # Assign an iterator to the runner.
        iterators = algo.offline_data.sample(
            num_samples=self.config.offline_eval_batch_size_per_runner,
            return_iterator=True,
            num_shards=0,
        )
        offline_eval_runner.set_dataset_iterator(iterator=iterators[0])

        # Ensure the dataset iterator is set.
        self.assertIsNotNone(offline_eval_runner._dataset_iterator)

        # Clean up.
        algo.cleanup()

    def test_offline_evaluation_runner_run(self):

        # Build an algorithm.
        algo = self.config.build()
        # Build an `OfflineEvaluationRunner` instance.
        offline_eval_runner = OfflineEvaluationRunner(config=self.config)

        # Assign a data iterator to the runner.
        iterators = algo.offline_data.sample(
            num_samples=self.config.offline_eval_batch_size_per_runner,
            return_iterator=True,
            num_shards=0,
        )
        offline_eval_runner.set_dataset_iterator(iterator=iterators[0])

        # Run the runner and receive metrics.
        metrics = offline_eval_runner.run()

        # Ensure that we received a dictionary.
        self.assertIsInstance(metrics, ResultDict)
        # Ensure that the metrics of the `default_policy` are also a dict.
        self.assertIsInstance(metrics[DEFAULT_MODULE_ID], ResultDict)
        # Make sure that the metric for the total eval loss is a `Stats` instance.
        from ray.rllib.utils.metrics.stats import Stats

        self.assertIsInstance(metrics[DEFAULT_MODULE_ID][TOTAL_EVAL_LOSS_KEY], Stats)
        # Ensure that the `_batch_iterator` instance was built. Note, this is
        # built in the first call to `OfflineEvaluationRunner.run()`.
        from ray.rllib.utils.minibatch_utils import MiniBatchRayDataIterator

        self.assertIsInstance(
            offline_eval_runner._batch_iterator, MiniBatchRayDataIterator
        )

        # Clean up.
        algo.cleanup()

    def test_offline_evaluation_runner_loss_fn(self):

        # Import pytorch to define a custom SL loss function.
        from ray.rllib.utils.framework import try_import_torch

        torch, nn = try_import_torch()

        # Define a custom SL loss function for evaluation that considers
        # classification of actions.
        def _compute_loss_for_module(
            runner: OfflineEvaluationRunner,
            module_id: ModuleID,
            config: "AlgorithmConfig",
            batch: Dict[str, Any],
            fwd_out: Dict[str, TensorType],
        ):
            # Compute the log probabilities of the actions.
            action_dist_log_probs = nn.LogSoftmax()(fwd_out[Columns.ACTION_DIST_INPUTS])
            # Compute the negative log-loss of actions.
            loss = torch.nn.NLLLoss()(action_dist_log_probs, batch[Columns.ACTIONS])

            # Return the loss.
            return loss

        # Configure a custom loss function for offline evaluation.
        self.config = self.config.evaluation(
            offline_loss_for_module_fn=_compute_loss_for_module,
        )
        # Build the algorithm.
        algo = self.config.build()

        # Create an `OfflineEvaluatioRunner`.
        offline_eval_runner = OfflineEvaluationRunner(config=self.config)
        # Create a data iterator and assign it to the runner.
        iterators = algo.offline_data.sample(
            num_samples=self.config.offline_eval_batch_size_per_runner,
            return_iterator=True,
            num_shards=0,
        )
        offline_eval_runner.set_dataset_iterator(iterator=iterators[0])

        # Now run the runner and collect metrics.
        metrics = offline_eval_runner.run()

        # Assert that we got a `ResultDict`.
        self.assertIsInstance(metrics, ResultDict)
        # Ensure that the custom loss has been recorded.
        self.assertIn(TOTAL_EVAL_LOSS_KEY, metrics[DEFAULT_MODULE_ID])
        # Make sure that the number of steps is recorded.
        self.assertIn(NUM_ENV_STEPS_SAMPLED, metrics[ALL_MODULES])
        # Ensure that the number of steps evaluated is the same as the configured
        # batch size for offline evaluation.
        self.assertEqual(
            metrics[ALL_MODULES][NUM_ENV_STEPS_SAMPLED],
            self.config.offline_eval_batch_size_per_runner,
        )

        # Clean up.
        algo.cleanup()

        # Reset the config to the default.
        self.config = self.config.evaluation(
            offline_loss_for_module_fn=None,
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
