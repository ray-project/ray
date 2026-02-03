import unittest
from pathlib import Path

import gymnasium as gym

import ray
from ray.rllib.algorithms.bc.bc import BCConfig
from ray.rllib.core import ALL_MODULES, DEFAULT_MODULE_ID
from ray.rllib.offline.offline_policy_evaluation_runner import (
    OfflinePolicyEvaluationRunner,
)
from ray.rllib.utils.metrics import (
    DATASET_NUM_ITERS_EVALUATED,
    DATASET_NUM_ITERS_EVALUATED_LIFETIME,
    EPISODE_LEN_MAX,
    EPISODE_LEN_MEAN,
    EPISODE_LEN_MIN,
    EVALUATION_RESULTS,
    MODULE_SAMPLE_BATCH_SIZE_MEAN,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_MODULE_STEPS_SAMPLED,
    NUM_MODULE_STEPS_SAMPLED_LIFETIME,
    OFFLINE_EVAL_RUNNER_RESULTS,
    WEIGHTS_SEQ_NO,
)
from ray.rllib.utils.typing import ResultDict


class TestOfflineEvaluationRunner(unittest.TestCase):
    def setUp(self) -> None:
        data_path = "offline/tests/data/cartpole/cartpole-v1_large"
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
                offline_evaluation_interval=2,
                offline_evaluation_type="is",
                num_offline_eval_runners=0,
                offline_eval_batch_size_per_runner=256,
            )
        )

    def tearDown(self):
        # Pull down Ray after each test.
        ray.shutdown()

    def test_offline_policy_evaluation_runner_setup(self):
        """Test the setup of the `OfflinePolicyEvaluationRunner`.

        Checks that after instantiation, the runner has a valid config and
        a `MultiRLModule`.
        """
        # Create an `OfflinePolicyEvaluationRunner` instance.
        offline_policy_eval_runner = OfflinePolicyEvaluationRunner(config=self.config)

        # Ensure that the runner has a config.
        self.assertIsInstance(offline_policy_eval_runner.config, BCConfig)
        # Ensure that the runner has an `MultiRLModule`.
        from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule

        self.assertIsInstance(offline_policy_eval_runner.module, MultiRLModule)

    def test_offline_policy_evaluation_runner_dataset_iterator(self):
        """Test setting the dataset iterator in the `OfflinePolicyEvaluationRunner`.

        Ensures that after setting the iterator, the internal `_dataset_iterator`
        is not `None`.
        """
        # Create an algorithm from the config.
        algo = self.config.build()

        # Create an `OfflinePolicyEvaluationRunner`.
        offline_eval_runner = OfflinePolicyEvaluationRunner(config=self.config)

        # Assign an iterator to the runner.
        iterators = algo.offline_eval_runner_group._offline_data_iterators
        offline_eval_runner.set_dataset_iterator(iterator=iterators[0])

        # Ensure the dataset iterator is set.
        self.assertIsNotNone(offline_eval_runner._dataset_iterator)

        # Clean up.
        algo.cleanup()

    def test_offline_policy_evaluation_runner_run(self):
        """Test the `OfflinePolicyEvaluationRunner.run()` method.

        Checks, that the correct number of env steps and dataset iterations
        were sampled. Furthermore, ensures that the returned metrics dict has the
        correct structure and types. Tests also that the internal `_batch_iterator`
        was built correctly.
        """
        # Build an algorithm.
        algo = self.config.build()
        # Build an `OfflinePolicyEvaluationRunner` instance.
        offline_eval_runner = OfflinePolicyEvaluationRunner(config=self.config)

        # Assign a data iterator to the runner.
        iterators = algo.offline_eval_runner_group._offline_data_iterators
        offline_eval_runner.set_dataset_iterator(iterator=iterators[0])

        # Run the runner and receive metrics.
        metrics = offline_eval_runner.run()

        # Ensure that we received a dictionary.
        self.assertIsInstance(metrics, ResultDict)
        # Ensure that the metrics of the `default_policy` are also a dict.
        self.assertIsInstance(metrics[DEFAULT_MODULE_ID], ResultDict)
        # Make sure that the metric for the total eval loss is a `Stats` instance.
        from ray.rllib.utils.metrics.stats import StatsBase

        for key in metrics[DEFAULT_MODULE_ID]:
            self.assertIsInstance(metrics[DEFAULT_MODULE_ID][key], StatsBase)

        # Ensure that we sampled exactly the desired number of env steps.
        self.assertEqual(
            metrics[DEFAULT_MODULE_ID][MODULE_SAMPLE_BATCH_SIZE_MEAN].peek(),
            self.config.offline_eval_batch_size_per_runner,
        )
        # Ensure that - in this case of 1-step episodes - the number of
        # module steps sampled equals the number of env steps sampled.
        for key in [DEFAULT_MODULE_ID, ALL_MODULES]:
            self.assertEqual(
                metrics[key][NUM_MODULE_STEPS_SAMPLED].peek(),
                self.config.offline_eval_batch_size_per_runner
                * self.config.dataset_num_iters_per_learner,
            )
            self.assertEqual(
                metrics[key][NUM_MODULE_STEPS_SAMPLED_LIFETIME].peek(),
                self.config.offline_eval_batch_size_per_runner
                * self.config.dataset_num_iters_per_learner,
            )
        # Ensure that we sampled the correct number of env steps.
        self.assertEqual(
            metrics[key][NUM_ENV_STEPS_SAMPLED].peek(),
            self.config.offline_eval_batch_size_per_runner
            * self.config.dataset_num_iters_per_learner,
        )
        # Ensure that the lifetime env steps sampled equal the number of
        # env steps sampled.
        self.assertEqual(
            metrics[key][NUM_ENV_STEPS_SAMPLED_LIFETIME].peek(),
            self.config.offline_eval_batch_size_per_runner
            * self.config.dataset_num_iters_per_learner,
        )

        # Make sure we also iterated only once over the dataset.
        self.assertEqual(
            metrics[ALL_MODULES][DATASET_NUM_ITERS_EVALUATED].peek(),
            self.config.dataset_num_iters_per_learner,
        )
        self.assertEqual(
            metrics[ALL_MODULES][DATASET_NUM_ITERS_EVALUATED_LIFETIME].peek(),
            self.config.dataset_num_iters_per_learner,
        )
        # Since we have 1-step episodes, ensure that min, max, mean episode
        # lengths are all equal to 1.
        self.assertEqual(metrics[EPISODE_LEN_MIN].peek().item(), 1)
        self.assertEqual(metrics[EPISODE_LEN_MAX].peek().item(), 1)
        self.assertEqual(metrics[EPISODE_LEN_MEAN].peek().item(), 1)
        # Ensure that the `_batch_iterator` instance was built. Note, this is
        # built in the first call to `OfflineEvaluationRunner.run()`.
        from ray.rllib.offline.offline_policy_evaluation_runner import (
            MiniBatchEpisodeRayDataIterator,
        )

        self.assertIsInstance(
            offline_eval_runner._batch_iterator, MiniBatchEpisodeRayDataIterator
        )

        # Clean up.
        algo.cleanup()

    def test_evaluation_in_algorithm_evaluate_offline(self):
        """Test using the algorithm's `evaluate_offline()` method.

        Checks, that the correct number of env steps and dataset iterations
        were sampled.
        """
        # Build an algorithm.
        algo = self.config.build()

        # Get evaluation metrics.
        eval_metrics = algo.evaluate_offline()

        # Ensure that we received a dictionary.
        self.assertIsInstance(eval_metrics, ResultDict)
        # Ensure that the metrics of the `default_policy` are also a dict.
        self.assertIsInstance(eval_metrics[OFFLINE_EVAL_RUNNER_RESULTS], ResultDict)

        eval_metrics = eval_metrics[OFFLINE_EVAL_RUNNER_RESULTS]
        # Ensure that we sampled exactly the desired number of env steps.
        self.assertEqual(
            eval_metrics[DEFAULT_MODULE_ID][MODULE_SAMPLE_BATCH_SIZE_MEAN],
            self.config.offline_eval_batch_size_per_runner,
        )
        # Ensure that - in this case of 1-step episodes - the number of
        # module steps sampled equals the number of env steps sampled.
        for key in [DEFAULT_MODULE_ID, ALL_MODULES]:
            self.assertEqual(
                eval_metrics[key][NUM_MODULE_STEPS_SAMPLED],
                self.config.offline_eval_batch_size_per_runner
                * self.config.dataset_num_iters_per_learner,
            )
            self.assertEqual(
                eval_metrics[key][NUM_MODULE_STEPS_SAMPLED_LIFETIME],
                self.config.offline_eval_batch_size_per_runner
                * self.config.dataset_num_iters_per_learner,
            )
        # Ensure that we sampled the correct number of env steps.
        self.assertEqual(
            eval_metrics[key][NUM_ENV_STEPS_SAMPLED],
            self.config.offline_eval_batch_size_per_runner
            * self.config.dataset_num_iters_per_learner,
        )
        # Ensure that the lifetime env steps sampled equal the number of
        # env steps sampled.
        self.assertEqual(
            eval_metrics[key][NUM_ENV_STEPS_SAMPLED_LIFETIME],
            self.config.offline_eval_batch_size_per_runner
            * self.config.dataset_num_iters_per_learner,
        )

        # Make sure we also iterated only once over the dataset.
        self.assertEqual(
            eval_metrics[ALL_MODULES][DATASET_NUM_ITERS_EVALUATED],
            self.config.dataset_num_iters_per_learner,
        )
        self.assertEqual(
            eval_metrics[ALL_MODULES][DATASET_NUM_ITERS_EVALUATED_LIFETIME],
            self.config.dataset_num_iters_per_learner,
        )

        # Clean up.
        algo.cleanup()

    def test_evaluation_in_algorithm_train(self):
        """Test using the algorithm's `train()` method with offline evaluation.

        Checks, that the correct number of env steps and dataset iterations
        were sampled. Furthermore, ensures that offline evaluation is run at the
        correct interval.
        """
        # Build an algorithm.
        algo = self.config.build()

        # Run a few training iterations.
        results = []
        for i in range(5):
            results.append(algo.train())

        # Ensure that we evaluated every 2 training iterations.
        self.assertEqual(self.config.offline_evaluation_interval, 2)
        self.assertIn(EVALUATION_RESULTS, results[1])
        self.assertIn(EVALUATION_RESULTS, results[2])
        self.assertIn(EVALUATION_RESULTS, results[3])
        self.assertIn(EVALUATION_RESULTS, results[4])
        # Also ensure we have no evaluation results in the first iteration.
        self.assertNotIn(EVALUATION_RESULTS, results[0])

        # Ensure that we did 2 iterations over the dataset in each evaluation.
        # TODO (simon): Add a test for the `weights_seq_no`.
        expected_weights_seq_no = 0
        for eval_idx in [1, 2, 3, 4]:
            # Evaluation ran at this iteration.
            evaluation_ran = (
                eval_idx + 1
            ) % self.config.offline_evaluation_interval == 0
            # Get evaluation metrics.
            eval_metrics = results[eval_idx][EVALUATION_RESULTS]
            eval_metrics = eval_metrics[OFFLINE_EVAL_RUNNER_RESULTS]
            # Ensure that we sampled exacxtly once from the dataset.
            self.assertEqual(
                eval_metrics[ALL_MODULES][DATASET_NUM_ITERS_EVALUATED],
                self.config.dataset_num_iters_per_learner,
            )
            # Update expected weights seq no.
            if evaluation_ran:
                expected_weights_seq_no = eval_idx + 1

            # Check weights seq no.
            self.assertEqual(
                eval_metrics[WEIGHTS_SEQ_NO],
                expected_weights_seq_no,
            )

            # Check lifetime dataset iterations one iteration after actual evaluation.
            if not evaluation_ran:
                # NOTE: In the first evaluation iteration the lifetime metrics are correct
                #   right away, in the later evaluations they are only updated one iteration later,
                #   due to compilation in `_run_one_training_iteration`. See also the note below.
                if eval_idx <= 2:
                    self.assertEqual(
                        eval_metrics[ALL_MODULES][DATASET_NUM_ITERS_EVALUATED_LIFETIME],
                        self.config.dataset_num_iters_per_learner,
                    )
                else:
                    self.assertEqual(
                        eval_metrics[ALL_MODULES][DATASET_NUM_ITERS_EVALUATED_LIFETIME],
                        results[eval_idx - 1][EVALUATION_RESULTS][
                            OFFLINE_EVAL_RUNNER_RESULTS
                        ][ALL_MODULES][DATASET_NUM_ITERS_EVALUATED_LIFETIME]
                        + self.config.dataset_num_iters_per_learner,
                    )

        # Get evaluation metrics from the last training iteration.
        # NOTE: Evaluation ran at one iteration before, but lifetime metrics
        #   are updated only one iteration later. This is a known issue, but hard
        #   to fix without breaking existing code.
        eval_metrics = results[4][EVALUATION_RESULTS]

        # Ensure that we received a dictionary.
        self.assertIsInstance(eval_metrics, ResultDict)
        # Ensure that the metrics of the `default_policy` are also a dict.
        self.assertIsInstance(eval_metrics[OFFLINE_EVAL_RUNNER_RESULTS], ResultDict)

        eval_metrics = eval_metrics[OFFLINE_EVAL_RUNNER_RESULTS]
        # Ensure that we sampled exactly the desired number of env steps.
        self.assertEqual(
            eval_metrics[DEFAULT_MODULE_ID][MODULE_SAMPLE_BATCH_SIZE_MEAN],
            self.config.offline_eval_batch_size_per_runner,
        )
        # Ensure that - in this case of 1-step episodes - the number of
        # module steps sampled equals the number of env steps sampled.
        for key in [DEFAULT_MODULE_ID, ALL_MODULES]:
            self.assertEqual(
                eval_metrics[key][NUM_MODULE_STEPS_SAMPLED],
                self.config.offline_eval_batch_size_per_runner
                * self.config.dataset_num_iters_per_learner,
            )
            self.assertEqual(
                eval_metrics[key][NUM_MODULE_STEPS_SAMPLED_LIFETIME],
                self.config.offline_eval_batch_size_per_runner
                * self.config.dataset_num_iters_per_learner
                * self.config.offline_evaluation_interval,
            )
        # Ensure that we sampled the correct number of env steps.
        self.assertEqual(
            eval_metrics[key][NUM_ENV_STEPS_SAMPLED],
            self.config.offline_eval_batch_size_per_runner
            * self.config.dataset_num_iters_per_learner,
        )
        # Ensure that the lifetime env steps sampled equal the number of
        # env steps sampled.
        self.assertEqual(
            eval_metrics[key][NUM_ENV_STEPS_SAMPLED_LIFETIME],
            self.config.offline_eval_batch_size_per_runner
            * self.config.dataset_num_iters_per_learner
            * self.config.offline_evaluation_interval,
        )

        # Clean up.
        algo.cleanup()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
