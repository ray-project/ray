import sys
import unittest
from pathlib import Path

import gymnasium as gym

import ray
from ray.rllib.algorithms.bc.bc import BCConfig
from ray.rllib.offline.offline_evaluation_runner_group import (
    OfflineEvaluationRunnerGroup,
)


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
                offline_evaluation_type="eval_loss",
                offline_eval_batch_size_per_runner=256,
            )
        )
        # Start ray.
        ray.init(ignore_reinit_error=True)

    def tearDown(self) -> None:
        ray.shutdown()

    def test_offline_evaluation_runner_group_setup(self):

        # Build the algorithm.
        algo = self.config.build()

        # The module state is needed for the `OfflinePreLearner`.
        module_state = algo.learner_group._learner.module.get_state()

        # Setup the runner group.
        offline_runner_group = OfflineEvaluationRunnerGroup(
            config=self.config,
            local_runner=False,
            module_state=module_state,
        )

        # Ensure we have indeed 2 `OfflineEvalautionRunner`s.
        self.assertEqual(
            offline_runner_group.num_runners, self.config.num_offline_eval_runners
        )
        # Make sure we have no local runner.
        self.assertEqual(
            offline_runner_group.num_runners, offline_runner_group.num_remote_runners
        )
        self.assertIsNone(offline_runner_group.local_runner)

        # Make sure that an `OfflineData` instance is created.
        from ray.rllib.offline.offline_data import OfflineData

        self.assertIsInstance(offline_runner_group._offline_data, OfflineData)
        # Ensure that there are as many iterators as there are workers.
        self.assertEqual(
            len(offline_runner_group._offline_data_iterators),
            offline_runner_group.num_runners,
        )
        # Ensure that all iterators are indeed `DataIterator` instances.
        from ray.data.iterator import DataIterator

        for iter in offline_runner_group._offline_data_iterators:
            self.assertIsInstance(iter, DataIterator)

        # Clean up.
        algo.cleanup()

    def test_offline_evaluation_runner_group_run(self):

        algo = self.config.build()

        # The module state is needed for the `OfflinePreLearner`.
        module_state = algo.learner_group._learner.module.get_state()

        # Setup the runner group.
        offline_runner_group = OfflineEvaluationRunnerGroup(
            config=self.config,
            local_runner=False,
            module_state=module_state,
        )

        # Run the runner group and receive metrics.
        metrics = offline_runner_group.foreach_runner(
            "run",
            local_runner=False,
        )
        from ray.rllib.utils.metrics.stats import Stats

        # Ensure that `metrics`` is a list of 2 metric dictionaries.
        self.assertIsInstance(metrics, list)
        self.assertEqual(len(metrics), offline_runner_group.num_runners)
        # Ensure that the `eval_total_loss_key` is part of the runner metrics.
        from ray.rllib.core import ALL_MODULES, DEFAULT_MODULE_ID
        from ray.rllib.offline.offline_evaluation_runner import TOTAL_EVAL_LOSS_KEY
        from ray.rllib.utils.metrics import (
            NUM_ENV_STEPS_SAMPLED,
            NUM_ENV_STEPS_SAMPLED_LIFETIME,
            NUM_MODULE_STEPS_SAMPLED,
            NUM_MODULE_STEPS_SAMPLED_LIFETIME,
        )

        for metric_dict in metrics:
            # Ensure the most generic metrics are contained in the `ResultDict`.
            self.assertIn(TOTAL_EVAL_LOSS_KEY, metric_dict[DEFAULT_MODULE_ID])
            self.assertIn(NUM_ENV_STEPS_SAMPLED, metric_dict[ALL_MODULES])
            self.assertIn(NUM_ENV_STEPS_SAMPLED_LIFETIME, metric_dict[ALL_MODULES])
            self.assertIn(NUM_MODULE_STEPS_SAMPLED, metric_dict[ALL_MODULES])
            self.assertIn(NUM_MODULE_STEPS_SAMPLED_LIFETIME, metric_dict[ALL_MODULES])
            # Ensure all entries are `Stats` instances.
            for metric in metric_dict[DEFAULT_MODULE_ID].values():
                self.assertIsInstance(metric, Stats)

        # Clean up.
        algo.cleanup()

    def test_offline_evaluation_runner_group_with_local_runner(self):

        algo = self.config.build()

        # The module state is needed for the `OfflinePreLearner`.
        module_state = algo.learner_group._learner.module.get_state()

        self.config.evaluation(num_offline_eval_runners=0)
        # Setup the runner group.
        offline_runner_group = OfflineEvaluationRunnerGroup(
            config=self.config,
            local_runner=True,
            module_state=module_state,
        )

        # Ensure that we have a local runner.
        self.assertTrue(
            offline_runner_group.num_runners == offline_runner_group.num_remote_runners
        )
        self.assertIsNotNone(offline_runner_group.local_runner)

        # Make sure that the local runner has also a data split stream iterator.
        self.assertIsNotNone(offline_runner_group.local_runner._dataset_iterator)
        from ray.data.iterator import DataIterator

        self.assertIsInstance(
            offline_runner_group.local_runner._dataset_iterator, DataIterator
        )

        # Ensure that we can run the group together with a local runner.
        metrics = offline_runner_group.foreach_runner(
            "run",
            local_runner=True,
        )
        self.assertEqual(len(metrics), 1)

        # Clean up.
        algo.cleanup()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
