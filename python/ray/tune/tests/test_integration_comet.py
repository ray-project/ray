import unittest
from unittest.mock import patch
from ray.tune.integration.comet import CometLoggerCallback
from collections import namedtuple


class MockTrial(
    namedtuple("MockTrial", ["config", "trial_name", "trial_id", "logdir"])
):
    def __hash__(self):
        return hash(self.trial_id)

    def __str__(self):
        return self.trial_name


class InitializationTests(unittest.TestCase):
    def setUp(self):
        self.logger = CometLoggerCallback()

    def test_class_variable_to_instance(self):
        """Test that class variables get properly assigned to instance
        variables.
        """
        logger = self.logger
        self.assertEqual(logger._to_exclude, logger._exclude_results)
        self.assertEqual(logger._to_system, logger._system_results)
        self.assertEqual(logger._to_other, logger._other_results)
        self.assertEqual(logger._to_episodes, logger._episode_results)

    def test_configure_experiment_defaults(self):
        """Test CometLoggerCallback._configure_experiment_defaults."""
        logger = self.logger

        # Test that autologging features are properly disabled
        exclude = CometLoggerCallback._exclude_autolog
        for option in exclude:
            self.assertFalse(logger.experiment_kwargs.get(option))
        del logger

        # Don't disable logging if user overwrites defaults by passing in args
        for include_option in exclude:
            # This unpacks to become e.g. CometLoggerCallback(log_env_cpu=True)
            logger = CometLoggerCallback(**{include_option: True})
            for option in exclude:
                if option == include_option:
                    self.assertTrue(logger.experiment_kwargs.get(option))
                else:
                    self.assertFalse(logger.experiment_kwargs.get(option))


class HelperMethodTests(unittest.TestCase):
    def setUp(self):
        self.logger = CometLoggerCallback()

    def test_check_key_name(self):

        logger = self.logger
        # Return True when key == item
        self.assertTrue(logger._check_key_name("name", "name"))
        # Return True when key.startswith(item + "/")
        self.assertTrue(logger._check_key_name("name/", "name"))
        # Return False when item.startswith(key + "/")
        self.assertFalse(logger._check_key_name("name", "name/"))
        # Return False when key != item and not key.startswith(item."/")
        self.assertFalse(logger._check_key_name("name", "x"))


@patch("comet_ml.OfflineExperiment")
@patch("comet_ml.Experiment")
class OnlineVsOfflineTests(unittest.TestCase):
    def setUp(self):
        self.loggers = {
            "online": CometLoggerCallback(),
            "offline": CometLoggerCallback(online=False),
        }

        self.trial = MockTrial({"p1": 1}, "trial_1", 1, "artifact")

    def test_online_dispatch(self, experiment, offline_experiment):

        # To start, there should be no experiments
        experiment.assert_not_called()
        offline_experiment.assert_not_called()

        # Start online experiment
        logger = self.loggers["online"]
        logger.log_trial_start(self.trial)

        # Check that Experiment was called and OfflineExperiment was not
        experiment.assert_called_once()
        offline_experiment.assert_not_called()

    def test_offline_dispatch(self, experiment, offline_experiment):

        # To start, there should be no experiments
        experiment.assert_not_called()
        offline_experiment.assert_not_called()

        # Start online experiment
        logger = self.loggers["offline"]
        logger.log_trial_start(self.trial)

        # Check that Experiment was called and OfflineExperiment was not
        experiment.assert_not_called()
        offline_experiment.assert_called_once()


@patch("comet_ml.OfflineExperiment")
@patch("comet_ml.Experiment")
class LogTrialStartTest(unittest.TestCase):
    def setUp(self):
        self.loggers = {
            "online": CometLoggerCallback(),
            "offline": CometLoggerCallback(online=False),
        }

        self.trials = [
            MockTrial({"p1": 1}, "trial_1", 1, "artifact"),
            MockTrial({"p1": 2}, "trial_2", 1, "artifact"),
        ]

    def test_existing_trialexperiment(self, experiment, offline_experiment):

        mocks = {"online": experiment, "offline": offline_experiment}
        for option in ["online", "offline"]:
            logger = self.loggers[option]
            mock = mocks[option]

            # This should create an experiment
            logger.log_trial_start(self.trials[0])
            mock.assert_called_once()

            # This should NOT create an experiment because it's the same trial
            logger.log_trial_start(self.trials[0])
            mock.assert_called_once()

            # This should create another new experiment
            logger.log_trial_start(self.trials[1])

            # Number of times the mock was called
            num_calls = len(mock.call_args_list)

            # Assert that Experiment/OfflineExperiment was called twice
            self.assertEqual(num_calls, 2)

    def test_set_global_experiment(self, experiment, offline_experiment):
        for option in ["online", "offline"]:
            logger = self.loggers[option]
            with patch("comet_ml.config.set_global_experiment") as mock:
                logger.log_trial_start(self.trials[0])
                mock.assert_called_with(None)
                mock.assert_called_once()
                mock.reset_mock()

    def test_experiment_addtags(self, experiment, offline_experiment):
        logger = self.loggers["online"]
        logger.log_trial_start(self.trials[0])
        experiment.return_value.add_tags.assert_called_with(logger.tags)

    def test_experiment_setname(self, experiment, offline_experiment):
        logger = self.loggers["online"]
        trial = self.trials[0]
        logger.log_trial_start(trial)
        experiment.return_value.set_name.assert_called_with(trial.trial_name)

    def test_experiment_logparams(self, experiment, offline_experiment):
        logger = self.loggers["online"]
        trial = self.trials[0]
        logger.log_trial_start(trial)
        config = trial.config.copy()
        config.pop("callbacks", None)
        experiment.return_value.log_parameters.assert_called_with(config)


class ExperimentKwargsTest(unittest.TestCase):
    @patch("comet_ml.Experiment")
    def test_kwargs_passthrough(self, experiment):
        """Test that additional keyword arguments to CometLoggerCallback get
        passed through to comet_ml.Experiment on log_trial_start
        """
        experiment_kwargs = {"kwarg_1": "val_1"}
        logger = CometLoggerCallback(**experiment_kwargs)
        trial = MockTrial({"parameter": 1}, "trial2", 1, "artifact")
        logger.log_trial_start(trial)

        # These are the default kwargs that get passed to create the experiment
        expected_kwargs = {kwarg: False for kwarg in logger._exclude_autolog}
        expected_kwargs.update(experiment_kwargs)

        experiment.assert_called_with(**expected_kwargs)


@patch("comet_ml.Experiment")
class LogTrialResultTests(unittest.TestCase):
    """
    * test log_others logs
    * test log_system logs
    * test log_curve logs
    """

    def setUp(self):
        self.logger = CometLoggerCallback()
        self.trials = [
            MockTrial({"p1": 1}, "trial_1", 1, "artifact"),
            MockTrial({"p1": 2}, "trial_2", 1, "artifact"),
        ]
        self.result = {
            "config": {"p1": 1},
            "node_ip": "0.0.0.0",
            "hostname": "hostname_val",
            "pid": "1234",
            "date": "2000-01-01",
            "experiment_id": "1234",
            "trial_id": 1,
            "experiment_tag": "tag1",
            "hist_stats/episode_reward": [1, 0, 1, -1, 0, 1],
            "hist_stats/episode_lengths": [1, 2, 3, 4, 5, 6],
            "metric1": 0.8,
            "metric2": 1,
            "metric3": None,
            "training_iteration": 0,
        }

    def test_log_parameters(self, experiment):
        logger = self.logger
        trial = self.trials[0]
        result = self.result.copy()

        # Check parameters are logged properly.
        logger.log_trial_result(1, trial, self.result)

        config_update = result.copy().pop("config", {})
        config_update.pop("callbacks", None)  # Remove callbacks

        experiment.return_value.log_parameters.assert_any_call(config_update)

    def test_log_metrics(self, experiment):
        logger = self.logger
        trial = self.trials[0]
        result = self.result.copy()
        step = result["training_iteration"]

        logger.log_trial_result(1, trial, self.result)
        result_metrics = {
            "metric1": 0.8,
            "metric2": 1,
            "metric3": None,
            "training_iteration": 0,
        }

        method = experiment.return_value.log_metrics
        method.assert_any_call(result_metrics, step=step)

    def test_log_other(self, experiment):
        logger = self.logger
        trial = self.trials[0]
        result = self.result.copy()

        logger.log_trial_result(1, trial, result)
        result_other = {
            "experiment_id": "1234",
            "trial_id": 1,
            "experiment_tag": "tag1",
        }
        method = experiment.return_value.log_others

        # for k,v in result_other.items():
        method.assert_any_call(result_other)

    def test_log_system(self, experiment):
        logger = self.logger
        trial = self.trials[0]
        result = self.result.copy()

        logger.log_trial_result(1, trial, result)
        result_system = {
            "node_ip": "0.0.0.0",
            "hostname": "hostname_val",
            "pid": "1234",
            "date": "2000-01-01",
        }
        method = experiment.return_value.log_system_info
        for k, v in result_system.items():
            method.assert_any_call(k, v)

    def test_log_curve(self, experiment):
        logger = self.logger
        trial = self.trials[0]

        # Check parameters are logged properly.
        result = self.result
        step = result["training_iteration"]
        logger.log_trial_result(1, trial, result)

        results_curve = {
            "hist_stats/episode_reward": [1, 0, 1, -1, 0, 1],
            "hist_stats/episode_lengths": [1, 2, 3, 4, 5, 6],
        }

        method = experiment.return_value.log_curve
        print(method.call_args_list)
        for k, v in results_curve.items():

            method.assert_any_call(k, x=range(len(v)), y=v, step=step)


@patch("comet_ml.Experiment")
class LogTrialEndTests(unittest.TestCase):
    def setUp(self):
        self.logger = CometLoggerCallback()
        self.trials = [
            MockTrial({"p1": 1}, "trial_1", 1, "artifact"),
            MockTrial({"p1": 2}, "trial_2", 2, "artifact"),
            MockTrial({"p1": 2}, "trial_3", 3, "artifact"),
        ]

    def test_not_started_exception(self, experiment):
        logger = self.logger
        with self.assertRaises(KeyError):
            logger.log_trial_end(self.trials[0])

    def test_repeat_throws_error(self, experiment):
        logger = self.logger
        trial = self.trials[0]

        logger.log_trial_start(trial)
        logger.log_trial_end(trial)
        with self.assertRaises(KeyError):
            logger.log_trial_end(trial)

    def test_log_trial_end(self, experiment):
        logger = self.logger
        trials = self.trials
        method = experiment.return_value.end

        # Should not have ended yet
        method.assert_not_called()

        for trial in trials:
            logger.log_trial_start(trial)
            logger.log_trial_end(trial)

        self.assertEqual(len(method.call_args_list), len(trials))

    def test_del(self, experiment):
        logger = self.logger

        for trial in self.trials:
            logger.log_trial_start(trial)

        end = experiment.return_value.end
        end.assert_not_called()

        logger.__del__()

        self.assertEqual(len(end.call_args_list), len(self.trials))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
