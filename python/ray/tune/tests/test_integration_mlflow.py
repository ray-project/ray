import os
import unittest
from collections import namedtuple
from unittest.mock import patch

from ray.tune.function_runner import wrap_function
from ray.tune.integration.mlflow import MLflowLoggerCallback, MLflowLogger, \
    mlflow_mixin, MLflowTrainableMixin


class MockTrial(
        namedtuple("MockTrial",
                   ["config", "trial_name", "trial_id", "logdir"])):
    def __hash__(self):
        return hash(self.trial_id)

    def __str__(self):
        return self.trial_name


MockRunInfo = namedtuple("MockRunInfo", ["run_id"])


class MockRun:
    def __init__(self, run_id, tags=None):
        self.run_id = run_id
        self.tags = tags
        self.info = MockRunInfo(run_id)
        self.params = []
        self.metrics = []
        self.artifacts = []

    def log_param(self, key, value):
        self.params.append({key: value})

    def log_metric(self, key, value):
        self.metrics.append({key: value})

    def log_artifact(self, artifact):
        self.artifacts.append(artifact)

    def set_terminated(self, status):
        self.terminated = True
        self.status = status


MockExperiment = namedtuple("MockExperiment", ["name", "experiment_id"])


class MockMlflowClient:
    def __init__(self, tracking_uri=None, registry_uri=None):
        self.tracking_uri = tracking_uri
        self.registry_uri = registry_uri
        self.experiments = [MockExperiment("existing_experiment", 0)]
        self.runs = {0: []}
        self.active_run = None

    def set_tracking_uri(self, tracking_uri):
        self.tracking_uri = tracking_uri

    def get_experiment_by_name(self, name):
        try:
            index = self.experiment_names.index(name)
            return self.experiments[index]
        except ValueError:
            return None

    def get_experiment(self, experiment_id):
        experiment_id = int(experiment_id)
        try:
            return self.experiments[experiment_id]
        except IndexError:
            return None

    def create_experiment(self, name):
        experiment_id = len(self.experiments)
        self.experiments.append(MockExperiment(name, experiment_id))
        self.runs[experiment_id] = []
        return experiment_id

    def create_run(self, experiment_id, tags=None):
        experiment_runs = self.runs[experiment_id]
        run_id = (experiment_id, len(experiment_runs))
        run = MockRun(run_id=run_id, tags=tags)
        experiment_runs.append(run)
        return run

    def start_run(self, experiment_id, run_name):
        # Creates new run and sets it as active.
        run = self.create_run(experiment_id)
        self.active_run = run

    def get_mock_run(self, run_id):
        return self.runs[run_id[0]][run_id[1]]

    def log_param(self, run_id, key, value):
        run = self.get_mock_run(run_id)
        run.log_param(key, value)

    def log_metric(self, run_id, key, value, step):
        run = self.get_mock_run(run_id)
        run.log_metric(key, value)

    def log_artifacts(self, run_id, local_dir):
        run = self.get_mock_run(run_id)
        run.log_artifact(local_dir)

    def set_terminated(self, run_id, status):
        run = self.get_mock_run(run_id)
        run.set_terminated(status)

    @property
    def experiment_names(self):
        return [e.name for e in self.experiments]


def clear_env_vars():
    if "MLFLOW_EXPERIMENT_NAME" in os.environ:
        del os.environ["MLFLOW_EXPERIMENT_NAME"]
    if "MLFLOW_EXPERIMENT_ID" in os.environ:
        del os.environ["MLFLOW_EXPERIMENT_ID"]


class MLflowTest(unittest.TestCase):
    @patch("mlflow.tracking.MlflowClient", MockMlflowClient)
    def testMlFlowLoggerCallbackConfig(self):
        # Explicitly pass in all args.
        logger = MLflowLoggerCallback(
            tracking_uri="test1",
            registry_uri="test2",
            experiment_name="test_exp")
        self.assertEqual(logger.client.tracking_uri, "test1")
        self.assertEqual(logger.client.registry_uri, "test2")
        self.assertListEqual(logger.client.experiment_names,
                             ["existing_experiment", "test_exp"])
        self.assertEqual(logger.experiment_id, 1)

        # Check if client recognizes already existing experiment.
        logger = MLflowLoggerCallback(experiment_name="existing_experiment")
        self.assertListEqual(logger.client.experiment_names,
                             ["existing_experiment"])
        self.assertEqual(logger.experiment_id, 0)

        # Pass in experiment name as env var.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_NAME"] = "test_exp"
        logger = MLflowLoggerCallback()
        self.assertListEqual(logger.client.experiment_names,
                             ["existing_experiment", "test_exp"])
        self.assertEqual(logger.experiment_id, 1)

        # Pass in existing experiment name as env var.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_NAME"] = "existing_experiment"
        logger = MLflowLoggerCallback()
        self.assertListEqual(logger.client.experiment_names,
                             ["existing_experiment"])
        self.assertEqual(logger.experiment_id, 0)

        # Pass in existing experiment id as env var.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_ID"] = "0"
        logger = MLflowLoggerCallback()
        self.assertListEqual(logger.client.experiment_names,
                             ["existing_experiment"])
        self.assertEqual(logger.experiment_id, "0")

        # Pass in non existing experiment id as env var.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_ID"] = "500"
        with self.assertRaises(ValueError):
            logger = MLflowLoggerCallback()

        # Experiment name env var should take precedence over id env var.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_NAME"] = "test_exp"
        os.environ["MLFLOW_EXPERIMENT_ID"] = "0"
        logger = MLflowLoggerCallback()
        self.assertListEqual(logger.client.experiment_names,
                             ["existing_experiment", "test_exp"])
        self.assertEqual(logger.experiment_id, 1)

    @patch("mlflow.tracking.MlflowClient", MockMlflowClient)
    def testMlFlowLoggerLogging(self):
        clear_env_vars()
        trial_config = {"par1": 4, "par2": 9.}
        trial = MockTrial(trial_config, "trial1", 0, "artifact")

        logger = MLflowLoggerCallback(
            experiment_name="test1", save_artifact=True)

        # Check if run is created.
        logger.on_trial_start(iteration=0, trials=[], trial=trial)
        # New run should be created for this trial with correct tag.
        mock_run = logger.client.runs[1][0]
        self.assertDictEqual(mock_run.tags, {"trial_name": "trial1"})
        self.assertTupleEqual(mock_run.run_id, (1, 0))
        self.assertTupleEqual(logger._trial_runs[trial], mock_run.run_id)
        # Params should be logged.
        self.assertListEqual(mock_run.params, [{"par1": 4}, {"par2": 9}])

        # When same trial is started again, new run should not be created.
        logger.on_trial_start(iteration=0, trials=[], trial=trial)
        self.assertEqual(len(logger.client.runs[1]), 1)

        # Check metrics are logged properly.
        result = {"metric1": 0.8, "metric2": 1, "metric3": None}
        logger.on_trial_result(0, [], trial, result)
        mock_run = logger.client.runs[1][0]
        # metric3 is not logged since it cannot be converted to float.
        self.assertListEqual(mock_run.metrics, [{
            "metric1": 0.8
        }, {
            "metric2": 1.0
        }])

        # Check that artifact is logged on termination.
        logger.on_trial_complete(0, [], trial)
        mock_run = logger.client.runs[1][0]
        self.assertListEqual(mock_run.artifacts, ["artifact"])
        self.assertTrue(mock_run.terminated)
        self.assertEqual(mock_run.status, "FINISHED")

    @patch("mlflow.tracking.MlflowClient", MockMlflowClient)
    def testMlFlowLegacyLoggerConfig(self):
        mlflow = MockMlflowClient()
        with patch.dict("sys.modules", mlflow=mlflow):
            clear_env_vars()
            trial_config = {"par1": 4, "par2": 9.}
            trial = MockTrial(trial_config, "trial1", 0, "artifact")

            # No experiment_id is passed in config, should raise an error.
            with self.assertRaises(ValueError):
                logger = MLflowLogger(trial_config, "/tmp", trial)

            trial_config.update({
                "logger_config": {
                    "mlflow_tracking_uri": "test_tracking_uri",
                    "mlflow_experiment_id": 0
                }
            })
            trial = MockTrial(trial_config, "trial2", 1, "artifact")
            logger = MLflowLogger(trial_config, "/tmp", trial)
            experiment_logger = logger._trial_experiment_logger
            client = experiment_logger.client
            self.assertEqual(client.tracking_uri, "test_tracking_uri")
            # Check to make sure that a run was created on experiment_id 0.
            self.assertEqual(len(client.runs[0]), 1)
            mock_run = client.runs[0][0]
            self.assertDictEqual(mock_run.tags, {"trial_name": "trial2"})
            self.assertListEqual(mock_run.params, [{"par1": 4}, {"par2": 9}])

    @patch("ray.tune.integration.mlflow._import_mlflow",
           lambda: MockMlflowClient())
    def testMlFlowMixinConfig(self):
        clear_env_vars()
        trial_config = {"par1": 4, "par2": 9.}

        @mlflow_mixin
        def train_fn(config):
            return 1

        train_fn.__mixins__ = (MLflowTrainableMixin, )

        # No MLflow config passed in.
        with self.assertRaises(ValueError):
            wrapped = wrap_function(train_fn)(trial_config)

        trial_config.update({"mlflow": {}})
        # No tracking uri or experiment_id/name passed in.
        with self.assertRaises(ValueError):
            wrapped = wrap_function(train_fn)(trial_config)

        # Invalid experiment-id
        trial_config["mlflow"].update({"experiment_id": "500"})
        # No tracking uri or experiment_id/name passed in.
        with self.assertRaises(ValueError):
            wrapped = wrap_function(train_fn)(trial_config)

        trial_config["mlflow"].update({
            "tracking_uri": "test_tracking_uri",
            "experiment_name": "existing_experiment"
        })
        wrapped = wrap_function(train_fn)(trial_config)
        client = wrapped._mlflow
        self.assertEqual(client.tracking_uri, "test_tracking_uri")
        self.assertTupleEqual(client.active_run.run_id, (0, 0))

        with patch("ray.tune.integration.mlflow._import_mlflow",
                   lambda: client):
            train_fn.__mixins__ = (MLflowTrainableMixin, )
            wrapped = wrap_function(train_fn)(trial_config)
            client = wrapped._mlflow
            self.assertTupleEqual(client.active_run.run_id, (0, 1))

            # Set to experiment that does not already exist.
            # New experiment should be created.
            trial_config["mlflow"]["experiment_name"] = "new_experiment"
            with self.assertRaises(ValueError):
                wrapped = wrap_function(train_fn)(trial_config)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
