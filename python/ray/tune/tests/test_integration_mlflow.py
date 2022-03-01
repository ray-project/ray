import os
import tempfile
import unittest
from collections import namedtuple
from unittest.mock import patch

from ray.tune.function_runner import wrap_function
from ray.tune.integration.mlflow import (
    MLflowLoggerCallback,
    mlflow_mixin,
    MLflowTrainableMixin,
)

from mlflow.tracking import MlflowClient
from ray.util.ml_utils.mlflow import MLflowLoggerUtil


class MockTrial(
    namedtuple("MockTrial", ["config", "trial_name", "trial_id", "logdir"])
):
    def __hash__(self):
        return hash(self.trial_id)

    def __str__(self):
        return self.trial_name


class MockMLflowLoggerUtil(MLflowLoggerUtil):
    def save_artifacts(self, dir, run_id):
        self.artifact_saved = True
        self.artifact_info = {"dir": dir, "run_id": run_id}


def clear_env_vars():
    if "MLFLOW_EXPERIMENT_NAME" in os.environ:
        del os.environ["MLFLOW_EXPERIMENT_NAME"]
    if "MLFLOW_EXPERIMENT_ID" in os.environ:
        del os.environ["MLFLOW_EXPERIMENT_ID"]


class MLflowTest(unittest.TestCase):
    def setUp(self):
        self.tracking_uri = tempfile.mkdtemp()
        self.registry_uri = tempfile.mkdtemp()

        client = MlflowClient(
            tracking_uri=self.tracking_uri, registry_uri=self.registry_uri
        )
        client.create_experiment(name="existing_experiment")
        assert client.get_experiment_by_name("existing_experiment").experiment_id == "0"

    def testMlFlowLoggerCallbackConfig(self):
        # Explicitly pass in all args.
        logger = MLflowLoggerCallback(
            tracking_uri=self.tracking_uri,
            registry_uri=self.registry_uri,
            experiment_name="test_exp",
        )
        logger.setup()
        self.assertEqual(
            logger.mlflow_util._mlflow.get_tracking_uri(), self.tracking_uri
        )
        self.assertEqual(
            logger.mlflow_util._mlflow.get_registry_uri(), self.registry_uri
        )
        self.assertListEqual(
            [e.name for e in logger.mlflow_util._mlflow.list_experiments()],
            ["existing_experiment", "test_exp"],
        )
        self.assertEqual(logger.mlflow_util.experiment_id, "1")

        # Check if client recognizes already existing experiment.
        logger = MLflowLoggerCallback(
            experiment_name="existing_experiment",
            tracking_uri=self.tracking_uri,
            registry_uri=self.registry_uri,
        )
        logger.setup()
        self.assertEqual(logger.mlflow_util.experiment_id, "0")

        # Pass in experiment name as env var.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_NAME"] = "test_exp"
        logger = MLflowLoggerCallback(
            tracking_uri=self.tracking_uri, registry_uri=self.registry_uri
        )
        logger.setup()
        self.assertEqual(logger.mlflow_util.experiment_id, "1")

        # Pass in existing experiment name as env var.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_NAME"] = "existing_experiment"
        logger = MLflowLoggerCallback(
            tracking_uri=self.tracking_uri, registry_uri=self.registry_uri
        )
        logger.setup()
        self.assertEqual(logger.mlflow_util.experiment_id, "0")

        # Pass in existing experiment id as env var.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_ID"] = "0"
        logger = MLflowLoggerCallback(
            tracking_uri=self.tracking_uri, registry_uri=self.registry_uri
        )
        logger.setup()
        self.assertEqual(logger.mlflow_util.experiment_id, "0")

        # Pass in non existing experiment id as env var.
        # This should create a new experiment.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_ID"] = "500"
        with self.assertRaises(ValueError):
            logger = MLflowLoggerCallback(
                tracking_uri=self.tracking_uri, registry_uri=self.registry_uri
            )
            logger.setup()

        # Experiment id env var should take precedence over name env var.
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_NAME"] = "test_exp"
        os.environ["MLFLOW_EXPERIMENT_ID"] = "0"
        logger = MLflowLoggerCallback(
            tracking_uri=self.tracking_uri, registry_uri=self.registry_uri
        )
        logger.setup()
        self.assertEqual(logger.mlflow_util.experiment_id, "0")

        # Using tags
        tags = {"user_name": "John", "git_commit_hash": "abc123"}
        clear_env_vars()
        os.environ["MLFLOW_EXPERIMENT_NAME"] = "test_tags"
        os.environ["MLFLOW_EXPERIMENT_ID"] = "0"
        logger = MLflowLoggerCallback(
            tracking_uri=self.tracking_uri, registry_uri=self.registry_uri, tags=tags
        )
        logger.setup()
        self.assertEqual(logger.tags, tags)

    @patch("ray.tune.integration.mlflow.MLflowLoggerUtil", MockMLflowLoggerUtil)
    def testMlFlowLoggerLogging(self):
        clear_env_vars()
        trial_config = {"par1": "a", "par2": "b"}
        trial = MockTrial(trial_config, "trial1", 0, "artifact")

        logger = MLflowLoggerCallback(
            tracking_uri=self.tracking_uri,
            registry_uri=self.registry_uri,
            experiment_name="test1",
            save_artifact=True,
            tags={"hello": "world"},
        )
        logger.setup()

        # Check if run is created with proper tags.
        logger.on_trial_start(iteration=0, trials=[], trial=trial)
        all_runs = logger.mlflow_util._mlflow.search_runs(experiment_ids=["1"])
        self.assertEqual(len(all_runs), 1)
        # all_runs is a pandas dataframe.
        all_runs = all_runs.to_dict(orient="records")
        run = logger.mlflow_util._mlflow.get_run(all_runs[0]["run_id"])
        self.assertDictEqual(
            run.data.tags,
            {"hello": "world", "trial_name": "trial1", "mlflow.runName": "trial1"},
        )
        self.assertEqual(logger._trial_runs[trial], run.info.run_id)
        # Params should be logged.
        self.assertDictEqual(run.data.params, trial_config)

        # When same trial is started again, new run should not be created.
        logger.on_trial_start(iteration=0, trials=[], trial=trial)
        all_runs = logger.mlflow_util._mlflow.search_runs(experiment_ids=["1"])
        self.assertEqual(len(all_runs), 1)

        # Check metrics are logged properly.
        result = {
            "metric1": 0.8,
            "metric2": 1,
            "metric3": None,
            "training_iteration": 0,
        }
        logger.on_trial_result(0, [], trial, result)
        run = logger.mlflow_util._mlflow.get_run(run_id=run.info.run_id)
        # metric3 is not logged since it cannot be converted to float.
        self.assertDictEqual(
            run.data.metrics, {"metric1": 0.8, "metric2": 1.0, "training_iteration": 0}
        )

        # Check that artifact is logged on termination.
        logger.on_trial_complete(0, [], trial)
        self.assertTrue(logger.mlflow_util.artifact_saved)
        self.assertDictEqual(
            logger.mlflow_util.artifact_info,
            {"dir": "artifact", "run_id": run.info.run_id},
        )

    def testMlFlowMixinConfig(self):
        clear_env_vars()
        trial_config = {"par1": 4, "par2": 9.0}

        @mlflow_mixin
        def train_fn(config):
            return 1

        train_fn.__mixins__ = (MLflowTrainableMixin,)

        # No MLflow config passed in.
        with self.assertRaises(ValueError):
            wrap_function(train_fn)(trial_config)

        trial_config.update({"mlflow": {}})
        # No tracking uri or experiment_id/name passed in.
        with self.assertRaises(ValueError):
            wrap_function(train_fn)(trial_config)

        # Invalid experiment-id
        trial_config["mlflow"].update({"experiment_id": "500"})
        # No tracking uri or experiment_id/name passed in.
        with self.assertRaises(ValueError):
            wrap_function(train_fn)(trial_config)

        # Set to experiment that does not already exist.
        # New experiment should be created.
        trial_config["mlflow"]["experiment_name"] = "new_experiment"
        with self.assertRaises(ValueError):
            wrap_function(train_fn)(trial_config)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
