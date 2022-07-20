import os
import shutil
import tempfile
import unittest

from ray.util.ml_utils.mlflow import _MLflowLoggerUtil


class MLflowTest(unittest.TestCase):
    def setUp(self):
        self.dirpath = tempfile.mkdtemp()
        import mlflow

        mlflow.set_tracking_uri(self.dirpath)
        mlflow.create_experiment(name="existing_experiment")

        self.mlflow_util = _MLflowLoggerUtil()
        self.tracking_uri = mlflow.get_tracking_uri()

    def tearDown(self):
        shutil.rmtree(self.dirpath)

    def test_experiment_id(self):
        self.mlflow_util.setup_mlflow(tracking_uri=self.tracking_uri, experiment_id="0")
        assert self.mlflow_util.experiment_id == "0"

    def test_experiment_id_env_var(self):
        os.environ["MLFLOW_EXPERIMENT_ID"] = "0"
        self.mlflow_util.setup_mlflow(tracking_uri=self.tracking_uri)
        assert self.mlflow_util.experiment_id == "0"
        del os.environ["MLFLOW_EXPERIMENT_ID"]

    def test_experiment_name(self):
        self.mlflow_util.setup_mlflow(
            tracking_uri=self.tracking_uri, experiment_name="existing_experiment"
        )
        assert self.mlflow_util.experiment_id == "0"

    def test_run_started_with_correct_experiment(self):
        experiment_name = "my_experiment_name"
        # Make sure run is started under the correct experiment.
        self.mlflow_util.setup_mlflow(
            tracking_uri=self.tracking_uri, experiment_name=experiment_name
        )
        run = self.mlflow_util.start_run(set_active=True)
        assert (
            run.info.experiment_id
            == self.mlflow_util._mlflow.get_experiment_by_name(
                experiment_name
            ).experiment_id
        )

        self.mlflow_util.end_run()

    def test_experiment_name_env_var(self):
        os.environ["MLFLOW_EXPERIMENT_NAME"] = "existing_experiment"
        self.mlflow_util.setup_mlflow(tracking_uri=self.tracking_uri)
        assert self.mlflow_util.experiment_id == "0"
        del os.environ["MLFLOW_EXPERIMENT_NAME"]

    def test_id_precedence(self):
        os.environ["MLFLOW_EXPERIMENT_ID"] = "0"
        self.mlflow_util.setup_mlflow(
            tracking_uri=self.tracking_uri, experiment_name="new_experiment"
        )
        assert self.mlflow_util.experiment_id == "0"
        del os.environ["MLFLOW_EXPERIMENT_ID"]

    def test_new_experiment(self):
        self.mlflow_util.setup_mlflow(
            tracking_uri=self.tracking_uri, experiment_name="new_experiment"
        )
        assert self.mlflow_util.experiment_id == "1"

    def test_setup_fail(self):
        with self.assertRaises(ValueError):
            self.mlflow_util.setup_mlflow(
                tracking_uri=self.tracking_uri,
                experiment_name="new_experiment2",
                create_experiment_if_not_exists=False,
            )

    def test_log_params(self):
        params = {"a": "a"}
        self.mlflow_util.setup_mlflow(
            tracking_uri=self.tracking_uri, experiment_name="new_experiment"
        )
        run = self.mlflow_util.start_run()
        run_id = run.info.run_id
        self.mlflow_util.log_params(params_to_log=params, run_id=run_id)

        run = self.mlflow_util._mlflow.get_run(run_id=run_id)
        assert run.data.params == params

        params2 = {"b": "b"}
        self.mlflow_util.start_run(set_active=True)
        self.mlflow_util.log_params(params_to_log=params2, run_id=run_id)
        run = self.mlflow_util._mlflow.get_run(run_id=run_id)
        assert run.data.params == {
            **params,
            **params2,
        }

        self.mlflow_util.end_run()

    def test_log_metrics(self):
        metrics = {"a": 1.0}
        self.mlflow_util.setup_mlflow(
            tracking_uri=self.tracking_uri, experiment_name="new_experiment"
        )
        run = self.mlflow_util.start_run()
        run_id = run.info.run_id
        self.mlflow_util.log_metrics(metrics_to_log=metrics, run_id=run_id, step=0)

        run = self.mlflow_util._mlflow.get_run(run_id=run_id)
        assert run.data.metrics == metrics

        metrics2 = {"b": 1.0}
        self.mlflow_util.start_run(set_active=True)
        self.mlflow_util.log_metrics(metrics_to_log=metrics2, run_id=run_id, step=0)
        assert self.mlflow_util._mlflow.get_run(run_id=run_id).data.metrics == {
            **metrics,
            **metrics2,
        }
        self.mlflow_util.end_run()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
