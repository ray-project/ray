import os
from typing import Dict, Callable, Optional
import logging

from ray.tune.trainable import Trainable
from ray.tune.logger import Logger, LoggerCallback
from ray.tune.result import TRAINING_ITERATION
from ray.tune.trial import Trial

logger = logging.getLogger(__name__)


def _import_mlflow():
    try:
        import mlflow
    except ImportError:
        mlflow = None
    return mlflow


class MLflowLoggerCallback(LoggerCallback):
    """MLflow Logger to automatically log Tune results and config to MLflow.

    MLflow (https://mlflow.org) Tracking is an open source library for
    recording and querying experiments. This Ray Tune ``LoggerCallback``
    sends information (config parameters, training results & metrics,
    and artifacts) to MLflow for automatic experiment tracking.

    Args:
        tracking_uri (str): The tracking URI for where to manage experiments
            and runs. This can either be a local file path or a remote server.
            This arg gets passed directly to mlflow.tracking.MlflowClient
            initialization. When using Tune in a multi-node setting, make sure
            to set this to a remote server and not a local file path.
        registry_uri (str): The registry URI that gets passed directly to
            mlflow.tracking.MlflowClient initialization.
        experiment_name (str): The experiment name to use for this Tune run.
            If None is passed in here, the Logger will automatically then
            check the MLFLOW_EXPERIMENT_NAME and then the MLFLOW_EXPERIMENT_ID
            environment variables to determine the experiment name.
            If the experiment with the name already exists with MlFlow,
            it will be reused. If not, a new experiment will be created with
            that name.
        save_artifact (bool): If set to True, automatically save the entire
            contents of the Tune local_dir as an artifact to the
            corresponding run in MlFlow.

    Example:

    .. code-block:: python

        from ray.tune.integration.mlflow import MLflowLoggerCallback
        tune.run(
            train_fn,
            config={
                # define search space here
                "parameter_1": tune.choice([1, 2, 3]),
                "parameter_2": tune.choice([4, 5, 6]),
            },
            callbacks=[MLflowLoggerCallback(
                experiment_name="experiment1",
                save_artifact=True)])

    """

    def __init__(self,
                 tracking_uri: Optional[str] = None,
                 registry_uri: Optional[str] = None,
                 experiment_name: Optional[str] = None,
                 save_artifact: bool = False):

        mlflow = _import_mlflow()
        if mlflow is None:
            raise RuntimeError("MLflow has not been installed. Please `pip "
                               "install mlflow` to use the MLflowLogger.")

        from mlflow.tracking import MlflowClient
        self.client = MlflowClient(
            tracking_uri=tracking_uri, registry_uri=registry_uri)

        if experiment_name is None:
            # If no name is passed in, then check env vars.
            # First check if experiment_name env var is set.
            experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME")

        if experiment_name is not None:
            # First check if experiment with name exists.
            experiment = self.client.get_experiment_by_name(experiment_name)
            if experiment is not None:
                # If it already exists then get the id.
                experiment_id = experiment.experiment_id
            else:
                # If it does not exist, create the experiment.
                experiment_id = self.client.create_experiment(
                    name=experiment_name)
        else:
            # No experiment_name is passed in and name env var is not set.
            # Now check the experiment id env var.
            experiment_id = os.environ.get("MLFLOW_EXPERIMENT_ID")
            # Confirm that an experiment with this id exists.
            if experiment_id is None or self.client.get_experiment(
                    experiment_id) is None:
                raise ValueError("No experiment_name passed, "
                                 "MLFLOW_EXPERIMENT_NAME env var is not "
                                 "set, and MLFLOW_EXPERIMENT_ID either "
                                 "is not set or does not exist. Please "
                                 "set one of these to use the "
                                 "MLflowLoggerCallback.")

        # At this point, experiment_id should be set.
        self.experiment_id = experiment_id
        self.save_artifact = save_artifact

        self._trial_runs = {}

    def log_trial_start(self, trial: "Trial"):
        # Create run if not already exists.
        if trial not in self._trial_runs:
            run = self.client.create_run(
                experiment_id=self.experiment_id,
                tags={"trial_name": str(trial)})
            self._trial_runs[trial] = run.info.run_id

        run_id = self._trial_runs[trial]

        # Log the config parameters.
        config = trial.config

        for key, value in config.items():
            self.client.log_param(run_id=run_id, key=key, value=value)

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        run_id = self._trial_runs[trial]
        for key, value in result.items():
            try:
                value = float(value)
            except (ValueError, TypeError):
                logger.debug("Cannot log key {} with value {} since the "
                             "value cannot be converted to float.".format(
                                 key, value))
                continue
            self.client.log_metric(
                run_id=run_id, key=key, value=value, step=iteration)

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        run_id = self._trial_runs[trial]

        # Log the artifact if set_artifact is set to True.
        if self.save_artifact:
            self.client.log_artifacts(run_id, local_dir=trial.logdir)

        # Stop the run once trial finishes.
        status = "FINISHED" if not failed else "FAILED"
        self.client.set_terminated(run_id=run_id, status=status)


class MLflowLogger(Logger):
    """MLflow logger using the deprecated Logger API.

    Requires the experiment configuration to have a MLflow Experiment ID
    or manually set the proper environment variables.
    """

    _experiment_logger_cls = MLflowLoggerCallback

    def _init(self):
        mlflow = _import_mlflow()
        logger_config = self.config.pop("logger_config", {})
        tracking_uri = logger_config.get("mlflow_tracking_uri")
        registry_uri = logger_config.get("mlflow_registry_uri")

        experiment_id = logger_config.get("mlflow_experiment_id")
        if experiment_id is None or not mlflow.get_experiment(experiment_id):
            raise ValueError(
                "You must provide a valid `mlflow_experiment_id` "
                "in your `logger_config` dict in the `config` "
                "dict passed to `tune.run`. "
                "Are you sure you passed in a `experiment_id` and "
                "the experiment exists?")
        else:
            experiment_name = mlflow.get_experiment(experiment_id).name

        self._trial_experiment_logger = self._experiment_logger_cls(
            tracking_uri, registry_uri, experiment_name)

        self._trial_experiment_logger.log_trial_start(self.trial)

    def on_result(self, result: Dict):
        self._trial_experiment_logger.log_trial_result(
            iteration=result.get(TRAINING_ITERATION),
            trial=self.trial,
            result=result)

    def close(self):
        self._trial_experiment_logger.log_trial_end(
            trial=self.trial, failed=False)
        del self._trial_experiment_logger


def mlflow_mixin(func: Callable):
    """mlflow_mixin

    MLflow (https://mlflow.org) Tracking is an open source library for
    recording and querying experiments. This Ray Tune Trainable mixin helps
    initialize the MLflow API for use with the ``Trainable`` class or the
    ``@mlflow_mixin`` function API. This mixin automatically configures MLflow
    and creates a run in the same process as each Tune trial. You can then
    use the mlflow API inside the your training function and it will
    automatically get reported to the correct run.

    For basic usage, just prepend your training function with the
    ``@mlflow_mixin`` decorator:

    .. code-block:: python

        from ray.tune.integration.mlflow import mlflow_mixin

        @mlflow_mixin
        def train_fn(config):
            ...
            mlflow.log_metric(...)

    You can also use MlFlow's autologging feature if using a training
    framework like Pytorch Lightning, XGBoost, etc. More information can be
    found here
    (https://mlflow.org/docs/latest/tracking.html#automatic-logging).

    .. code-block:: python

        from ray.tune.integration.mlflow import mlflow_mixin

        @mlflow_mixin
        def train_fn(config):
            mlflow.autolog()
            xgboost_results = xgb.train(config, ...)

    The MlFlow configuration is done by passing a ``mlflow`` key to
    the ``config`` parameter of ``tune.run()`` (see example below).

    The content of the ``mlflow`` config entry is used to
    configure MlFlow. Here are the keys you can pass in to this config entry:

    Args:
        tracking_uri (str): The tracking URI for MLflow tracking. If using
            Tune in a multi-node setting, make sure to use a remote server for
            tracking.
        experiment_id (str): The id of an already created MLflow experiment.
            All logs from all trials in ``tune.run`` will be reported to this
            experiment. If this is not provided or the experiment with this
            id does not exist, you must provide an``experiment_name``. This
            parameter takes precedence over ``experiment_name``.
        experiment_name (str): The name of an already existing MLflow
            experiment. All logs from all trials in ``tune.run`` will be
            reported to this experiment. If this is not provided, you must
            provide a valid ``experiment_id``.
        token (optional, str): A token to use for HTTP authentication when
            logging to a remote tracking server. This is useful when you
            want to log to a Databricks server, for example. This value will
            be used to set the MLFLOW_TRACKING_TOKEN environment variable on
            all the remote training processes.

    Example:

    .. code-block:: python

        from ray import tune
        from ray.tune.integration.mlflow import mlflow_mixin

        import mlflow

        # Create the MlFlow expriment.
        mlflow.create_experiment("my_experiment")

        @mlflow_mixin
        def train_fn(config):
            for i in range(10):
                loss = config["a"] + config["b"]
                mlflow.log_metric(key="loss", value=loss)
            tune.report(loss=loss, done=True)

        tune.run(
            train_fn,
            config={
                # define search space here
                "a": tune.choice([1, 2, 3]),
                "b": tune.choice([4, 5, 6]),
                # mlflow configuration
                "mlflow": {
                    "experiment_name": "my_experiment",
                    "tracking_uri": mlflow.get_tracking_uri()
                }
            })
    """
    if _import_mlflow() is None:
        raise RuntimeError("MLflow has not been installed. Please `pip "
                           "install mlflow` to use the mlflow_mixin.")
    if hasattr(func, "__mixins__"):
        func.__mixins__ = func.__mixins__ + (MLflowTrainableMixin, )
    else:
        func.__mixins__ = (MLflowTrainableMixin, )
    return func


class MLflowTrainableMixin:
    def __init__(self, config: Dict, *args, **kwargs):
        self._mlflow = _import_mlflow()

        if not isinstance(self, Trainable):
            raise ValueError(
                "The `MLflowTrainableMixin` can only be used as a mixin "
                "for `tune.Trainable` classes. Please make sure your "
                "class inherits from both. For example: "
                "`class YourTrainable(MLflowTrainableMixin)`.")

        super().__init__(config, *args, **kwargs)
        _config = config.copy()
        try:
            mlflow_config = _config.pop("mlflow").copy()
        except KeyError as e:
            raise ValueError(
                "MLflow mixin specified but no configuration has been passed. "
                "Make sure to include a `mlflow` key in your `config` dict "
                "containing at least a `tracking_uri` and either "
                "`experiment_name` or `experiment_id` specification.") from e

        tracking_uri = mlflow_config.pop("tracking_uri", None)
        if tracking_uri is None:
            raise ValueError("MLflow mixin specified but no "
                             "tracking_uri has been "
                             "passed in. Make sure to include a `mlflow` "
                             "key in your `config` dict containing at "
                             "least a `tracking_uri`")
        self._mlflow.set_tracking_uri(tracking_uri)

        # Set the tracking token if one is passed in.
        tracking_token = mlflow_config.pop("token", None)
        if tracking_token is not None:
            os.environ["MLFLOW_TRACKING_TOKEN"] = tracking_token

        # First see if experiment_id is passed in.
        experiment_id = mlflow_config.pop("experiment_id", None)
        if experiment_id is None or self._mlflow.get_experiment(
                experiment_id) is None:
            logger.debug("Either no experiment_id is passed in, or the "
                         "experiment with the given id does not exist. "
                         "Checking experiment_name")
            # Check for name.
            experiment_name = mlflow_config.pop("experiment_name", None)
            if experiment_name is None:
                raise ValueError(
                    "MLflow mixin specified but no "
                    "experiment_name or experiment_id has been "
                    "passed in. Make sure to include a `mlflow` "
                    "key in your `config` dict containing at "
                    "least a `experiment_name` or `experiment_id` "
                    "specification.")
            experiment = self._mlflow.get_experiment_by_name(experiment_name)
            if experiment is not None:
                # Experiment with this name exists.
                experiment_id = experiment.experiment_id
            else:
                raise ValueError("No experiment with the given "
                                 "name: {} or id: {} currently exists. Make "
                                 "sure to first start the MLflow experiment "
                                 "before calling tune.run.".format(
                                     experiment_name, experiment_id))

        self.experiment_id = experiment_id

        run_name = self.trial_name + "_" + self.trial_id
        run_name = run_name.replace("/", "_")
        self._mlflow.start_run(
            experiment_id=self.experiment_id, run_name=run_name)

    def stop(self):
        self._mlflow.end_run()
