from typing import Dict, Callable, Optional
import logging

import ray
from ray.tune.trainable import Trainable
from ray.tune.logger import LoggerCallback
from ray.tune.result import TRAINING_ITERATION, TIMESTEPS_TOTAL
from ray.tune.trial import Trial
from ray.util.ml_utils.mlflow import MLflowLoggerUtil

logger = logging.getLogger(__name__)


class MLflowLoggerCallback(LoggerCallback):
    """MLflow Logger to automatically log Tune results and config to MLflow.

    MLflow (https://mlflow.org) Tracking is an open source library for
    recording and querying experiments. This Ray Tune ``LoggerCallback``
    sends information (config parameters, training results & metrics,
    and artifacts) to MLflow for automatic experiment tracking.

    Args:
        tracking_uri: The tracking URI for where to manage experiments
            and runs. This can either be a local file path or a remote server.
            This arg gets passed directly to mlflow
            initialization. When using Tune in a multi-node setting, make sure
            to set this to a remote server and not a local file path.
        registry_uri: The registry URI that gets passed directly to
            mlflow initialization.
        experiment_name: The experiment name to use for this Tune run.
            If the experiment with the name already exists with MLflow,
            it will be reused. If not, a new experiment will be created with
            that name.
        tags: An optional dictionary of string keys and values to set
            as tags on the run
        save_artifact: If set to True, automatically save the entire
            contents of the Tune local_dir as an artifact to the
            corresponding run in MlFlow.

    Example:

    .. code-block:: python

        from ray.tune.integration.mlflow import MLflowLoggerCallback

        tags = { "user_name" : "John",
                 "git_commit_hash" : "abc123"}

        tune.run(
            train_fn,
            config={
                # define search space here
                "parameter_1": tune.choice([1, 2, 3]),
                "parameter_2": tune.choice([4, 5, 6]),
            },
            callbacks=[MLflowLoggerCallback(
                experiment_name="experiment1",
                tags=tags,
                save_artifact=True)])

    """

    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        registry_uri: Optional[str] = None,
        experiment_name: Optional[str] = None,
        tags: Optional[Dict] = None,
        save_artifact: bool = False,
    ):

        self.tracking_uri = tracking_uri
        self.registry_uri = registry_uri
        self.experiment_name = experiment_name
        self.tags = tags
        self.should_save_artifact = save_artifact

        self.mlflow_util = MLflowLoggerUtil()

        if ray.util.client.ray.is_connected():
            logger.warning(
                "When using MLflowLoggerCallback with Ray Client, "
                "it is recommended to use a remote tracking "
                "server. If you are using a MLflow tracking server "
                "backed by the local filesystem, then it must be "
                "setup on the server side and not on the client "
                "side."
            )

    def setup(self, *args, **kwargs):
        # Setup the mlflow logging util.
        self.mlflow_util.setup_mlflow(
            tracking_uri=self.tracking_uri,
            registry_uri=self.registry_uri,
            experiment_name=self.experiment_name,
        )

        if self.tags is None:
            # Create empty dictionary for tags if not given explicitly
            self.tags = {}

        self._trial_runs = {}

    def log_trial_start(self, trial: "Trial"):
        # Create run if not already exists.
        if trial not in self._trial_runs:

            # Set trial name in tags
            tags = self.tags.copy()
            tags["trial_name"] = str(trial)

            run = self.mlflow_util.start_run(tags=tags, run_name=str(trial))
            self._trial_runs[trial] = run.info.run_id

        run_id = self._trial_runs[trial]

        # Log the config parameters.
        config = trial.config
        self.mlflow_util.log_params(run_id=run_id, params_to_log=config)

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        step = result.get(TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]
        run_id = self._trial_runs[trial]
        self.mlflow_util.log_metrics(run_id=run_id, metrics_to_log=result, step=step)

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        run_id = self._trial_runs[trial]

        # Log the artifact if set_artifact is set to True.
        if self.should_save_artifact:
            self.mlflow_util.save_artifacts(run_id=run_id, dir=trial.logdir)

        # Stop the run once trial finishes.
        status = "FINISHED" if not failed else "FAILED"
        self.mlflow_util.end_run(run_id=run_id, status=status)


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
    if ray.util.client.ray.is_connected():
        logger.warning(
            "When using mlflow_mixin with Ray Client, "
            "it is recommended to use a remote tracking "
            "server. If you are using a MLflow tracking server "
            "backed by the local filesystem, then it must be "
            "setup on the server side and not on the client "
            "side."
        )
    if hasattr(func, "__mixins__"):
        func.__mixins__ = func.__mixins__ + (MLflowTrainableMixin,)
    else:
        func.__mixins__ = (MLflowTrainableMixin,)
    return func


class MLflowTrainableMixin:
    def __init__(self, config: Dict, *args, **kwargs):
        self.mlflow_util = MLflowLoggerUtil()

        if not isinstance(self, Trainable):
            raise ValueError(
                "The `MLflowTrainableMixin` can only be used as a mixin "
                "for `tune.Trainable` classes. Please make sure your "
                "class inherits from both. For example: "
                "`class YourTrainable(MLflowTrainableMixin)`."
            )

        super().__init__(config, *args, **kwargs)
        _config = config.copy()
        try:
            mlflow_config = _config.pop("mlflow").copy()
        except KeyError as e:
            raise ValueError(
                "MLflow mixin specified but no configuration has been passed. "
                "Make sure to include a `mlflow` key in your `config` dict "
                "containing at least a `tracking_uri` and either "
                "`experiment_name` or `experiment_id` specification."
            ) from e

        tracking_uri = mlflow_config.pop("tracking_uri", None)
        if tracking_uri is None:
            raise ValueError(
                "MLflow mixin specified but no "
                "tracking_uri has been "
                "passed in. Make sure to include a `mlflow` "
                "key in your `config` dict containing at "
                "least a `tracking_uri`"
            )

        # Set the tracking token if one is passed in.
        tracking_token = mlflow_config.pop("token", None)

        experiment_id = mlflow_config.pop("experiment_id", None)

        experiment_name = mlflow_config.pop("experiment_name", None)

        # This initialization happens in each of the Trainables/workers.
        # So we have to set `create_experiment_if_not_exists` to False.
        # Otherwise there might be race conditions when each worker tries to
        # create the same experiment.
        # For the mixin, the experiment must be created beforehand.
        self.mlflow_util.setup_mlflow(
            tracking_uri=tracking_uri,
            experiment_id=experiment_id,
            experiment_name=experiment_name,
            tracking_token=tracking_token,
            create_experiment_if_not_exists=False,
        )

        run_name = self.trial_name + "_" + self.trial_id
        run_name = run_name.replace("/", "_")
        self.mlflow_util.start_run(set_active=True, run_name=run_name)

    def stop(self):
        self.mlflow_util.end_run()
