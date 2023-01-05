import logging
import warnings
from types import ModuleType
from typing import Dict, Optional, Union

import ray
from ray.air import session

from ray.air._internal.mlflow import _MLflowLoggerUtil
from ray.tune.logger import LoggerCallback
from ray.tune.result import TIMESTEPS_TOTAL, TRAINING_ITERATION
from ray.tune.experiment import Trial
from ray.util.annotations import PublicAPI

try:
    import mlflow
except ImportError:
    mlflow = None


logger = logging.getLogger(__name__)


class _NoopModule:
    def __getattr__(self, item):
        return _NoopModule()

    def __call__(self, *args, **kwargs):
        return None


@PublicAPI(stability="alpha")
def setup_mlflow(
    config: Optional[Dict] = None,
    tracking_uri: Optional[str] = None,
    registry_uri: Optional[str] = None,
    experiment_id: Optional[str] = None,
    experiment_name: Optional[str] = None,
    tracking_token: Optional[str] = None,
    create_experiment_if_not_exists: bool = False,
    tags: Optional[Dict] = None,
    rank_zero_only: bool = True,
) -> Union[ModuleType, _NoopModule]:
    """Set up a MLflow session.

    This function can be used to initialize an MLflow session in a
    (distributed) training or tuning run.

    By default, the MLflow experiment ID is the Ray trial ID and the
    MLlflow experiment name is the Ray trial name. These settings can be overwritten by
    passing the respective keyword arguments.

    The ``config`` dict is automatically logged as the run parameters (excluding the
    mlflow settings).

    In distributed training with Ray Train, only the zero-rank worker will initialize
    mlflow. All other workers will return a noop client, so that logging is not
    duplicated in a distributed run. This can be disabled by passing
    ``rank_zero_only=False``, which will then initialize mlflow in every training
    worker.

    This function will return the ``mlflow`` module or a noop module for
    non-rank zero workers ``if rank_zero_only=True``. By using
    ``mlflow = setup_mlflow(config)`` you can ensure that only the rank zero worker
    calls the mlflow API.

    Args:
        config: Configuration dict to be logged to mlflow as parameters.
        tracking_uri: The tracking URI for MLflow tracking. If using
            Tune in a multi-node setting, make sure to use a remote server for
            tracking.
        registry_uri: The registry URI for the MLflow model registry.
        experiment_id: The id of an already created MLflow experiment.
            All logs from all trials in ``tune.Tuner()`` will be reported to this
            experiment. If this is not provided or the experiment with this
            id does not exist, you must provide an``experiment_name``. This
            parameter takes precedence over ``experiment_name``.
        experiment_name: The name of an already existing MLflow
            experiment. All logs from all trials in ``tune.Tuner()`` will be
            reported to this experiment. If this is not provided, you must
            provide a valid ``experiment_id``.
        tracking_token: A token to use for HTTP authentication when
            logging to a remote tracking server. This is useful when you
            want to log to a Databricks server, for example. This value will
            be used to set the MLFLOW_TRACKING_TOKEN environment variable on
            all the remote training processes.
        create_experiment_if_not_exists: Whether to create an
            experiment with the provided name if it does not already
            exist. Defaults to False.
        tags: Tags to set for the new run.
        rank_zero_only: If True, will return an initialized session only for the
            rank 0 worker in distributed training. If False, will initialize a
            session for all workers. Defaults to True.

    Example:

        Per default, you can just call ``setup_mlflow`` and continue to use
        MLflow like you would normally do:

        .. code-block:: python

            from ray.air.integrations.mlflow import setup_mlflow

            def training_loop(config):
                setup_mlflow(config)
                # ...
                mlflow.log_metric(key="loss", val=0.123, step=0)

        In distributed data parallel training, you can utilize the return value of
        ``setup_mlflow``. This will make sure it is only invoked on the first worker
        in distributed training runs.

        .. code-block:: python

            from ray.air.integrations.mlflow import setup_mlflow

            def training_loop(config):
                mlflow = setup_mlflow(config)
                # ...
                mlflow.log_metric(key="loss", val=0.123, step=0)


        You can also use MlFlow's autologging feature if using a training
        framework like Pytorch Lightning, XGBoost, etc. More information can be
        found here
        (https://mlflow.org/docs/latest/tracking.html#automatic-logging).

        .. code-block:: python

            from ray.tune.integration.mlflow import setup_mlflow

            def train_fn(config):
                mlflow = setup_mlflow(config)
                mlflow.autolog()
                xgboost_results = xgb.train(config, ...)

    """
    if not mlflow:
        raise RuntimeError(
            "mlflow was not found - please install with `pip install mlflow`"
        )

    try:
        # Do a try-catch here if we are not in a train session
        _session = session._get_session(warn=False)
        if _session and rank_zero_only and session.get_world_rank() != 0:
            return _NoopModule()

        default_trial_id = session.get_trial_id()
        default_trial_name = session.get_trial_name()

    except RuntimeError:
        default_trial_id = None
        default_trial_name = None

    _config = config.copy() if config else {}
    mlflow_config = _config.pop("mlflow", {}).copy()

    # Deprecate: 2.4
    if mlflow_config:
        warnings.warn(
            "Passing a `mlflow` key in the config dict is deprecated and will raise an "
            "error in the future. Please pass the actual arguments to `setup_mlflow()` "
            "instead.",
            DeprecationWarning,
        )

    experiment_id = experiment_id or default_trial_id
    experiment_name = experiment_name or default_trial_name

    # Setup mlflow
    mlflow_util = _MLflowLoggerUtil()
    mlflow_util.setup_mlflow(
        tracking_uri=tracking_uri or mlflow_config.get("tracking_uri", None),
        registry_uri=registry_uri or mlflow_config.get("registry_uri", None),
        experiment_id=experiment_id or mlflow_config.get("experiment_id", None),
        experiment_name=experiment_name or mlflow_config.get("experiment_name", None),
        tracking_token=tracking_token or mlflow_config.get("tracking_token", None),
        create_experiment_if_not_exists=create_experiment_if_not_exists,
    )

    mlflow_util.start_run(
        run_name=experiment_name,
        tags=tags or mlflow_config.get("tags", None),
        set_active=True,
    )
    mlflow_util.log_params(_config)
    return mlflow_util._mlflow


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
        tracking_token: Tracking token used to authenticate with MLflow.
        save_artifact: If set to True, automatically save the entire
            contents of the Tune local_dir as an artifact to the
            corresponding run in MlFlow.

    Example:

    .. code-block:: python

        from ray.air.integrations.mlflow import MLflowLoggerCallback

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
        *,
        registry_uri: Optional[str] = None,
        experiment_name: Optional[str] = None,
        tags: Optional[Dict] = None,
        tracking_token: Optional[str] = None,
        save_artifact: bool = False,
    ):

        self.tracking_uri = tracking_uri
        self.registry_uri = registry_uri
        self.experiment_name = experiment_name
        self.tags = tags
        self.tracking_token = tracking_token
        self.should_save_artifact = save_artifact

        self.mlflow_util = _MLflowLoggerUtil()

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
            tracking_token=self.tracking_token,
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
