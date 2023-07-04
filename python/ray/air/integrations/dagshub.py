import os
import warnings
from .mlflow import MLflowLoggerCallback, _NoopModule
from types import ModuleType
from typing import Dict, Optional, Union
from ray.tune.experiment import Trial
from ray.tune.result_grid import ResultGrid
from ray.air._internal.dagshub import _DagsHubLoggerUtil
from ray.util.annotations import PublicAPI

from ray.air import session

try:
    import dagshub

    from dagshub.common import config as dagshub_sys_config
except Exception:
    dagshub = None


@PublicAPI(stability="alpha")
def setup_dagshub(
    config: Optional[Dict] = None,
    tracking_uri: Optional[str] = None,
    registry_uri: Optional[str] = None,
    experiment_id: Optional[str] = None,
    experiment_name: Optional[str] = None,
    tracking_token: Optional[str] = None,
    artifact_location: Optional[str] = None,
    run_name: Optional[str] = None,
    create_experiment_if_not_exists: bool = True,
    tags: Optional[Dict] = None,
    rank_zero_only: bool = True,
    dagshub_repository: Optional[str] = None,
    log_mlflow_only: bool = False,
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
    ``mlflow = setup_dagshub(config)`` you can ensure that only the rank zero worker
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
        artifact_location: The location to store run artifacts.
            If not provided, MLFlow picks an appropriate default.
            Ignored if experiment already exists.
        run_name: Name of the new MLflow run that will be created.
            If not set, will default to the ``experiment_name``.
        create_experiment_if_not_exists: Whether to create an
            experiment with the provided name if it does not already
            exist. Defaults to False.
        tags: Tags to set for the new run.
        rank_zero_only: If True, will return an initialized session only for the
            rank 0 worker in distributed training. If False, will initialize a
            session for all workers. Defaults to True.

    Example:

        Per default, you can just call ``setup_dagshub`` and continue to use
        MLflow like you would normally do:

        .. code-block:: python

            from ray.air.integrations.dagshub import setup_dagshub

            def training_loop(config):
                setup_dagshub(config)
                # ...
                mlflow.log_metric(key="loss", val=0.123, step=0)

        In distributed data parallel training, you can utilize the return value of
        ``setup_dagshub``. This will make sure it is only invoked on the first worker
        in distributed training runs.

        .. code-block:: python

            from ray.air.integrations.mlflow import setup_dagshub

            def training_loop(config):
                mlflow = setup_dagshub(config)
                # ...
                mlflow.log_metric(key="loss", val=0.123, step=0)


        You can also use MlFlow's autologging feature if using a training
        framework like Pytorch Lightning, XGBoost, etc. More information can be
        found here
        (https://mlflow.org/docs/latest/tracking.html#automatic-logging).

        .. code-block:: python

            from ray.tune.integration.dagshub import setup_dagshub

            def train_fn(config):
                mlflow = setup_dagshub(config)
                mlflow.autolog()
                xgboost_results = xgb.train(config, ...)

    """
    if not dagshub:
        raise RuntimeError(
            "dagshub was not found - please install with `pip install dagshub`"
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
    mlflow_config = _config.pop("dagshub", {}).copy()

    # Deprecate: 2.4
    if mlflow_config:
        warnings.warn(
            "Passing a `dagshub` key in the config dict is deprecated and will raise an"
            "error in the future. Please pass the actual arguments to `setup_dagshub()`"
            "instead.",
            DeprecationWarning,
        )

    experiment_id = experiment_id or default_trial_id
    experiment_name = experiment_name or default_trial_name

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", None)
    repo_owner = tracking_uri.split(os.sep)[-2]
    repo_name = tracking_uri.split(os.sep)[-1].replace(".mlflow", "")
    dagshub_util = _DagsHubLoggerUtil(
        repo_owner=repo_owner,
        repo_name=repo_name,
        mlflow_only=log_mlflow_only or mlflow_config.get("log_mlflow_only", False),
    )

    dagshub_util.setup_mlflow(
        tracking_uri=tracking_uri or mlflow_config.get("tracking_uri", None),
        registry_uri=registry_uri or mlflow_config.get("registry_uri", None),
        experiment_id=experiment_id or mlflow_config.get("experiment_id", None),
        experiment_name=experiment_name or mlflow_config.get("experiment_name", None),
        tracking_token=tracking_token or mlflow_config.get("tracking_token", None),
        artifact_location=artifact_location
        or mlflow_config.get("artifact_location", None),
        create_experiment_if_not_exists=create_experiment_if_not_exists,
    )
    dagshub_util.start_run(
        run_name=run_name or experiment_name,
        tags=tags or mlflow_config.get("tags", None),
        set_active=True,
    )
    dagshub_util.log_params(_config)
    return dagshub_util._mlflow


@PublicAPI(stability="alpha")
def upload_artifacts(
    results: Optional["ResultGrid"] = None, repo_name: str = "", repo_owner: str = ""
):
    if results is None:
        return
    dagshub_util = _DagsHubLoggerUtil(
        repo_owner=repo_owner, repo_name=repo_name, mlflow_only=False
    )
    # Get all trial IDs
    trial_metas = [(trial.metrics["trial_id"], trial.log_dir) for trial in results]
    for trial_id, trial_dir in trial_metas:
        dagshub_util.save_artifacts(dir=str(trial_dir), run_id=trial_id)
        dagshub_util.upload(run_id=trial_id)


class DagsHubLoggerCallback(MLflowLoggerCallback):
    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        *,
        registry_uri: Optional[str] = None,
        experiment_name: Optional[str] = None,
        tags: Optional[Dict] = None,
        tracking_token: Optional[str] = None,
        save_artifact: bool = True,
        dagshub_repository: Optional[str] = None,
        log_mlflow_only: bool = False,
    ):
        super(DagsHubLoggerCallback, self).__init__(
            tracking_uri=tracking_uri,
            registry_uri=registry_uri,
            experiment_name=experiment_name,
            tags=tags,
            tracking_token=tracking_token,
            save_artifact=save_artifact,
        )

        if dagshub is None:
            raise RuntimeError(
                "dagshub was not found - please install with `pip install dagshub`"
            )

        repo_name, repo_owner = None, None
        self.tracking_uri = os.getenv("MLFLOW_TRACKING_URI", None)
        if dagshub_repository:
            repo_name, repo_owner = self.splitter(dagshub_repository)
        else:
            if self.tracking_uri:
                repo_owner = (self.tracking_uri.split("/")[-2],)
                repo_name = (self.tracking_uri.split("/")[-1].replace(".mlflow", ""),)

        if not repo_name or not repo_owner:
            repo_name, repo_owner = self.splitter(
                input("Please insert your repository owner_name/repo_name:")
            )

        if "MLFLOW_TRACKING_URI" not in os.environ:
            dagshub.init(repo_name=repo_name, repo_owner=repo_owner)
            self.tracking_uri = os.getenv("MLFLOW_TRACKING_URI")

        self.mlflow_util = _DagsHubLoggerUtil(
            repo_owner=repo_owner, repo_name=repo_name, mlflow_only=log_mlflow_only
        )

    @staticmethod
    def splitter(repo):
        splitted = repo.split("/")
        if len(splitted) != 2:
            raise ValueError(
                f"Invalid input, should be owner_name/repo_name, but got {repo} instead"
            )
        return splitted[1], splitted[0]

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        run_id = self._trial_runs[trial]

        # Log the artifact if set_artifact is set to True.
        if self.should_save_artifact:
            self.mlflow_util.save_artifacts(run_id=run_id, dir=trial.local_path)
            if not self.mlflow_util.mlflow_only:
                self.mlflow_util.upload(run_id=run_id if run_id else "")
        # Stop the run once trial finishes.
        status = "FINISHED" if not failed else "FAILED"
        self.mlflow_util.end_run(run_id=run_id, status=status)
