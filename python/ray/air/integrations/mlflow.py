import logging
from typing import Dict, Optional, Union


import ray
from ray.air import session

from ray.air._internal.mlflow import _MLflowLoggerUtil
from ray.tune.logger import LoggerCallback
from ray.tune.result import TIMESTEPS_TOTAL, TRAINING_ITERATION
from ray.tune.experiment import Trial
from ray.util.annotations import PublicAPI

try:
    from mlflow.tracking import MlflowClient
except Exception:
    MlflowClient = object


logger = logging.getLogger(__name__)


class _NoopClient(MlflowClient):
    pass


@PublicAPI(stability="alpha")
def setup_mlflow(
    config: Optional[Dict] = None, rank_zero_only: bool = True, **kwargs
) -> MlflowClient:
    """Set up a MLflow session.

    This function can be used to initialize an Mlflow session in a
    (distributed) training or tuning run.

    By default, the run ID is the trial ID, the run name is the trial name, and
    the run group is the experiment name. These settings can be overwritten by
    passing the respective arguments as ``kwargs``, which will be passed to
    ``wandb.init()``.

    In distributed training with Ray Train, only the zero-rank worker will initialize
    wandb. All other workers will return a disabled run object, so that logging is not
    duplicated in a distributed run. This can be disabled by passing
    ``rank_zero_only=False``, which will then initialize wandb in every training
    worker.

    The ``config`` argument will be passed to Weights and Biases and will be logged
    as the run configuration. If wandb-specific settings are found, they will
    be used to initialize the session. These settings can be

    - api_key_file: Path to locally available file containing a W&B API key
    - api_key: API key to authenticate with W&B

    If no API information is found in the config, wandb will try to authenticate
    using locally stored credentials, created for instance by running ``wandb login``.

    All other keys found in the ``wandb`` config parameter will be passed to
    ``wandb.init()``. If the same keys are present in multiple locations, the
    ``kwargs`` passed to ``setup_wandb()`` will take precedence over those passed
    as config keys.

    Args:
        config: Configuration dict to be logged to weights and biases. Can contain
            arguments for ``wandb.init()`` as well as authentication information.
        rank_zero_only: If True, will return an initialized session only for the
            rank 0 worker in distributed training. If False, will initialize a
            session for all workers.
        kwargs: Passed to ``wandb.init()``.

    Example:

        .. code-block: python

            from ray.air.integrations.wandb import wandb_setup

            def training_loop(config):
                wandb = wandb_setup(config)
                # ...
                wandb.log({"loss": 0.123})

    """
    if MlflowClient == object:
        raise RuntimeError(
            "mlflow was not found - please install with `pip install mlflow`"
        )

    try:
        # Do a try-catch here if we are not in a train session
        _session = session._get_session(warn=False)
        if _session and rank_zero_only and session.get_world_rank() != 0:
            return _NoopClient()
    except RuntimeError:
        pass

    default_kwargs = {
        "trial_id": kwargs.get("trial_id") or session.get_trial_id(),
        "trial_name": kwargs.get("trial_name") or session.get_trial_name(),
        "group": kwargs.get("group") or session.get_experiment_name(),
    }
    default_kwargs.update(kwargs)

    mlflow_util = _MLflowLoggerUtil()
    mlflow_util.setup_mlflow(
        experiment_id=None,  # Todo: continue
        experiment_name=None,
    )
    return mlflow_util._get_client()

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
