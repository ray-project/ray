import logging
from typing import Dict, List, Optional

import numpy as np

try:
    import trackio
except ImportError:
    trackio = None

from ray.train._internal.session import get_session
from ray.tune.experiment import Trial
from ray.tune.logger import LoggerCallback
from ray.tune.utils import flatten_dict
from ray.util import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
def setup_trackio(
    config: Optional[Dict] = None,
    project: Optional[str] = None,
    name: Optional[str] = None,
    group: Optional[str] = None,
    auto_log_gpu: bool = False,
    gpu_log_interval: float = 10.0,
    dataset_id: Optional[str] = None,
    space_id: Optional[str] = None,
    rank_zero_only: bool = True,
):
    """
    Set up a Trackio experiment run.

    This function initializes a Trackio run for experiment tracking within
    Ray Train training loops. Trackio is a lightweight experiment tracking
    system that logs metrics, configuration parameters, and system statistics
    during model training.

    By default, the run name is derived from the Ray trial name and the run
    group corresponds to the Ray experiment name. These values can be overridden
    by explicitly passing ``name`` and ``group``.

    In distributed training with Ray Train, only the worker with rank 0 will
    initialize the Trackio run by default. This prevents duplicate logging from
    multiple workers. If ``rank_zero_only=False`` is specified, every worker
    will initialize its own Trackio run.

    Trackio supports additional features such as GPU utilization logging,
    remote experiment logging via Hugging Face datasets, and remote dashboards
    hosted on Hugging Face Spaces.

    Args:
        config: Configuration dictionary to log as part of the experiment
            metadata. This typically contains hyperparameters or training
            configuration values.
        project: Name of the Trackio project under which runs are grouped.
            Defaults to ``"ray-train"`` if not provided.
        name: Optional name of the Trackio run. Defaults to the Ray trial name.
        group: Optional grouping identifier for runs. Defaults to the Ray
            experiment name.
        auto_log_gpu: If True, Trackio automatically records GPU utilization
            metrics during training.
        gpu_log_interval: Interval (in seconds) between GPU metric samples.
        dataset_id: Optional Hugging Face dataset ID where experiment logs
            should be uploaded.
        space_id: Optional Hugging Face Space ID used for hosting the Trackio
            dashboard remotely.
        rank_zero_only: If True, only the rank 0 worker in distributed training
            will initialize Trackio. If False, all workers will create runs.

    Returns:
        A Trackio run object returned by ``trackio.init()``.

    Example:

        .. code-block:: python

            from ray.air.integrations.trackio import setup_trackio

            def training_loop(config):
                run = setup_trackio(config=config, project="my-project")

                for step in range(10):
                    loss = train_step()
                    run.log({"loss": loss}, step=step)

                run and run.finish()
    """

    if trackio is None:
        raise RuntimeError("Trackio was not found. Install with `pip install trackio`.")

    session = get_session()

    trial_name = None
    experiment_name = None

    if session:

        if (
            rank_zero_only
            and session.world_rank is not None
            and session.world_rank != 0
        ):
            return None

        trial_name = session.trial_name
        experiment_name = session.experiment_name

    name = name or trial_name
    group = group or experiment_name

    run = trackio.init(
        project=project or "ray-train",
        name=name,
        group=group,
        config=config,
        auto_log_gpu=auto_log_gpu,
        gpu_log_interval=gpu_log_interval,
        dataset_id=dataset_id,
        space_id=space_id,
    )

    return run


@PublicAPI(stability="alpha")
class TrackioLoggerCallback(LoggerCallback):
    """
    Logger callback that logs Ray Tune experiment results to Trackio.

    This callback integrates Trackio experiment tracking with Ray Tune.
    Each Ray Tune trial corresponds to a Trackio run.Each Ray Tune trial
    corresponds to a separate Trackio run. Metrics reported by the training
    function are logged to the corresponding run.

    Trackio supports additional capabilities such as GPU telemetry logging,
    remote experiment logging through Hugging Face datasets, and remote
    dashboards hosted on Hugging Face Spaces.

    Example:

        .. code-block:: python

            from ray import tune
            from ray.air.integrations.trackio import TrackioLoggerCallback

            def train_fn(config):
                for step in range(10):
                    loss = 1 / (step + 1)
                    tune.report({"loss": loss})

            tuner = tune.Tuner(
                train_fn,
                param_space={"lr": tune.grid_search([0.001, 0.01])},
                run_config=tune.RunConfig(
                    callbacks=[
                        TrackioLoggerCallback(
                            project="ray-experiments",
                            auto_log_gpu=True
                        )
                    ]
                ),
            )

            tuner.fit()

    Args:
        project: Name of the Trackio project.
        group: Optional grouping identifier for runs.
        log_config: If True, Ray trial configuration parameters will be logged.
        auto_log_gpu: If True, GPU utilization metrics will be logged.
        gpu_log_interval: Interval (seconds) between GPU metric samples.
        dataset_id: Optional Hugging Face dataset ID used for remote logging.
        space_id: Optional Hugging Face Space used to host the Trackio dashboard.
        excludes: List of metric keys that should not be logged.
    """

    _exclude_results = ["done", "should_checkpoint"]

    def __init__(
        self,
        project: str,
        group: Optional[str] = None,
        log_config: bool = True,
        auto_log_gpu: bool = False,
        gpu_log_interval: float = 10.0,
        dataset_id: Optional[str] = None,
        space_id: Optional[str] = None,
        excludes: Optional[List[str]] = None,
    ):

        if trackio is None:
            raise RuntimeError(
                "Trackio was not found. Install with `pip install trackio`."
            )

        self.project = project
        self.group = group
        self.log_config = log_config

        self.auto_log_gpu = auto_log_gpu
        self.gpu_log_interval = gpu_log_interval
        self.dataset_id = dataset_id
        self.space_id = space_id

        self.excludes = excludes or []

        self._trial_runs: Dict[Trial, object] = {}

    def log_trial_start(self, trial: Trial):
        """Initialize a Trackio run when a Ray Tune trial starts."""

        # Prevent duplicate runs during trial recovery
        if trial in self._trial_runs:
            return

        config = trial.config.copy()

        if not self.log_config:
            config = {}

        run = trackio.init(
            project=self.project,
            name=str(trial),
            group=self.group or trial.experiment_dir_name,
            config=config,
            auto_log_gpu=self.auto_log_gpu,
            gpu_log_interval=self.gpu_log_interval,
            dataset_id=self.dataset_id,
            space_id=self.space_id,
        )

        self._trial_runs[trial] = run

    def log_trial_result(
        self,
        iteration: int,
        trial: Trial,
        result: Dict,
    ):
        """Log metrics from a Ray Tune training iteration to Trackio."""

        run = self._trial_runs.get(trial)

        # Lazy initialization after experiment restore
        if run is None:
            self.log_trial_start(trial)
            run = self._trial_runs.get(trial)

        if not run:
            return

        flat = flatten_dict(result)

        metrics = {}

        for key, value in flat.items():

            if key in self._exclude_results:
                continue

            if key in self.excludes:
                continue

            # Convert numpy arrays and scalar types
            if isinstance(value, np.ndarray):
                value = value.tolist()

            if isinstance(value, np.generic):
                value = value.item()

            # Only log supported metric types
            if isinstance(value, (int, float)):
                metrics[key] = value

        if metrics:
            run.log(metrics, step=iteration)

    def log_trial_save(self, trial: Trial):
        """Log checkpoint metadata when a Ray Tune trial checkpoint is saved."""

        checkpoint = trial.checkpoint

        if not checkpoint:
            return

        if checkpoint:
            try:
                path = checkpoint.path
                run = self._trial_runs.get(trial)
                if run:
                    run.log({"checkpoint_path": path})
            except Exception as e:
                logger.warning(f"trackio: Failed to log checkpoint path: {e}")

    def log_trial_end(self, trial: Trial, failed: bool = False):
        """Finalize the Trackio run when a Ray Tune trial finishes."""

        run = self._trial_runs.get(trial)

        if run:
            run.finish()

        self._trial_runs.pop(trial, None)

    def on_experiment_end(self, trials, **info):
        """Finalize all Trackio runs after the Ray Tune experiment completes."""

        for trial in list(self._trial_runs.keys()):

            run = self._trial_runs.get(trial)

            if run:
                run.finish()

        self._trial_runs.clear()

        try:
            trackio.finish()
        except Exception as e:
            logger.warning(f"trackio: `trackio.finish()` failed: {e}")
