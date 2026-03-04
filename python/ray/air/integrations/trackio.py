from typing import Dict, Optional, List

import trackio

from ray.train._internal.session import get_session
from ray.tune.logger import LoggerCallback
from ray.tune.utils import flatten_dict
from ray.tune.experiment import Trial
from ray.util import PublicAPI

try:
    import trackio
except ImportError:
    trackio = None

# ------------------------------------------------------------
# setup_trackio()
# ------------------------------------------------------------

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
    Initialize Trackio for Ray Train loops.

    Equivalent to ray.air.integrations.wandb.setup_wandb().

    Automatically derives metadata from Ray Train sessions.
    """

    session = get_session()

    trial_name = None
    experiment_name = None

    if session:

        if rank_zero_only:
            if session.world_rank is not None and session.world_rank != 0:
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


# ------------------------------------------------------------
# TrackioLoggerCallback
# ------------------------------------------------------------

@PublicAPI(stability="alpha")
class TrackioLoggerCallback(LoggerCallback):
    """
    Ray Tune logger callback for Trackio.

    Each Ray Trial corresponds to a Trackio run.
    """

    # Keys not logged
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
        """
        Args:
            project:
                Trackio project name

            group:
                Optional run grouping

            log_config:
                Whether to log Ray config parameters

            auto_log_gpu:
                Enable Trackio GPU monitoring

            gpu_log_interval:
                GPU logging interval (seconds)

            dataset_id:
                Hugging Face dataset for experiment logs

            space_id:
                Hugging Face Space dashboard

            excludes:
                Keys to exclude from logging
        """

        self.project = project
        self.group = group
        self.log_config = log_config

        # Trackio-specific features
        self.auto_log_gpu = auto_log_gpu
        self.gpu_log_interval = gpu_log_interval
        self.dataset_id = dataset_id
        self.space_id = space_id

        self.excludes = excludes or []

        self._trial_runs: Dict[Trial, trackio.Run] = {}

    # --------------------------------------------------------

    def log_trial_start(self, trial: Trial):

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

    # --------------------------------------------------------

    def log_trial_result(
        self,
        iteration: int,
        trial: Trial,
        result: Dict,
    ):

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

            if isinstance(value, (int, float)):
                metrics[key] = value

        if metrics:
            trackio.log(metrics, step=iteration)

    # --------------------------------------------------------

    def log_trial_save(self, trial: Trial):

        checkpoint = trial.checkpoint

        if checkpoint:

            try:
                path = checkpoint.path

                trackio.log(
                    {"checkpoint_path": path}
                )

            except Exception:
                pass

    # --------------------------------------------------------

    def log_trial_end(self, trial: Trial, failed: bool = False):

        run = self._trial_runs.get(trial)

        if run:
            run.finish()

        self._trial_runs.pop(trial, None)

    # --------------------------------------------------------

    def on_experiment_end(self, trials, **info):

        for trial in list(self._trial_runs.keys()):

            run = self._trial_runs.get(trial)

            if run:
                run.finish()

        self._trial_runs.clear()