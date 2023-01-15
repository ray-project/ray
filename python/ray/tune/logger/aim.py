import logging
import numpy as np

from typing import TYPE_CHECKING, Dict, Optional, List
from aim.ext.resource import DEFAULT_SYSTEM_TRACKING_INT
from aim.sdk import Run

from ray.tune.logger.logger import LoggerCallback
from ray.tune.result import (
    TRAINING_ITERATION,
    TIME_TOTAL_S,
    TIMESTEPS_TOTAL,
)
from ray.tune.utils import flatten_dict
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.tune.experiment.trial import Trial  # noqa: F401

logger = logging.getLogger(__name__)

VALID_SUMMARY_TYPES = [int, float, np.float32, np.float64, np.int32, np.int64]


@PublicAPI
class AimCallback(LoggerCallback):
    """Aim Logger, logs metrics in Aim format.

    Aim is an open-source, self-hosted ML experiment tracking tool.
    It's good at tracking lots (1000s) of training runs, and it allows you to compare them with a
    performant and beautiful UI.

    Source: https://github.com/aimhubio/aim

    Arguments:
        repo (:obj:`str`, optional): Aim repository path or Repo object to which Run object is bound.
            If skipped, default Repo is used.
        experiment (:obj:`str`, optional): Sets Run's `experiment` property. 'default' if not specified.
            Can be used later to query runs/sequences.
        system_tracking_interval (:obj:`int`, optional): Sets the tracking interval in seconds for system usage
            metrics (CPU, Memory, etc.). Set to `None` to disable system metrics tracking.
        log_system_params (:obj:`bool`, optional): Enable/Disable logging of system params such as installed packages,
            git info, environment variables, etc.
        metrics (:obj:`List[str]`, optional): Specific metrics to track,
            if no metric is specified log everything that is reported.
        as_multirun (:obj:`bool`, optional): Enable/Disable creating new runs for each trial.
    """

    VALID_HPARAMS = (str, bool, int, float, list, type(None))
    VALID_NP_HPARAMS = (np.bool8, np.float32, np.float64, np.int32, np.int64)

    def __init__(
        self,
        repo: Optional[str] = None,
        experiment: Optional[str] = None,
        system_tracking_interval: Optional[int] = DEFAULT_SYSTEM_TRACKING_INT,
        log_system_params: bool = True,
        metrics: Optional[List[str]] = None,
        as_multirun: bool = False,
    ):

        self._repo_path = repo
        self._experiment_name = experiment
        self._system_tracking_interval = system_tracking_interval
        self._log_system_params = log_system_params
        self._metrics = metrics
        self._as_multirun = as_multirun
        self._run_cls = Run
        self._trial_run: Dict["Trial", Run] = {}

    def _create_run(self, trial: "Trial"):
        """
        Returns: Run
        """
        run = self._run_cls(
            repo=self._repo_path,
            experiment=self._experiment_name,
            system_tracking_interval=self._system_tracking_interval,
            log_system_params=self._log_system_params,
        )
        if self._as_multirun:
            run["trial_id"] = trial.trial_id
        return run

    def log_trial_start(self, trial: "Trial"):
        logger.info(f"trial {trial} logger is started")

        if self._as_multirun:
            if trial in self._trial_run:
                self._trial_run[trial].close()
        elif self._trial_run:
            return

        trial.init_logdir()
        self._trial_run[trial] = self._create_run(trial)

        # log hyperparameters
        if trial and trial.evaluated_params:
            flat_result = flatten_dict(trial.evaluated_params, delimiter="/")
            scrubbed_result = {
                k: value
                for k, value in flat_result.items()
                if isinstance(value, tuple(VALID_SUMMARY_TYPES))
            }
            self._try_log_hparams(trial, scrubbed_result)

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        # create local copy to avoid problems
        tmp_result = result.copy()

        step = result.get(TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]

        for k in ["config", "pid", "timestamp", TIME_TOTAL_S, TRAINING_ITERATION]:
            if k in tmp_result:
                del tmp_result[k]  # not useful to log these

        context = {}
        if "context" in tmp_result:
            context.update(tmp_result["context"])
            del tmp_result["context"]

        epoch = None
        if "epoch" in tmp_result:
            epoch = tmp_result["epoch"]
            del tmp_result["epoch"]

        trial_run = self._get_trial_run(trial)
        if not self._as_multirun:
            context["trial"] = trial.trial_id

        if self._metrics:
            for metric in self._metrics:
                try:
                    trial_run.track(
                        value=tmp_result[metric],
                        epoch=epoch,
                        name=metric,
                        step=step,
                        context=context,
                    )
                except KeyError:
                    logger.warning(
                        f"The metric {metric} is specified but not reported."
                    )
        else:
            # if no metric is specified log everything that is reported
            flat_result = flatten_dict(tmp_result, delimiter="/")
            valid_result = {}

            for attr, value in flat_result.items():
                if isinstance(value, tuple(VALID_SUMMARY_TYPES)) and not np.isnan(
                    value
                ):
                    valid_result[attr] = value
                    trial_run.track(
                        value=value, name=attr, epoch=epoch, step=step, context=context
                    )
                elif (isinstance(value, list) and len(value) > 0) or (
                    isinstance(value, np.ndarray) and value.size > 0
                ):
                    valid_result[attr] = value

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        # cleanup in the end
        trial_run = self._get_trial_run(trial)
        trial_run.close()
        del trial_run

        logger.info(f"trial {trial} aim logger closed")

    def _try_log_hparams(self, trial: "Trial", params):
        flat_params = flatten_dict(params)
        scrubbed_params = {
            k: v for k, v in flat_params.items() if isinstance(v, self.VALID_HPARAMS)
        }

        np_params = {
            k: v.tolist()
            for k, v in flat_params.items()
            if isinstance(v, self.VALID_NP_HPARAMS)
        }

        scrubbed_params.update(np_params)
        removed = {
            k: v
            for k, v in flat_params.items()
            if not isinstance(v, self.VALID_HPARAMS + self.VALID_NP_HPARAMS)
        }
        if removed:
            logger.info(
                "Removed the following hyperparameter values when "
                "logging to aim: %s",
                str(removed),
            )

        self._trial_run[trial]["hparams"] = scrubbed_params

    def _get_trial_run(self, trial):
        if not self._as_multirun:
            return list(self._trial_run.values())[0]
        return self._trial_run[trial]
