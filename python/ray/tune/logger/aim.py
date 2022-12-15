import logging
import numpy as np

from typing import TYPE_CHECKING, Dict, Optional

from aim.ext.resource import DEFAULT_SYSTEM_TRACKING_INT
from ray.tune.logger.logger import LoggerCallback
from ray.util.debug import log_once
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
    """Aim Logger.
    Logs metrics in aim format.
    """

    VALID_HPARAMS = (str, bool, int, float, list, type(None))
    VALID_NP_HPARAMS = (np.bool8, np.float32, np.float64, np.int32, np.int64)

    def __init__(
        self,
        repo: Optional[str] = None,
        experiment: Optional[str] = None,
        system_tracking_interval: Optional[int] = DEFAULT_SYSTEM_TRACKING_INT,
        log_system_params: bool = True,
        metrics: Optional[str] = None,
    ):

        self._repo_path = repo
        self._experiment_name = experiment
        self._system_tracking_interval = system_tracking_interval
        self._log_system_params = log_system_params
        self._metrics = metrics
        self._log_value_warned = False

        try:
            from aim.sdk import Run

            self._run_cls = Run
            """
            # Todo: implement the capability of images audio etc.
            from aim import Image
            from aim import Distribution
            from aim.ext.resource.configs import DEFAULT_SYSTEM_TRACKING_INT
            """
        except ImportError:
            if log_once("aim-install"):
                logger.info("Please run `pip install aim` to use the AimLogger.")
            raise

        self._trial_run: Dict["Trial", Run] = {}

    def _create_run(self, trial):
        """
        Returns: Run
        """
        run = self._run_cls(
            repo=self._repo_path,
            experiment=self._experiment_name,
            system_tracking_interval=self._system_tracking_interval,
            log_system_params=self._log_system_params,
        )
        run["trial_id"] = trial.trial_id
        return run

    def log_trial_start(self, trial: "Trial"):
        logger.info(f"trial {trial} logger is started")

        # do nothing when the trial is alrady initiliated
        if trial in self._trial_run:
            self._trial_run[trial].close()
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
        if trial not in self._trial_run:
            self.log_trial_start(trial)

        step = result.get(TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]

        for k in ["config", "pid", "timestamp", TIME_TOTAL_S, TRAINING_ITERATION]:
            if k in tmp_result:
                del tmp_result[k]  # not useful to log these

        context = None
        if "context" in tmp_result:
            context = tmp_result["context"]
            del tmp_result["context"]

        epoch = None
        if "epoch" in tmp_result:
            epoch = tmp_result["epoch"]
            del tmp_result["epoch"]

        if self._metrics:
            for metric in self._metrics:
                try:
                    self._trial_run[trial].track(
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
                    self._trial_run[trial].track(
                        value=value, name=attr, epoch=epoch, step=step, context=context
                    )
                elif (isinstance(value, list) and len(value) > 0) or (
                    isinstance(value, np.ndarray) and value.size > 0
                ):
                    valid_result[attr] = value

                    """
                    # ToDO: implement usage of audio, images etc. here..
                    # Must be video
                    if isinstance(value, np.ndarray) and value.ndim == 5:
                        self._trial_writer[trial].add_video(
                            attr, value, global_step=step, fps=20
                        )
                        continue

                    try:
                        self._trial_writer[trial].add_histogram(
                            attr, value, global_step=step
                        )
                    # In case TensorboardX still doesn't think it's a valid value
                    # (e.g. `[[]]`), warn and move on.
                    except (ValueError, TypeError):
                        if log_once("invalid_tbx_value"):
                            logger.warning(
                                "You are trying to log an invalid value ({}={}) "
                                "via {}!".format(attr, value, type(self).__name__)
                            )
                    """

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        # cleanup in the end
        self._trial_run[trial].finalize()
        self._trial_run[trial].close()
        del self._trial_run[trial]

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
