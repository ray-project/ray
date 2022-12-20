import logging
import numpy as np

from typing import TYPE_CHECKING, Dict

from ray.tune.logger.logger import Logger, LoggerCallback
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
class TBXLogger(Logger):
    """TensorBoardX Logger.

    Note that hparams will be written only after a trial has terminated.
    This logger automatically flattens nested dicts to show on TensorBoard:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}
    """

    VALID_HPARAMS = (str, bool, int, float, list, type(None))
    VALID_NP_HPARAMS = (np.bool8, np.float32, np.float64, np.int32, np.int64)

    def _init(self):
        try:
            from tensorboardX import SummaryWriter
        except ImportError:
            if log_once("tbx-install"):
                logger.info('pip install "ray[tune]" to see TensorBoard files.')
            raise
        self._file_writer = SummaryWriter(self.logdir, flush_secs=30)
        self.last_result = None

    def on_result(self, result: Dict):
        step = result.get(TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]

        tmp = result.copy()
        for k in ["config", "pid", "timestamp", TIME_TOTAL_S, TRAINING_ITERATION]:
            if k in tmp:
                del tmp[k]  # not useful to log these

        flat_result = flatten_dict(tmp, delimiter="/")
        path = ["ray", "tune"]
        valid_result = {}

        for attr, value in flat_result.items():
            full_attr = "/".join(path + [attr])
            if isinstance(value, tuple(VALID_SUMMARY_TYPES)) and not np.isnan(value):
                valid_result[full_attr] = value
                self._file_writer.add_scalar(full_attr, value, global_step=step)
            elif (isinstance(value, list) and len(value) > 0) or (
                isinstance(value, np.ndarray) and value.size > 0
            ):
                valid_result[full_attr] = value

                # Must be video
                if isinstance(value, np.ndarray) and value.ndim == 5:
                    self._file_writer.add_video(
                        full_attr, value, global_step=step, fps=20
                    )
                    continue

                try:
                    self._file_writer.add_histogram(full_attr, value, global_step=step)
                # In case TensorboardX still doesn't think it's a valid value
                # (e.g. `[[]]`), warn and move on.
                except (ValueError, TypeError):
                    if log_once("invalid_tbx_value"):
                        logger.warning(
                            "You are trying to log an invalid value ({}={}) "
                            "via {}!".format(full_attr, value, type(self).__name__)
                        )

        self.last_result = valid_result
        self._file_writer.flush()

    def flush(self):
        if self._file_writer is not None:
            self._file_writer.flush()

    def close(self):
        if self._file_writer is not None:
            if self.trial and self.trial.evaluated_params and self.last_result:
                flat_result = flatten_dict(self.last_result, delimiter="/")
                scrubbed_result = {
                    k: value
                    for k, value in flat_result.items()
                    if isinstance(value, tuple(VALID_SUMMARY_TYPES))
                }
                self._try_log_hparams(scrubbed_result)
            self._file_writer.close()

    def _try_log_hparams(self, result):
        # TBX currently errors if the hparams value is None.
        flat_params = flatten_dict(self.trial.evaluated_params)
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
                "logging to tensorboard: %s",
                str(removed),
            )

        from tensorboardX.summary import hparams

        try:
            experiment_tag, session_start_tag, session_end_tag = hparams(
                hparam_dict=scrubbed_params, metric_dict=result
            )
            self._file_writer.file_writer.add_summary(experiment_tag)
            self._file_writer.file_writer.add_summary(session_start_tag)
            self._file_writer.file_writer.add_summary(session_end_tag)
        except Exception:
            logger.exception(
                "TensorboardX failed to log hparams. "
                "This may be due to an unsupported type "
                "in the hyperparameter values."
            )


@PublicAPI
class TBXLoggerCallback(LoggerCallback):
    """TensorBoardX Logger.

    Note that hparams will be written only after a trial has terminated.
    This logger automatically flattens nested dicts to show on TensorBoard:

        {"a": {"b": 1, "c": 2}} -> {"a/b": 1, "a/c": 2}
    """

    VALID_HPARAMS = (str, bool, int, float, list, type(None))
    VALID_NP_HPARAMS = (np.bool8, np.float32, np.float64, np.int32, np.int64)

    def __init__(self):
        try:
            from tensorboardX import SummaryWriter

            self._summary_writer_cls = SummaryWriter
        except ImportError:
            if log_once("tbx-install"):
                logger.info('pip install "ray[tune]" to see TensorBoard files.')
            raise
        self._trial_writer: Dict["Trial", SummaryWriter] = {}
        self._trial_result: Dict["Trial", Dict] = {}

    def log_trial_start(self, trial: "Trial"):
        if trial in self._trial_writer:
            self._trial_writer[trial].close()
        trial.init_logdir()
        self._trial_writer[trial] = self._summary_writer_cls(
            trial.logdir, flush_secs=30
        )
        self._trial_result[trial] = {}

    def log_trial_result(self, iteration: int, trial: "Trial", result: Dict):
        if trial not in self._trial_writer:
            self.log_trial_start(trial)

        step = result.get(TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]

        tmp = result.copy()
        for k in ["config", "pid", "timestamp", TIME_TOTAL_S, TRAINING_ITERATION]:
            if k in tmp:
                del tmp[k]  # not useful to log these

        flat_result = flatten_dict(tmp, delimiter="/")
        path = ["ray", "tune"]
        valid_result = {}

        for attr, value in flat_result.items():
            full_attr = "/".join(path + [attr])
            if isinstance(value, tuple(VALID_SUMMARY_TYPES)) and not np.isnan(value):
                valid_result[full_attr] = value
                self._trial_writer[trial].add_scalar(full_attr, value, global_step=step)
            elif (isinstance(value, list) and len(value) > 0) or (
                isinstance(value, np.ndarray) and value.size > 0
            ):
                valid_result[full_attr] = value

                # Must be video
                if isinstance(value, np.ndarray) and value.ndim == 5:
                    self._trial_writer[trial].add_video(
                        full_attr, value, global_step=step, fps=20
                    )
                    continue

                try:
                    self._trial_writer[trial].add_histogram(
                        full_attr, value, global_step=step
                    )
                # In case TensorboardX still doesn't think it's a valid value
                # (e.g. `[[]]`), warn and move on.
                except (ValueError, TypeError):
                    if log_once("invalid_tbx_value"):
                        logger.warning(
                            "You are trying to log an invalid value ({}={}) "
                            "via {}!".format(full_attr, value, type(self).__name__)
                        )

        self._trial_result[trial] = valid_result
        self._trial_writer[trial].flush()

    def log_trial_end(self, trial: "Trial", failed: bool = False):
        if trial in self._trial_writer:
            if trial and trial.evaluated_params and self._trial_result[trial]:
                flat_result = flatten_dict(self._trial_result[trial], delimiter="/")
                scrubbed_result = {
                    k: value
                    for k, value in flat_result.items()
                    if isinstance(value, tuple(VALID_SUMMARY_TYPES))
                }
                self._try_log_hparams(trial, scrubbed_result)
            self._trial_writer[trial].close()
            del self._trial_writer[trial]
            del self._trial_result[trial]

    def _try_log_hparams(self, trial: "Trial", result: Dict):
        # TBX currently errors if the hparams value is None.
        flat_params = flatten_dict(trial.evaluated_params)
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
                "logging to tensorboard: %s",
                str(removed),
            )

        from tensorboardX.summary import hparams

        try:
            experiment_tag, session_start_tag, session_end_tag = hparams(
                hparam_dict=scrubbed_params, metric_dict=result
            )
            self._trial_writer[trial].file_writer.add_summary(experiment_tag)
            self._trial_writer[trial].file_writer.add_summary(session_start_tag)
            self._trial_writer[trial].file_writer.add_summary(session_end_tag)
        except Exception:
            logger.exception(
                "TensorboardX failed to log hparams. "
                "This may be due to an unsupported type "
                "in the hyperparameter values."
            )
