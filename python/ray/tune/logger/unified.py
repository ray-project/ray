import logging
from typing import Optional, List, Type, Dict, TYPE_CHECKING

from ray.tune.logger import DEFAULT_LOGGERS
from ray.tune.logger.json import JsonLogger
from ray.tune.logger.logger import Logger
from ray.util import log_once
from ray.util.annotations import Deprecated, PublicAPI

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from ray.tune.experiment.trial import Trial  # noqa: F401


@Deprecated(message="`UnifiedLogger` will be removed in Ray 2.7.", warning=True)
@PublicAPI
class UnifiedLogger(Logger):
    """Unified result logger for TensorBoard, rllab/viskit, plain json.

    Arguments:
        config: Configuration passed to all logger creators.
        logdir: Directory for all logger creators to log to.
        loggers: List of logger creators. Defaults to CSV, Tensorboard,
            and JSON loggers.
    """

    def __init__(
        self,
        config: Dict,
        logdir: str,
        trial: Optional["Trial"] = None,
        loggers: Optional[List[Type[Logger]]] = None,
    ):
        if loggers is None:
            self._logger_cls_list = DEFAULT_LOGGERS
        else:
            self._logger_cls_list = loggers
        if JsonLogger not in self._logger_cls_list:
            if log_once("JsonLogger"):
                logger.warning(
                    "JsonLogger not provided. The ExperimentAnalysis tool is "
                    "disabled."
                )

        super(UnifiedLogger, self).__init__(config, logdir, trial)

    def _init(self):
        self._loggers = []
        for cls in self._logger_cls_list:
            try:
                self._loggers.append(cls(self.config, self.logdir, self.trial))
            except Exception as exc:
                if log_once(f"instantiate:{cls.__name__}"):
                    logger.warning(
                        "Could not instantiate %s: %s.", cls.__name__, str(exc)
                    )

    def on_result(self, result):
        for _logger in self._loggers:
            _logger.on_result(result)

    def update_config(self, config):
        for _logger in self._loggers:
            _logger.update_config(config)

    def close(self):
        for _logger in self._loggers:
            _logger.close()

    def flush(self):
        for _logger in self._loggers:
            _logger.flush()
