from typing import Any, Dict, List, Optional

import torch
from composer.core.state import State
from composer.loggers import Logger
from composer.loggers.logger_destination import LoggerDestination

import ray.train


class RayLogger(LoggerDestination):
    """A logger to relay information logged by composer models to ray.

    This logger allows utilizing all necessary logging and logged data handling provided
    by the Composer library. All the logged information is saved in the data dictionary
    every time a new information is logged, but to reduce unnecessary reporting, the
    most up-to-date logged information is reported as metrics every batch checkpoint and
    epoch checkpoint (see Composer's Event module for more details).

    Because ray's metric dataframe will not include new keys that is reported after the
    very first report call, any logged information with the keys not included in the
    first batch checkpoint would not be retrievable after training. In other words, if
    the log level is greater than `LogLevel.BATCH` for some data, they would not be
    present in `Result.metrics_dataframe`. To allow preserving those information, the
    user can provide keys to be always included in the reported data by using `keys`
    argument in the constructor. For `MosaicTrainer`, use
    `trainer_init_config['log_keys']` to populate these keys.

    Note that in the Event callback functions, we remove unused variables, as this is
    practiced in Mosaic's composer library.

    Args:
        keys: the key values that will be included in the reported metrics.
    """

    def __init__(self, keys: Optional[List[str]] = None) -> None:
        self.data = {}
        # report at fit end only if there are additional training batches run after the
        # last epoch checkpoint report
        self.should_report_fit_end = False
        if keys:
            for key in keys:
                self.data[key] = None

    def log_metrics(self, metrics: Dict[str, Any], step: Optional[int] = None) -> None:
        self.data.update(metrics.items())
        for key, val in self.data.items():
            if isinstance(val, torch.Tensor):
                self.data[key] = val.item()

    def batch_checkpoint(self, state: State, logger: Logger) -> None:
        del logger  # unused
        self.should_report_fit_end = True

    def epoch_checkpoint(self, state: State, logger: Logger) -> None:
        del logger  # unused
        self.should_report_fit_end = False
        ray.train.report(self.data)

        # flush the data
        self.data = {}

    def fit_end(self, state: State, logger: Logger) -> None:
        # report at close in case the trainer stops in the middle of an epoch.
        # this may be double counted with epoch checkpoint.
        del logger  # unused
        if self.should_report_fit_end:
            ray.train.report(self.data)
