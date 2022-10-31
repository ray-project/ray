from typing import Any, Dict, Optional, List
import torch
import os

from composer.loggers import Logger
from composer.loggers.logger_destination import LoggerDestination
from composer.core.state import State
from composer.callbacks.checkpoint_saver import CheckpointSaver
from composer.loggers.logger import LogLevel
from composer.utils import checkpoint, is_model_deepspeed
from composer.utils.checkpoint import PartialFilePath


from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.train.mosaic.mosaic_checkpoint import MosaicCheckpoint


class RayLogger(LoggerDestination):
    """A logger to relay information logged by composer models to ray.

    This logger allows utilizing all necessary logging and logged data handling provided
    by the Composer library. All the logged information is saved in the data dictionary
    every time a new information is logged, but to reduce unnecessary reporting, the
    most up-to-date logged information is reported as metrics every epoch checkpoint
    (see Composer's Event module for more details). Along with the metrics, model
    weights are reported with Ray``Checkpoint``. The model weights saved in each
    checkpoint has already removed the ``module`` wrapper added by Composer models and
    Ray distributed training. As such the checkpointed weights can be directly loaded
    via PyTorch ``load_state_dict`` call.

    Because ray's metric dataframe will not include new keys that is reported after the
    very first report call, any logged information with the keys not included in the
    first epoch checkpoint would not be retrievable after training. In other words, if
    the log level is greater than `LogLevel.EPOCH` for some data, they would not be
    present in `Result.metrics_dataframe`. To allow preserving such information, the
    user can provide keys to be always included in the reported data by using `keys`
    argument in the constructor. For `MosaicTrainer`, use
    `trainer_init_config['log_keys']` to populate these keys.

    Note that in the Event callback functions, we remove unused variables, as this is
    practiced in Mosaic's composer library.

    Args:
        keys: the key values that will be included in the reported metrics.
    """

    def __init__(
        self, keys: Optional[List[str]] = None
    ) -> None:
        self.data = {}
        self.filename = PartialFilePath('ep{epoch}-ba{batch}-rank{rank}.pt', "ray_tmp")
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
        self._report(state)

    def fit_end(self, state: State, logger: Logger) -> None:
        # report at close in case the trainer stops in the middle of an epoch.
        # this may be double counted with epoch checkpoint.
        if self.should_report_fit_end:
            self._report(state)

    def _report(self, state: State):
        # get last checkpoint from the checkpoint saver.
        filename = self.filename.format(state, is_model_deepspeed(state.model))
        saved_path = checkpoint.save_checkpoint(
            state=state,
            filename=filename,
            weights_only=False,
        )
        mosaic_checkpoint = MosaicCheckpoint.from_dict({"state" : None})
        if saved_path:
            saved_path = os.path.join(
                os.getcwd(), saved_path
            )
            mosaic_checkpoint=MosaicCheckpoint.from_directory(saved_path)


        session.report(
            self.data, checkpoint=mosaic_checkpoint
        )

        # flush the data
        self.data = {}
