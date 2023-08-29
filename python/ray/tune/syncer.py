import logging
import warnings

from ray.train._internal.syncer import SyncConfig as TrainSyncConfig
from ray.util.annotations import Deprecated
from ray.util.debug import log_once


logger = logging.getLogger(__name__)


@Deprecated
class SyncConfig(TrainSyncConfig):
    def __new__(cls: type, *args, **kwargs):
        if log_once("sync_config_moved"):
            warnings.warn(
                "`tune.SyncConfig` has been moved to `train.SyncConfig`. "
                "Please update your code to use `train.SyncConfig`, "
                "as this will raise an error in a future Ray version.",
                stacklevel=2,
            )
        return super(SyncConfig, cls).__new__(cls, *args, **kwargs)


@Deprecated
class Syncer:
    def __new__(cls: type, *args, **kwargs):
        # TODO(justinvyu): Link to a custom fs user guide
        raise DeprecationWarning(
            "`tune.syncer.Syncer` has been deprecated. "
            "Please implement custom syncing logic via a custom "
            "`train.RunConfig(storage_filesystem)` instead."
        )
