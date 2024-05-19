import logging

from ray.train._internal.syncer import SyncConfig as TrainSyncConfig
from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)


@Deprecated
class SyncConfig(TrainSyncConfig):
    def __new__(cls: type, *args, **kwargs):
        raise DeprecationWarning(
            "`ray.tune.SyncConfig` has been moved to `ray.train.SyncConfig`. "
            "Please update your code to use `ray.train.SyncConfig`."
        )
