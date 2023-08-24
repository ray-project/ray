
from dataclasses import dataclass
import logging

from ray.util.annotations import DeveloperAPI


logger = logging.getLogger(__name__)


@dataclass
class SyncConfig:
    def __new__(cls: type, *args, **kwargs):
        raise DeprecationWarning()


@DeveloperAPI
class Syncer:
    def __new__(cls: type, *args, **kwargs):
        raise DeprecationWarning()

@DeveloperAPI
class SyncerCallback:
    def __new__(cls: type, *args, **kwargs):
        raise DeprecationWarning()