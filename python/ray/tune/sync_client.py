import abc
import logging

from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)


@Deprecated
class SyncClient(abc.ABC):
    """Client interface for interacting with remote storage options."""

    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "SyncClient has been deprecated. Please implement a "
            "`ray.tune.syncer.Syncer` instead."
        )


@Deprecated
class FunctionBasedClient(SyncClient):
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "FunctionBasedClient has been deprecated. Please implement a "
            "`ray.tune.syncer.Syncer` instead."
        )


@Deprecated
class CommandBasedClient(SyncClient):
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "CommandBasedClient has been deprecated. Please implement a "
            "`ray.tune.syncer.Syncer` instead."
        )


@Deprecated
class RemoteTaskClient(SyncClient):
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "RemoteTaskClient has been deprecated. Please implement a "
            "`ray.tune.syncer.Syncer` instead."
        )
