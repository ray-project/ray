import logging
import os
from typing import List, Optional, Tuple

from ray._private.ray_constants import env_integer
from ray.autoscaler.sdk import configure_logging, rsync
from ray.tune.sync_client import SyncClient
from ray.tune.syncer import NodeSyncer
from ray.util import get_node_ip_address
from ray.util.debug import log_once
from ray.util.annotations import Deprecated


@Deprecated
class DockerSyncer:
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "DockerSyncer has been fully deprecated. There is no need to "
            "use this syncer anymore - data syncing will happen automatically "
            "using the Ray object store. You can just remove passing this class."
        )


@Deprecated
class DockerSyncClient:
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "DockerSyncClient has been fully deprecated. There is no need to "
            "use this syncer anymore - data syncing will happen automatically "
            "using the Ray object store. You can just remove passing this class."
        )
