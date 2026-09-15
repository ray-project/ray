"""Cluster-wide owner of the Ray Data usage-stats buffer.

Executions run in whichever process hosts the ``StreamingExecutor`` (the
driver, a ``SplitCoordinator`` actor under Ray Train, a user actor, ...).
``record_extra_usage_tag`` writes the whole payload to a single GCS key, so if
every process kept its own buffer the last writer would clobber the others.
Instead all processes forward their per-execution entries to one detached
actor, which owns the buffer and is the only writer of the ``DATA_USAGE`` tag.
"""

import json
import threading
from collections import OrderedDict
from dataclasses import asdict
from typing import TYPE_CHECKING

import ray
from ray._common.usage import usage_lib

if TYPE_CHECKING:
    from ray.data._internal.usage.collector import UsageInfo

# Bounded buffer of recent executions.
_MAX_EXECUTIONS_TO_TRACK = 100

# Ray Core doesn't allow creating the same named actor from multiple threads
# simultaneously, and executions start on per-dataset executor threads.
_get_or_create_lock = threading.Lock()


class _UsageCollectionActor:
    """Single writer of the ``DATA_USAGE`` usage tag.

    One per cluster (named, detached; see ``get_or_create_usage_collection_actor``).
    Every process that runs a Ray Data execution forwards its ``UsageInfo``
    here; the actor merges entries into one buffer and flushes the merged
    payload to GCS. Tests instantiate this class directly to run the same
    logic in-process.
    """

    def __init__(self):
        # OrderedDict so eviction picks the oldest-inserted entry.
        self._executions: "OrderedDict[str, UsageInfo]" = OrderedDict()

    def record(self, info: "UsageInfo") -> None:
        """Insert or overwrite ``info`` (evicting the oldest entry when full)
        and flush the whole buffer to GCS."""
        if (
            info.id not in self._executions
            and len(self._executions) >= _MAX_EXECUTIONS_TO_TRACK
        ):
            self._executions.popitem(last=False)
        self._executions[info.id] = info
        payload = json.dumps(
            {"executions": [asdict(e) for e in self._executions.values()]}
        )
        # Reference ``TagKey`` through the module: Ray pickles actor classes by
        # value, and the protobuf enum wrapper's descriptor isn't picklable.
        usage_lib.record_extra_usage_tag(usage_lib.TagKey.DATA_USAGE, payload)


def get_or_create_usage_collection_actor() -> ray.actor.ActorHandle:
    """Return the cluster's usage collection actor, creating it if needed.

    Pinned to the calling process's node so it fate-shares with the driver
    (the same placement the stats actor and actor-location tracker use).
    """
    label_selector = {
        ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
    }
    with _get_or_create_lock:
        return (
            ray.remote(num_cpus=0)(_UsageCollectionActor)
            .options(
                name="DataUsageCollectionActor",
                namespace="DataUsageCollectionActor",
                get_if_exists=True,
                lifetime="detached",
                label_selector=label_selector,
            )
            .remote()
        )
