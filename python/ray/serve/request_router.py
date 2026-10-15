from ray.serve._private.common import ReplicaID
from ray.serve._private.replica_result import ReplicaResult  # noqa: F401
from ray.serve._private.request_router.common import (
    PendingRequest,
)
from ray.serve._private.request_router.replica_wrapper import (
    RunningReplica,
)
from ray.serve._private.request_router.request_router import (
    FIFOMixin,
    LocalityMixin,
    MultiplexMixin,
    RequestRouter,
)

# The public request-router extension surface. Every name here is decorated
# `@PublicAPI` at its definition site and documented in the Serve API reference
# under its `serve.request_router.*` path, so declaring them explicitly records
# an existing contract rather than creating a new one.
#
# `ReplicaResult` is deliberately absent: it carries no API annotation and has no
# reference-page entry, so it isn't part of this public surface. It keeps an
# explicit F401 suppression because the re-export itself is still in use.
__all__ = [
    "FIFOMixin",
    "LocalityMixin",
    "MultiplexMixin",
    "PendingRequest",
    "ReplicaID",
    "RequestRouter",
    "RunningReplica",
]
