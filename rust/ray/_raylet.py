"""Compatibility shim for ``ray._raylet``.

The Rust backend's compiled extension is imported as top-level ``_raylet`` by
``rust/ray/__init__.py``. This shim re-exports the key extension entry points
under ``ray._raylet`` so Python-side code that expects the Cython layout can
continue importing from the package namespace.
"""

from _raylet import (
    PyActorID as ActorID,
    PyCoreWorker as CoreWorker,
    PyGcsClient as GcsClient,
    PyJobID as JobID,
    PyNodeID as NodeID,
    PyObjectID as ObjectID,
    PyPlacementGroupID as PlacementGroupID,
    PyTaskID as TaskID,
    PyWorkerID as WorkerID,
    start_cluster,
)
from ray import ObjectRef

__all__ = [
    "ActorID",
    "CoreWorker",
    "GcsClient",
    "JobID",
    "NodeID",
    "ObjectID",
    "ObjectRef",
    "PlacementGroupID",
    "TaskID",
    "WorkerID",
    "start_cluster",
]
