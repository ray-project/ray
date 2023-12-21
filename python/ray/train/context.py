import threading
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.train._internal import session
from ray.train._internal.storage import StorageContext
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.tune.execution.placement_groups import PlacementGroupFactory


# The context singleton on this process.
_default_context: "Optional[TrainContext]" = None
_context_lock = threading.Lock()


def _copy_doc(copy_func):
    def wrapped(func):
        func.__doc__ = copy_func.__doc__
        return func

    return wrapped


@PublicAPI(stability="stable")
class TrainContext:
    """Context for Ray training executions."""

    @_copy_doc(session.get_metadata)
    def get_metadata(self) -> Dict[str, Any]:
        return session.get_metadata()

    @_copy_doc(session.get_experiment_name)
    def get_experiment_name(self) -> str:
        return session.get_experiment_name()

    @_copy_doc(session.get_trial_name)
    def get_trial_name(self) -> str:
        return session.get_trial_name()

    @_copy_doc(session.get_trial_id)
    def get_trial_id(self) -> str:
        return session.get_trial_id()

    @_copy_doc(session.get_trial_resources)
    def get_trial_resources(self) -> "PlacementGroupFactory":
        return session.get_trial_resources()

    @_copy_doc(session.get_trial_dir)
    def get_trial_dir(self) -> str:
        return session.get_trial_dir()

    @_copy_doc(session.get_world_size)
    def get_world_size(self) -> int:
        return session.get_world_size()

    @_copy_doc(session.get_world_rank)
    def get_world_rank(self) -> int:
        return session.get_world_rank()

    @_copy_doc(session.get_local_rank)
    def get_local_rank(self) -> int:
        return session.get_local_rank()

    @_copy_doc(session.get_local_world_size)
    def get_local_world_size(self) -> int:
        return session.get_local_world_size()

    @_copy_doc(session.get_node_rank)
    def get_node_rank(self) -> int:
        return session.get_node_rank()

    @DeveloperAPI
    @_copy_doc(session.get_storage)
    def get_storage(self) -> StorageContext:
        return session.get_storage()


@PublicAPI(stability="stable")
def get_context() -> TrainContext:
    """Get or create a singleton training context.

    The context is only available in a training or tuning loop.

    See the :class:`~ray.train.TrainContext` API reference to see available methods.
    """
    global _default_context

    with _context_lock:
        if _default_context is None:
            _default_context = TrainContext()
        return _default_context
