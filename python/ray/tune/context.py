import threading
from typing import Optional

from ray.train._internal import session
from ray.util.annotations import Deprecated, PublicAPI

from ray.train.context import TrainContext as TrainV1Context, _copy_doc

# The context singleton on this process.
_tune_context: Optional["TuneContext"] = None
_tune_context_lock = threading.Lock()


@PublicAPI(stability="stable")
class TuneContext(TrainV1Context):
    @Deprecated
    @_copy_doc(TrainV1Context.get_world_size)
    def get_world_size(self) -> int:
        return session.get_world_size()

    @Deprecated
    @_copy_doc(TrainV1Context.get_world_rank)
    def get_world_rank(self) -> int:
        return session.get_world_rank()

    @Deprecated
    @_copy_doc(TrainV1Context.get_local_rank)
    def get_local_rank(self) -> int:
        return session.get_local_rank()

    @Deprecated
    @_copy_doc(TrainV1Context.get_local_world_size)
    def get_local_world_size(self) -> int:
        return session.get_local_world_size()

    @Deprecated
    @_copy_doc(TrainV1Context.get_node_rank)
    def get_node_rank(self) -> int:
        return session.get_node_rank()


@PublicAPI(stability="stable")
def get_context() -> TuneContext:
    """Get or create a singleton Ray Tune context.

    The context is only available in a tune function passed to the `ray.tune.Tuner`.

    See the :class:`~ray.tune.TuneContext` API reference to see available methods.
    """
    global _tune_context

    with _tune_context_lock:
        if _tune_context is None:
            # TODO(justinvyu): This default should be a dummy context
            # that is only used for testing / running outside of Tune.
            _tune_context = TuneContext()
        return _tune_context
