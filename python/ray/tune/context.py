import threading
from typing import Any, Dict, Optional

from ray.train._internal import session
from ray.train.context import TrainContext as TrainV1Context
from ray.train.context import _copy_doc
from ray.util.annotations import Deprecated, PublicAPI


# The context singleton on this process.
_tune_context: Optional["TuneContext"] = None
_tune_context_lock = threading.Lock()


_TRAIN_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE = (
    "`{}` is deprecated for Ray Tune because there is no concept of worker ranks "
    "for Ray Tune, so these methods only make sense to use in the context of "
    "a Ray Train worker."
)


@PublicAPI(stability="stable")
class TuneContext(TrainV1Context):
    """Context for Ray Tune training executions."""

    @Deprecated
    def get_metadata(self) -> Dict[str, Any]:
        raise DeprecationWarning(
            "`get_metadata` is deprecated for Ray Tune, as it has never been usable."
        )

    @Deprecated(
        message=_TRAIN_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_world_size"),
        warning=True,
    )
    @_copy_doc(TrainV1Context.get_world_size)
    def get_world_size(self) -> int:
        return session.get_world_size()

    @Deprecated(
        message=_TRAIN_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_world_rank"),
        warning=True,
    )
    @_copy_doc(TrainV1Context.get_world_rank)
    def get_world_rank(self) -> int:
        return session.get_world_rank()

    @Deprecated(
        message=_TRAIN_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_local_rank"),
        warning=True,
    )
    @_copy_doc(TrainV1Context.get_local_rank)
    def get_local_rank(self) -> int:
        return session.get_local_rank()

    @Deprecated(
        message=_TRAIN_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format(
            "get_local_world_size"
        ),
        warning=True,
    )
    @_copy_doc(TrainV1Context.get_local_world_size)
    def get_local_world_size(self) -> int:
        return session.get_local_world_size()

    @Deprecated(
        message=_TRAIN_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_node_rank"),
        warning=True,
    )
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
