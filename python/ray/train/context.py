import threading
from typing import TYPE_CHECKING, Any, Dict, Optional

from ray.train._internal import session
from ray.train._internal.storage import StorageContext
from ray.train.constants import (
    _v2_migration_warnings_enabled,
    V2_MIGRATION_GUIDE_MESSAGE,
)
from ray.train.utils import _copy_doc, _log_deprecation_warning
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.tune.execution.placement_groups import PlacementGroupFactory


# The context singleton on this process.
_default_context: "Optional[TrainContext]" = None
_context_lock = threading.Lock()


_GET_METADATA_DEPRECATION_MESSAGE = (
    "`get_metadata` was an experimental API that accessed the metadata passed "
    "to `<Framework>Trainer(metadata=...)`. This API can be replaced by passing "
    "the metadata directly to the training function (e.g., via `train_loop_config`). "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)

_TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE = (
    "`{}` is deprecated because the concept of a `Trial` will "
    "soon be removed in Ray Train."
    "Ray Train will no longer assume that it's running within a Ray Tune `Trial` "
    "in the future. "
    f"{V2_MIGRATION_GUIDE_MESSAGE}"
)


@PublicAPI(stability="stable")
class TrainContext:
    """Context containing metadata that can be accessed within Ray Train workers."""

    @_copy_doc(session.get_experiment_name)
    def get_experiment_name(self) -> str:
        return session.get_experiment_name()

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

    # Deprecated APIs

    @Deprecated(
        message=_GET_METADATA_DEPRECATION_MESSAGE,
        warning=_v2_migration_warnings_enabled(),
    )
    @_copy_doc(session.get_metadata)
    def get_metadata(self) -> Dict[str, Any]:
        return session.get_metadata()

    @Deprecated(
        message=_TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_name"),
        warning=_v2_migration_warnings_enabled(),
    )
    @_copy_doc(session.get_trial_name)
    def get_trial_name(self) -> str:
        return session.get_trial_name()

    @Deprecated(
        message=_TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_id"),
        warning=_v2_migration_warnings_enabled(),
    )
    @_copy_doc(session.get_trial_id)
    def get_trial_id(self) -> str:
        return session.get_trial_id()

    @Deprecated(
        message=_TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format(
            "get_trial_resources"
        ),
        warning=_v2_migration_warnings_enabled(),
    )
    @_copy_doc(session.get_trial_resources)
    def get_trial_resources(self) -> "PlacementGroupFactory":
        return session.get_trial_resources()

    @Deprecated(
        message=_TUNE_SPECIFIC_CONTEXT_DEPRECATION_MESSAGE.format("get_trial_dir"),
        warning=_v2_migration_warnings_enabled(),
    )
    @_copy_doc(session.get_trial_dir)
    def get_trial_dir(self) -> str:
        return session.get_trial_dir()


@PublicAPI(stability="stable")
def get_context() -> TrainContext:
    """Get or create a singleton training context.

    The context is only available within a function passed to Ray Train.

    See the :class:`~ray.train.TrainContext` API reference to see available methods.
    """
    from ray.tune.trainable.trainable_fn_utils import _in_tune_session

    # If we are running in a Tune function, switch to Tune context.
    if _in_tune_session():
        from ray.tune import get_context as get_tune_context

        if _v2_migration_warnings_enabled():
            _log_deprecation_warning(
                "`ray.train.get_context()` should be switched to "
                "`ray.tune.get_context()` when running in a function "
                "passed to Ray Tune. This will be an error in the future. "
                f"{V2_MIGRATION_GUIDE_MESSAGE}"
            )
        return get_tune_context()

    global _default_context

    with _context_lock:
        if _default_context is None:
            _default_context = TrainContext()
        return _default_context
