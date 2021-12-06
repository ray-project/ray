from typing import Optional
import threading

import ray
from ray.ray_constants import env_bool
from ray.util.annotations import DeveloperAPI

# The context singleton on this process.
_default_context: "Optional[DatasetContext]" = None
_context_lock = threading.Lock()

# The max target block size in bytes for reads and transformations.
DEFAULT_TARGET_MAX_BLOCK_SIZE = 500 * 1024 * 1024

# Whether pandas block format is enabled.
DEFAULT_ENABLE_PANDAS_BLOCK = env_bool("RAY_DATA_PANDAS_BLOCK_FORMAT", True)


@DeveloperAPI
class DatasetContext:
    """Singleton for shared Dataset resources and configurations.

    This object is automatically propagated to workers and can be retrieved
    from the driver and remote workers via DatasetContext.get_current().
    """

    def __init__(
            self,
            block_owner: ray.actor.ActorHandle,
            target_max_block_size: int,
            enable_pandas_block: bool,
    ):
        """Private constructor (use get_current() instead)."""
        self.block_owner = block_owner
        self.target_max_block_size = target_max_block_size
        self.enable_pandas_block = enable_pandas_block

    @staticmethod
    def get_current() -> "DatasetContext":
        """Get or create a singleton context.

        If the context has not yet been created in this process, it will be
        initialized with default settings.
        """
        global _default_context

        with _context_lock:

            if _default_context is None:
                _default_context = DatasetContext(
                    None,
                    DEFAULT_TARGET_MAX_BLOCK_SIZE,
                    DEFAULT_ENABLE_PANDAS_BLOCK,
                )

            if _default_context.block_owner is None:
                owner = _DesignatedBlockOwner.options(
                    lifetime="detached").remote()
                ray.get(owner.ping.remote())

                # Clear the actor handle after Ray reinits since it's no longer
                # valid.
                def clear_owner():
                    if _default_context:
                        _default_context.block_owner = None

                ray.worker._post_init_hooks.append(clear_owner)
                _default_context.block_owner = owner

            return _default_context

    @staticmethod
    def _set_current(context: "DatasetContext") -> None:
        """Set the current context in a remote worker.

        This is used internally by Dataset to propagate the driver context to
        remote workers used for parallelization.
        """
        global _default_context
        _default_context = context


@ray.remote(num_cpus=0, placement_group=None)
class _DesignatedBlockOwner:
    def ping(self):
        return "ok"
