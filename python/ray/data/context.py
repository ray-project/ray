from typing import Optional
import threading
import os

import ray
from ray.util.annotations import DeveloperAPI


# The context singleton on this process.
_default_context: "Optional[DatasetContext]" = None
_context_lock = threading.Lock()

# The max target block size in bytes for reads and transformations.
DEFAULT_TARGET_MAX_BLOCK_SIZE = 2048 * 1024 * 1024

# Whether block splitting is on by default
DEFAULT_BLOCK_SPLITTING_ENABLED = False

# Whether pandas block format is enabled.
# TODO (kfstorm): Remove this once stable.
DEFAULT_ENABLE_PANDAS_BLOCK = True

# Whether to enable stage-fusion optimizations for dataset pipelines.
DEFAULT_OPTIMIZE_FUSE_STAGES = True

# Whether to furthermore fuse read stages. When this is enabled, data will also be
# re-read from the base dataset in each repetition of a DatasetPipeline.
DEFAULT_OPTIMIZE_FUSE_READ_STAGES = True

# Whether to furthermore fuse prior map tasks with shuffle stages.
DEFAULT_OPTIMIZE_FUSE_SHUFFLE_STAGES = True

# Wether to use actor based block prefetcher.
DEFAULT_ACTOR_PREFETCHER_ENABLED = True

# Whether to use push-based shuffle by default.
DEFAULT_USE_PUSH_BASED_SHUFFLE = bool(
    os.environ.get("RAY_DATASET_PUSH_BASED_SHUFFLE", None)
)

# Whether to use Polars for tabular dataset sorts, groupbys, and aggregations.
DEFAULT_USE_POLARS = False


@DeveloperAPI
class DatasetContext:
    """Singleton for shared Dataset resources and configurations.

    This object is automatically propagated to workers and can be retrieved
    from the driver and remote workers via DatasetContext.get_current().
    """

    def __init__(
        self,
        block_owner: ray.actor.ActorHandle,
        block_splitting_enabled: bool,
        target_max_block_size: int,
        enable_pandas_block: bool,
        optimize_fuse_stages: bool,
        optimize_fuse_read_stages: bool,
        optimize_fuse_shuffle_stages: bool,
        actor_prefetcher_enabled: bool,
        use_push_based_shuffle: bool,
        use_polars: bool,
    ):
        """Private constructor (use get_current() instead)."""
        self.block_owner = block_owner
        self.block_splitting_enabled = block_splitting_enabled
        self.target_max_block_size = target_max_block_size
        self.enable_pandas_block = enable_pandas_block
        self.optimize_fuse_stages = optimize_fuse_stages
        self.optimize_fuse_read_stages = optimize_fuse_read_stages
        self.optimize_fuse_shuffle_stages = optimize_fuse_shuffle_stages
        self.actor_prefetcher_enabled = actor_prefetcher_enabled
        self.use_push_based_shuffle = use_push_based_shuffle
        self.use_polars = use_polars

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
                    block_owner=None,
                    block_splitting_enabled=DEFAULT_BLOCK_SPLITTING_ENABLED,
                    target_max_block_size=DEFAULT_TARGET_MAX_BLOCK_SIZE,
                    enable_pandas_block=DEFAULT_ENABLE_PANDAS_BLOCK,
                    optimize_fuse_stages=DEFAULT_OPTIMIZE_FUSE_STAGES,
                    optimize_fuse_read_stages=DEFAULT_OPTIMIZE_FUSE_READ_STAGES,
                    optimize_fuse_shuffle_stages=DEFAULT_OPTIMIZE_FUSE_SHUFFLE_STAGES,
                    actor_prefetcher_enabled=DEFAULT_ACTOR_PREFETCHER_ENABLED,
                    use_push_based_shuffle=DEFAULT_USE_PUSH_BASED_SHUFFLE,
                    use_polars=DEFAULT_USE_POLARS,
                )

            if (
                _default_context.block_splitting_enabled
                and _default_context.block_owner is None
            ):
                owner = _DesignatedBlockOwner.remote()
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
