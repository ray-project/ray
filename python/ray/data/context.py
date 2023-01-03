import os
import threading
from typing import Optional

import ray
from ray.util.annotations import DeveloperAPI
from ray.util.scheduling_strategies import SchedulingStrategyT

# The context singleton on this process.
_default_context: "Optional[DatasetContext]" = None
_context_lock = threading.Lock()

# An estimate of what fraction of the object store a Dataset can use without too high
# a risk of triggering spilling. This is used to generate user warnings only.
ESTIMATED_SAFE_MEMORY_FRACTION = 0.25

# The max target block size in bytes for reads and transformations.
# We choose 512MiB as 8x less than the typical memory:core ratio of 4:1.
DEFAULT_TARGET_MAX_BLOCK_SIZE = 512 * 1024 * 1024

# Datasets will avoid creating blocks smaller than this size in bytes on read.
# This takes precedence over DEFAULT_MIN_PARALLELISM.
DEFAULT_TARGET_MIN_BLOCK_SIZE = 1 * 1024 * 1024

# Default buffer size when doing streaming reads from local or remote storage.
# This default appears to work well with most file sizes on remote storage systems,
# which is very sensitive to the buffer size.
DEFAULT_STREAMING_READ_BUFFER_SIZE = 32 * 1024 * 1024

# Whether dynamic block splitting is enabled.
# NOTE: disable dynamic block splitting when using Ray client.
DEFAULT_BLOCK_SPLITTING_ENABLED = not ray.util.client.ray.is_connected()

# Whether pandas block format is enabled.
# TODO (kfstorm): Remove this once stable.
DEFAULT_ENABLE_PANDAS_BLOCK = True

# Whether to enable stage-fusion optimizations for dataset pipelines.
DEFAULT_OPTIMIZE_FUSE_STAGES = True

# Whether to enable stage-reorder optimizations for dataset pipelines.
DEFAULT_OPTIMIZE_REORDER_STAGES = True

# Whether to furthermore fuse read stages. When this is enabled, data will also be
# re-read from the base dataset in each repetition of a DatasetPipeline.
DEFAULT_OPTIMIZE_FUSE_READ_STAGES = True

# Whether to furthermore fuse prior map tasks with shuffle stages.
DEFAULT_OPTIMIZE_FUSE_SHUFFLE_STAGES = True

# Minimum amount of parallelism to auto-detect for a dataset. Note that the min
# block size config takes precedence over this.
DEFAULT_MIN_PARALLELISM = 200

# Wether to use actor based block prefetcher.
DEFAULT_ACTOR_PREFETCHER_ENABLED = True

# Whether to use push-based shuffle by default.
DEFAULT_USE_PUSH_BASED_SHUFFLE = bool(
    os.environ.get("RAY_DATASET_PUSH_BASED_SHUFFLE", None)
)

# The default global scheduling strategy.
DEFAULT_SCHEDULING_STRATEGY = "DEFAULT"

# Whether to use Polars for tabular dataset sorts, groupbys, and aggregations.
DEFAULT_USE_POLARS = False

# Whether to use the new executor backend.
DEFAULT_NEW_EXECUTION_BACKEND = bool(
    int(os.environ.get("RAY_DATASET_NEW_EXECUTION_BACKEND", "1"))
)

# Whether to eagerly free memory (new backend only).
DEFAULT_EAGER_FREE = bool(int(os.environ.get("RAY_DATASET_EAGER_FREE", "1")))

# Whether to trace allocations / eager free (new backend only). This adds significant
# performance overheads and should only be used for debugging.
DEFAULT_TRACE_ALLOCATIONS = bool(
    int(os.environ.get("RAY_DATASET_TRACE_ALLOCATIONS", "0"))
)

# Whether to estimate in-memory decoding data size for data source.
DEFAULT_DECODING_SIZE_ESTIMATION_ENABLED = True

# Whether to automatically cast NumPy ndarray columns in Pandas DataFrames to tensor
# extension columns.
DEFAULT_ENABLE_TENSOR_EXTENSION_CASTING = True

# Whether to automatically print Dataset stats after execution.
# If disabled, users can still manually print stats with Dataset.stats().
DEFAULT_AUTO_LOG_STATS = False

# Use this to prefix important warning messages for the user.
WARN_PREFIX = "⚠️ "

# Use this to prefix important success messages for the user.
OK_PREFIX = "✔️ "

# Default batch size for batch transformations.
DEFAULT_BATCH_SIZE = 4096


@DeveloperAPI
class DatasetContext:
    """Singleton for shared Dataset resources and configurations.

    This object is automatically propagated to workers and can be retrieved
    from the driver and remote workers via DatasetContext.get_current().
    """

    def __init__(
        self,
        block_splitting_enabled: bool,
        target_max_block_size: int,
        target_min_block_size: int,
        streaming_read_buffer_size: int,
        enable_pandas_block: bool,
        optimize_fuse_stages: bool,
        optimize_fuse_read_stages: bool,
        optimize_fuse_shuffle_stages: bool,
        optimize_reorder_stages: bool,
        actor_prefetcher_enabled: bool,
        use_push_based_shuffle: bool,
        pipeline_push_based_shuffle_reduce_tasks: bool,
        scheduling_strategy: SchedulingStrategyT,
        use_polars: bool,
        new_execution_backend: bool,
        eager_free: bool,
        decoding_size_estimation: bool,
        min_parallelism: bool,
        enable_tensor_extension_casting: bool,
        enable_auto_log_stats: bool,
        trace_allocations: bool,
    ):
        """Private constructor (use get_current() instead)."""
        self.block_splitting_enabled = block_splitting_enabled
        self.target_max_block_size = target_max_block_size
        self.target_min_block_size = target_min_block_size
        self.streaming_read_buffer_size = streaming_read_buffer_size
        self.enable_pandas_block = enable_pandas_block
        self.optimize_fuse_stages = optimize_fuse_stages
        self.optimize_fuse_read_stages = optimize_fuse_read_stages
        self.optimize_fuse_shuffle_stages = optimize_fuse_shuffle_stages
        self.optimize_reorder_stages = optimize_reorder_stages
        self.actor_prefetcher_enabled = actor_prefetcher_enabled
        self.use_push_based_shuffle = use_push_based_shuffle
        self.pipeline_push_based_shuffle_reduce_tasks = (
            pipeline_push_based_shuffle_reduce_tasks
        )
        self.scheduling_strategy = scheduling_strategy
        self.use_polars = use_polars
        self.new_execution_backend = new_execution_backend
        self.eager_free = eager_free
        self.decoding_size_estimation = decoding_size_estimation
        self.min_parallelism = min_parallelism
        self.enable_tensor_extension_casting = enable_tensor_extension_casting
        self.enable_auto_log_stats = enable_auto_log_stats
        self.trace_allocations = trace_allocations

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
                    block_splitting_enabled=DEFAULT_BLOCK_SPLITTING_ENABLED,
                    target_max_block_size=DEFAULT_TARGET_MAX_BLOCK_SIZE,
                    target_min_block_size=DEFAULT_TARGET_MIN_BLOCK_SIZE,
                    streaming_read_buffer_size=DEFAULT_STREAMING_READ_BUFFER_SIZE,
                    enable_pandas_block=DEFAULT_ENABLE_PANDAS_BLOCK,
                    optimize_fuse_stages=DEFAULT_OPTIMIZE_FUSE_STAGES,
                    optimize_fuse_read_stages=DEFAULT_OPTIMIZE_FUSE_READ_STAGES,
                    optimize_fuse_shuffle_stages=DEFAULT_OPTIMIZE_FUSE_SHUFFLE_STAGES,
                    optimize_reorder_stages=DEFAULT_OPTIMIZE_REORDER_STAGES,
                    actor_prefetcher_enabled=DEFAULT_ACTOR_PREFETCHER_ENABLED,
                    use_push_based_shuffle=DEFAULT_USE_PUSH_BASED_SHUFFLE,
                    # NOTE(swang): We have to pipeline reduce tasks right now
                    # because of a scheduling bug at large scale.
                    # See https://github.com/ray-project/ray/issues/25412.
                    pipeline_push_based_shuffle_reduce_tasks=True,
                    scheduling_strategy=DEFAULT_SCHEDULING_STRATEGY,
                    use_polars=DEFAULT_USE_POLARS,
                    new_execution_backend=DEFAULT_NEW_EXECUTION_BACKEND,
                    eager_free=DEFAULT_EAGER_FREE,
                    decoding_size_estimation=DEFAULT_DECODING_SIZE_ESTIMATION_ENABLED,
                    min_parallelism=DEFAULT_MIN_PARALLELISM,
                    enable_tensor_extension_casting=(
                        DEFAULT_ENABLE_TENSOR_EXTENSION_CASTING
                    ),
                    enable_auto_log_stats=DEFAULT_AUTO_LOG_STATS,
                    trace_allocations=DEFAULT_TRACE_ALLOCATIONS,
                )

            # Check if using Ray client and disable dynamic block splitting.
            if ray.util.client.ray.is_connected():
                _default_context.block_splitting_enabled = False

            return _default_context

    @staticmethod
    def _set_current(context: "DatasetContext") -> None:
        """Set the current context in a remote worker.

        This is used internally by Dataset to propagate the driver context to
        remote workers used for parallelization.
        """
        global _default_context
        _default_context = context
