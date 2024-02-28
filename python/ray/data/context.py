import os
import threading
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import ray
from ray._private.ray_constants import env_integer
from ray.util.annotations import DeveloperAPI
from ray.util.scheduling_strategies import SchedulingStrategyT

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import ExecutionOptions

# The context singleton on this process.
_default_context: "Optional[DataContext]" = None
_context_lock = threading.Lock()

# An estimate of what fraction of the object store a Dataset can use without too high
# a risk of triggering spilling. This is used to generate user warnings only.
ESTIMATED_SAFE_MEMORY_FRACTION = 0.25

# The max target block size in bytes for reads and transformations.  We choose
# 128MiB: With streaming execution and num_cpus many concurrent tasks, the
# memory footprint will be about 2 * num_cpus * target_max_block_size ~= RAM *
# DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION * 0.3 (default object store memory
# fraction set by Ray core), assuming typical memory:core ratio of 4:1.
DEFAULT_TARGET_MAX_BLOCK_SIZE = 128 * 1024 * 1024

# The max target block size in bytes for shuffle ops (random_shuffle, sort,
# repartition). Set a higher target block size because we have to materialize
# all input blocks anyway, so there is no performance advantage to having
# smaller blocks. Setting a larger block size allows avoiding overhead from an
# excessive number of partitions.
# We choose 1GiB as 4x less than the typical memory:core ratio (4:1).
DEFAULT_SHUFFLE_TARGET_MAX_BLOCK_SIZE = 1024 * 1024 * 1024

# We will attempt to slice blocks whose size exceeds this factor *
# target_max_block_size. We will warn the user if slicing fails and we produce
# blocks larger than this threshold.
MAX_SAFE_BLOCK_SIZE_FACTOR = 1.5

# Dataset will avoid creating blocks smaller than this size in bytes on read.
# This takes precedence over DEFAULT_MIN_PARALLELISM.
DEFAULT_TARGET_MIN_BLOCK_SIZE = 1 * 1024 * 1024

# Default buffer size when doing streaming reads from local or remote storage.
# This default appears to work well with most file sizes on remote storage systems,
# which is very sensitive to the buffer size.
DEFAULT_STREAMING_READ_BUFFER_SIZE = 32 * 1024 * 1024

# Whether pandas block format is enabled.
# TODO (kfstorm): Remove this once stable.
DEFAULT_ENABLE_PANDAS_BLOCK = True

# Minimum amount of parallelism to auto-detect for a dataset. Note that the min
# block size config takes precedence over this.
DEFAULT_MIN_PARALLELISM = 200

# Wether to use actor based block prefetcher.
DEFAULT_ACTOR_PREFETCHER_ENABLED = False

# Whether to use push-based shuffle by default.
DEFAULT_USE_PUSH_BASED_SHUFFLE = bool(
    os.environ.get("RAY_DATA_PUSH_BASED_SHUFFLE", None)
)

# The default global scheduling strategy. Note that for tasks with large args,
# DEFAULT_SCHEDULING_STRATEGY_LARGE_ARGS applies.
DEFAULT_SCHEDULING_STRATEGY = "SPREAD"

# Default scheduling strategy for tasks with large args. This enables locality-based
# scheduling in Ray for tasks where arg data transfer is a bottleneck.
DEFAULT_SCHEDULING_STRATEGY_LARGE_ARGS = "DEFAULT"

# Size in bytes after which point task arguments are considered large. Choose a value
# here at which point data transfer overhead becomes significant in comparison to
# task scheduling (i.e., low tens of ms).
DEFAULT_LARGE_ARGS_THRESHOLD = 50 * 1024 * 1024

# Whether to use Polars for tabular dataset sorts, groupbys, and aggregations.
DEFAULT_USE_POLARS = False

# Whether to eagerly free memory (new backend only).
DEFAULT_EAGER_FREE = bool(int(os.environ.get("RAY_DATA_EAGER_FREE", "1")))

# Whether to trace allocations / eager free (new backend only). This adds significant
# performance overheads and should only be used for debugging.
DEFAULT_TRACE_ALLOCATIONS = bool(int(os.environ.get("RAY_DATA_TRACE_ALLOCATIONS", "0")))

# Whether to estimate in-memory decoding data size for data source.
DEFAULT_DECODING_SIZE_ESTIMATION_ENABLED = True

# Whether to automatically cast NumPy ndarray columns in Pandas DataFrames to tensor
# extension columns.
DEFAULT_ENABLE_TENSOR_EXTENSION_CASTING = True

# Whether to automatically print Dataset stats after execution.
# If disabled, users can still manually print stats with Dataset.stats().
DEFAULT_AUTO_LOG_STATS = False

# Whether stats logs should be verbose. This will include fields such
# as `extra_metrics` in the stats output, which are excluded by default.
DEFAULT_VERBOSE_STATS_LOG = False

# Set this env var to enable distributed tqdm (experimental).
DEFAULT_USE_RAY_TQDM = bool(int(os.environ.get("RAY_TQDM", "1")))

# If driver memory exceeds this threshold, warn the user.
# For now, this only applies to shuffle ops because most other ops are unlikely
# to use as much driver memory.
DEFAULT_WARN_ON_DRIVER_MEMORY_USAGE_BYTES = 2 * 1024 * 1024 * 1024

# Use this to prefix important warning messages for the user.
WARN_PREFIX = "⚠️ "

# Use this to prefix important success messages for the user.
OK_PREFIX = "✔️ "

# Default batch size for batch transformations.
DEFAULT_BATCH_SIZE = 1024

# Whether to enable progress bars.
DEFAULT_ENABLE_PROGRESS_BARS = not bool(
    env_integer("RAY_DATA_DISABLE_PROGRESS_BARS", 0)
)

# Whether to enable get_object_locations for metric
DEFAULT_ENABLE_GET_OBJECT_LOCATIONS_FOR_METRICS = False

DEFAULT_WRITE_FILE_RETRY_ON_ERRORS = [
    "AWS Error INTERNAL_FAILURE",
    "AWS Error NETWORK_CONNECTION",
    "AWS Error SLOW_DOWN",
]

# The application-level errors that actor task would retry.
# Default to `False` to not retry on any errors.
# Set to `True` to retry all errors, or set to a list of errors to retry.
# This follows same format as `retry_exceptions` in Ray Core.
DEFAULT_ACTOR_TASK_RETRY_ON_ERRORS = False

# Whether to enable ReservationOpResourceAllocator by default.
DEFAULT_ENABLE_OP_RESOURCE_RESERVATION = bool(
    os.environ.get("RAY_DATA_ENABLE_OP_RESOURCE_RESERVATION", "1")
)

# The default reservation ratio for ReservationOpResourceAllocator.
DEFAULT_OP_RESOURCE_RESERVATION_RATIO = float(
    os.environ.get("RAY_DATA_OP_RESERVATION_RATIO", "0.5")
)

# Default value of the max number of blocks that can be buffered at the
# streaming generator of each `DataOpTask`.
# Note, if this value is too large, we'll need to allocate more memory
# buffer for the pending task outputs, which may lead to bad performance
# as we may not have enough memory buffer for the operator outputs.
# If the value is too small, the task may be frequently blocked due to
# streaming generator backpressure.
DEFAULT_MAX_NUM_BLOCKS_IN_STREAMING_GEN_BUFFER = 2


@DeveloperAPI
class DataContext:
    """Singleton for shared Dataset resources and configurations.

    This object is automatically propagated to workers and can be retrieved
    from the driver and remote workers via DataContext.get_current().
    """

    def __init__(
        self,
        target_max_block_size: int,
        target_shuffle_max_block_size: int,
        target_min_block_size: int,
        streaming_read_buffer_size: int,
        enable_pandas_block: bool,
        actor_prefetcher_enabled: bool,
        use_push_based_shuffle: bool,
        pipeline_push_based_shuffle_reduce_tasks: bool,
        scheduling_strategy: SchedulingStrategyT,
        scheduling_strategy_large_args: SchedulingStrategyT,
        large_args_threshold: int,
        use_polars: bool,
        eager_free: bool,
        decoding_size_estimation: bool,
        min_parallelism: bool,
        enable_tensor_extension_casting: bool,
        enable_auto_log_stats: bool,
        verbose_stats_log: bool,
        trace_allocations: bool,
        execution_options: "ExecutionOptions",
        use_ray_tqdm: bool,
        enable_progress_bars: bool,
        enable_get_object_locations_for_metrics: bool,
        write_file_retry_on_errors: List[str],
        warn_on_driver_memory_usage_bytes: int,
        actor_task_retry_on_errors: Union[bool, List[BaseException]],
    ):
        """Private constructor (use get_current() instead)."""
        self.target_max_block_size = target_max_block_size
        self.target_shuffle_max_block_size = target_shuffle_max_block_size
        self.target_min_block_size = target_min_block_size
        self.streaming_read_buffer_size = streaming_read_buffer_size
        self.enable_pandas_block = enable_pandas_block
        self.actor_prefetcher_enabled = actor_prefetcher_enabled
        self.use_push_based_shuffle = use_push_based_shuffle
        self.pipeline_push_based_shuffle_reduce_tasks = (
            pipeline_push_based_shuffle_reduce_tasks
        )
        self.scheduling_strategy = scheduling_strategy
        self.scheduling_strategy_large_args = scheduling_strategy_large_args
        self.large_args_threshold = large_args_threshold
        self.use_polars = use_polars
        self.eager_free = eager_free
        self.decoding_size_estimation = decoding_size_estimation
        self.min_parallelism = min_parallelism
        self.enable_tensor_extension_casting = enable_tensor_extension_casting
        self.enable_auto_log_stats = enable_auto_log_stats
        self.verbose_stats_logs = verbose_stats_log
        self.trace_allocations = trace_allocations
        # TODO: expose execution options in Dataset public APIs.
        self.execution_options = execution_options
        self.use_ray_tqdm = use_ray_tqdm
        self.enable_progress_bars = enable_progress_bars
        self.enable_get_object_locations_for_metrics = (
            enable_get_object_locations_for_metrics
        )
        self.write_file_retry_on_errors = write_file_retry_on_errors
        self.warn_on_driver_memory_usage_bytes = warn_on_driver_memory_usage_bytes
        self.actor_task_retry_on_errors = actor_task_retry_on_errors
        # The additonal ray remote args that should be added to
        # the task-pool-based data tasks.
        self._task_pool_data_task_remote_args: Dict[str, Any] = {}
        # Max number of blocks that are allowed to have errors, unlimited if negative.
        # This option allows application-level exceptions in block processing tasks.
        # These exceptions may be caused by UDFs (e.g., due to corrupted data samples)
        # or IO errors.
        # Data in the failed blocks will be dropped.
        # This option can be useful to prevent a long-running job from failing due to
        # a small number of bad blocks.
        self.max_errored_blocks = 0
        # The extra key-value style configs.
        # These configs are managed by individual components or plugins via
        # `set_config`, `get_config` and `remove_config`.
        # The reason why we use a dict instead of individual fields is to decouple
        # the DataContext from the plugin implementations, as well as to avoid
        # circular dependencies.
        self._kv_configs: Dict[str, Any] = {}
        self._max_num_blocks_in_streaming_gen_buffer = (
            DEFAULT_MAX_NUM_BLOCKS_IN_STREAMING_GEN_BUFFER
        )
        # Whether to enable ReservationOpResourceAllocator.
        self.op_resource_reservation_enabled = DEFAULT_ENABLE_OP_RESOURCE_RESERVATION
        # The reservation ratio for ReservationOpResourceAllocator.
        self.op_resource_reservation_ratio = DEFAULT_OP_RESOURCE_RESERVATION_RATIO

    @staticmethod
    def get_current() -> "DataContext":
        """Get or create a singleton context.

        If the context has not yet been created in this process, it will be
        initialized with default settings.
        """

        global _default_context

        with _context_lock:
            if _default_context is None:
                _default_context = DataContext(
                    target_max_block_size=DEFAULT_TARGET_MAX_BLOCK_SIZE,
                    target_shuffle_max_block_size=DEFAULT_SHUFFLE_TARGET_MAX_BLOCK_SIZE,
                    target_min_block_size=DEFAULT_TARGET_MIN_BLOCK_SIZE,
                    streaming_read_buffer_size=DEFAULT_STREAMING_READ_BUFFER_SIZE,
                    enable_pandas_block=DEFAULT_ENABLE_PANDAS_BLOCK,
                    actor_prefetcher_enabled=DEFAULT_ACTOR_PREFETCHER_ENABLED,
                    use_push_based_shuffle=DEFAULT_USE_PUSH_BASED_SHUFFLE,
                    # NOTE(swang): We have to pipeline reduce tasks right now
                    # because of a scheduling bug at large scale.
                    # See https://github.com/ray-project/ray/issues/25412.
                    pipeline_push_based_shuffle_reduce_tasks=True,
                    scheduling_strategy=DEFAULT_SCHEDULING_STRATEGY,
                    scheduling_strategy_large_args=(
                        DEFAULT_SCHEDULING_STRATEGY_LARGE_ARGS
                    ),
                    large_args_threshold=DEFAULT_LARGE_ARGS_THRESHOLD,
                    use_polars=DEFAULT_USE_POLARS,
                    eager_free=DEFAULT_EAGER_FREE,
                    decoding_size_estimation=DEFAULT_DECODING_SIZE_ESTIMATION_ENABLED,
                    min_parallelism=DEFAULT_MIN_PARALLELISM,
                    enable_tensor_extension_casting=(
                        DEFAULT_ENABLE_TENSOR_EXTENSION_CASTING
                    ),
                    enable_auto_log_stats=DEFAULT_AUTO_LOG_STATS,
                    verbose_stats_log=DEFAULT_VERBOSE_STATS_LOG,
                    trace_allocations=DEFAULT_TRACE_ALLOCATIONS,
                    execution_options=ray.data.ExecutionOptions(),
                    use_ray_tqdm=DEFAULT_USE_RAY_TQDM,
                    enable_progress_bars=DEFAULT_ENABLE_PROGRESS_BARS,
                    enable_get_object_locations_for_metrics=DEFAULT_ENABLE_GET_OBJECT_LOCATIONS_FOR_METRICS,  # noqa E501
                    write_file_retry_on_errors=DEFAULT_WRITE_FILE_RETRY_ON_ERRORS,
                    warn_on_driver_memory_usage_bytes=(
                        DEFAULT_WARN_ON_DRIVER_MEMORY_USAGE_BYTES
                    ),
                    actor_task_retry_on_errors=DEFAULT_ACTOR_TASK_RETRY_ON_ERRORS,
                )

            return _default_context

    @staticmethod
    def _set_current(context: "DataContext") -> None:
        """Set the current context in a remote worker.

        This is used internally by Dataset to propagate the driver context to
        remote workers used for parallelization.
        """
        global _default_context
        _default_context = context

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get the value for a key-value style config.

        Args:
            key: The key of the config.
            default: The default value to return if the key is not found.
        Returns: The value for the key, or the default value if the key is not found.
        """
        return self._kv_configs.get(key, default)

    def set_config(self, key: str, value: Any) -> None:
        """Set the value for a key-value style config.

        Args:
            key: The key of the config.
            value: The value of the config.
        """
        self._kv_configs[key] = value

    def remove_config(self, key: str) -> None:
        """Remove a key-value style config.

        Args:
            key: The key of the config.
        """
        self._kv_configs.pop(key, None)


# Backwards compatibility alias.
DatasetContext = DataContext
