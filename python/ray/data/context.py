import copy
import enum
import logging
import os
import threading
import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import ray
from ray._private.ray_constants import env_bool, env_float, env_integer
from ray._private.worker import WORKER_MODE
from ray.data._internal.logging import update_dataset_logger_for_worker
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once
from ray.util.scheduling_strategies import SchedulingStrategyT

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import ExecutionOptions
    from ray.data._internal.issue_detection.issue_detector_configuration import (
        IssueDetectorsConfiguration,
    )

logger = logging.getLogger(__name__)

# The context singleton on this process.
_default_context: "Optional[DataContext]" = None
_context_lock = threading.Lock()


@DeveloperAPI(stability="alpha")
class ShuffleStrategy(str, enum.Enum):
    """Shuffle strategy determines shuffling algorithm employed by operations
    like aggregate, repartition, etc"""

    SORT_SHUFFLE_PULL_BASED = "sort_shuffle_pull_based"
    SORT_SHUFFLE_PUSH_BASED = "sort_shuffle_push_based"
    HASH_SHUFFLE = "hash_shuffle"


# We chose 128MiB for default: With streaming execution and num_cpus many concurrent
# tasks, the memory footprint will be about 2 * num_cpus * target_max_block_size ~= RAM
# * DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION * 0.3 (default object store memory
# fraction set by Ray core), assuming typical memory:core ratio of 4:1.
DEFAULT_TARGET_MAX_BLOCK_SIZE = 128 * 1024 * 1024

# We set a higher target block size because we have to materialize
# all input blocks anyway, so there is no performance advantage to having
# smaller blocks. Setting a larger block size allows avoiding overhead from an
# excessive number of partitions.
# We choose 1GiB as 4x less than the typical memory:core ratio (4:1).
DEFAULT_SHUFFLE_TARGET_MAX_BLOCK_SIZE = 1024 * 1024 * 1024

# We will attempt to slice blocks whose size exceeds this factor *
# target_max_block_size. We will warn the user if slicing fails and we produce
# blocks larger than this threshold.
MAX_SAFE_BLOCK_SIZE_FACTOR = 1.5

DEFAULT_TARGET_MIN_BLOCK_SIZE = 1 * 1024 * 1024

# This default appears to work well with most file sizes on remote storage systems,
# which is very sensitive to the buffer size.
DEFAULT_STREAMING_READ_BUFFER_SIZE = 32 * 1024 * 1024

DEFAULT_ENABLE_PANDAS_BLOCK = True

DEFAULT_PANDAS_BLOCK_IGNORE_METADATA = env_bool(
    "RAY_DATA_PANDAS_BLOCK_IGNORE_METADATA", False
)

DEFAULT_READ_OP_MIN_NUM_BLOCKS = 200

DEFAULT_ACTOR_PREFETCHER_ENABLED = False

DEFAULT_ITER_GET_BLOCK_BATCH_SIZE = env_integer(
    "RAY_DATA_ITER_GET_BLOCK_BATCH_SIZE", 32
)

DEFAULT_USE_PUSH_BASED_SHUFFLE = bool(
    os.environ.get("RAY_DATA_PUSH_BASED_SHUFFLE", None)
)

DEFAULT_SHUFFLE_STRATEGY = os.environ.get(
    "RAY_DATA_DEFAULT_SHUFFLE_STRATEGY", ShuffleStrategy.HASH_SHUFFLE
)

DEFAULT_MAX_HASH_SHUFFLE_AGGREGATORS = env_integer(
    "RAY_DATA_MAX_HASH_SHUFFLE_AGGREGATORS", 128
)

DEFAULT_SCHEDULING_STRATEGY = "SPREAD"

# This default enables locality-based scheduling in Ray for tasks where arg data
# transfer is a bottleneck.
DEFAULT_SCHEDULING_STRATEGY_LARGE_ARGS = "DEFAULT"

DEFAULT_LARGE_ARGS_THRESHOLD = 50 * 1024 * 1024

DEFAULT_USE_POLARS = False

DEFAULT_USE_POLARS_SORT = False

DEFAULT_EAGER_FREE = bool(int(os.environ.get("RAY_DATA_EAGER_FREE", "0")))

DEFAULT_DECODING_SIZE_ESTIMATION_ENABLED = True

DEFAULT_MIN_PARALLELISM = env_integer("RAY_DATA_DEFAULT_MIN_PARALLELISM", 200)

DEFAULT_ENABLE_TENSOR_EXTENSION_CASTING = env_bool(
    "RAY_DATA_ENABLE_TENSOR_EXTENSION_CASTING",
    True,
)

# NOTE: V1 tensor type format only supports tensors of no more than 2Gb in
#       total cumulative size (due to it internally utilizing int32 offsets)
#
#       V2 in turn relies on int64 offsets, therefore having a limit of ~9Eb (exabytes)
DEFAULT_USE_ARROW_TENSOR_V2 = env_bool("RAY_DATA_USE_ARROW_TENSOR_V2", True)

DEFAULT_AUTO_LOG_STATS = False

DEFAULT_VERBOSE_STATS_LOG = False

DEFAULT_TRACE_ALLOCATIONS = bool(int(os.environ.get("RAY_DATA_TRACE_ALLOCATIONS", "0")))

DEFAULT_LOG_INTERNAL_STACK_TRACE_TO_STDOUT = env_bool(
    "RAY_DATA_LOG_INTERNAL_STACK_TRACE_TO_STDOUT", False
)

DEFAULT_RAY_DATA_RAISE_ORIGINAL_MAP_EXCEPTION = env_bool(
    "RAY_DATA_RAISE_ORIGINAL_MAP_EXCEPTION", False
)

DEFAULT_USE_RAY_TQDM = bool(int(os.environ.get("RAY_TQDM", "1")))

# Globally enable or disable all progress bars.
# If this is False, both the global and operator-level progress bars are disabled.
DEFAULT_ENABLE_PROGRESS_BARS = not bool(
    env_integer("RAY_DATA_DISABLE_PROGRESS_BARS", 0)
)
DEFAULT_ENABLE_PROGRESS_BAR_NAME_TRUNCATION = env_bool(
    "RAY_DATA_ENABLE_PROGRESS_BAR_NAME_TRUNCATION", True
)

# Progress bar log interval in seconds
DEFAULT_PROGRESS_BAR_LOG_INTERVAL = env_integer("RAY_DATA_PROGRESS_LOG_INTERVAL", 5)

# Globally enable or disable experimental rich progress bars. This is a new
# interface to replace the old tqdm progress bar implementation.
DEFAULT_ENABLE_RICH_PROGRESS_BARS = bool(
    env_integer("RAY_DATA_ENABLE_RICH_PROGRESS_BARS", 0)
)

DEFAULT_ENFORCE_SCHEMAS = env_bool("RAY_DATA_ENFORCE_SCHEMAS", False)

DEFAULT_ENABLE_GET_OBJECT_LOCATIONS_FOR_METRICS = False


# `write_file_retry_on_errors` is deprecated in favor of `retried_io_errors`. You
# shouldn't need to modify `DEFAULT_WRITE_FILE_RETRY_ON_ERRORS`.
DEFAULT_WRITE_FILE_RETRY_ON_ERRORS = (
    "AWS Error INTERNAL_FAILURE",
    "AWS Error NETWORK_CONNECTION",
    "AWS Error SLOW_DOWN",
    "AWS Error UNKNOWN (HTTP status 503)",
)

DEFAULT_RETRIED_IO_ERRORS = (
    "AWS Error INTERNAL_FAILURE",
    "AWS Error NETWORK_CONNECTION",
    "AWS Error SLOW_DOWN",
    "AWS Error UNKNOWN (HTTP status 503)",
    "AWS Error SERVICE_UNAVAILABLE",
)

DEFAULT_WARN_ON_DRIVER_MEMORY_USAGE_BYTES = 2 * 1024 * 1024 * 1024

DEFAULT_ACTOR_TASK_RETRY_ON_ERRORS = False

DEFAULT_ACTOR_INIT_RETRY_ON_ERRORS = False

DEFAULT_ACTOR_INIT_MAX_RETRIES = 3

DEFAULT_ENABLE_OP_RESOURCE_RESERVATION = env_bool(
    "RAY_DATA_ENABLE_OP_RESOURCE_RESERVATION", True
)

DEFAULT_OP_RESOURCE_RESERVATION_RATIO = float(
    os.environ.get("RAY_DATA_OP_RESERVATION_RATIO", "0.5")
)

DEFAULT_MAX_ERRORED_BLOCKS = 0

# Use this to prefix important warning messages for the user.
WARN_PREFIX = "⚠️ "

# Use this to prefix important success messages for the user.
OK_PREFIX = "✔️ "

# The default batch size for batch transformations before it was changed to `None`.
LEGACY_DEFAULT_BATCH_SIZE = 1024

# Default value of the max number of blocks that can be buffered at the
# streaming generator of each `DataOpTask`.
# Note, if this value is too large, we'll need to allocate more memory
# buffer for the pending task outputs, which may lead to bad performance
# as we may not have enough memory buffer for the operator outputs.
# If the value is too small, the task may be frequently blocked due to
# streaming generator backpressure.
DEFAULT_MAX_NUM_BLOCKS_IN_STREAMING_GEN_BUFFER = 2

# Default value for whether or not to try to create directories for write
# calls if the URI is an S3 URI.
DEFAULT_S3_TRY_CREATE_DIR = False

DEFAULT_WAIT_FOR_MIN_ACTORS_S = env_integer(
    "RAY_DATA_DEFAULT_WAIT_FOR_MIN_ACTORS_S", -1
)

DEFAULT_ACTOR_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR = env_integer(
    "RAY_DATA_ACTOR_DEFAULT_MAX_TASKS_IN_FLIGHT_TO_MAX_CONCURRENCY_FACTOR", 2
)

# Enable per node metrics reporting for Ray Data, disabled by default.
DEFAULT_ENABLE_PER_NODE_METRICS = bool(
    int(os.environ.get("RAY_DATA_PER_NODE_METRICS", "0"))
)

DEFAULT_MIN_HASH_SHUFFLE_AGGREGATOR_WAIT_TIME_IN_S = env_integer(
    "RAY_DATA_MIN_HASH_SHUFFLE_AGGREGATOR_WAIT_TIME_IN_S", 300
)

DEFAULT_HASH_SHUFFLE_AGGREGATOR_HEALTH_WARNING_INTERVAL_S = env_integer(
    "RAY_DATA_HASH_SHUFFLE_AGGREGATOR_HEALTH_WARNING_INTERVAL_S", 30
)


DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD: float = env_float(
    "RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD",
    2.0,
)

DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD: float = env_float(
    "RAY_DATA_DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD",
    0.5,
)

DEFAULT_ACTOR_POOL_MAX_UPSCALING_DELTA: int = env_integer(
    "RAY_DATA_DEFAULT_ACTOR_POOL_MAX_UPSCALING_DELTA",
    1,
)


# Dynamic output queue size backpressure disabled by default.
DEFAULT_ENABLE_DYNAMIC_OUTPUT_QUEUE_SIZE_BACKPRESSURE: bool = env_bool(
    "RAY_DATA_ENABLE_DYNAMIC_OUTPUT_QUEUE_SIZE_BACKPRESSURE", False
)


@DeveloperAPI
@dataclass
class AutoscalingConfig:
    """Configuration for autoscaling of Ray Data.

    Args:
        actor_pool_util_upscaling_threshold: Actor Pool utilization threshold for upscaling.
            Once Actor Pool exceeds this utilization threshold it will start adding new actors.
            Actor Pool utilization is defined as ratio of number of submitted tasks to the
            number of available concurrency-slots to run them in the current set of actors.
            This utilization value could exceed 100%, when the number of submitted tasks
            exceed available concurrency-slots to run them in the current set of actors.
            This is possible when `max_tasks_in_flight_per_actor`
            (defaults to 2 x of `max_concurrency`) > Actor's `max_concurrency`
            and allows to overlap task execution with the fetching of the blocks
            for the next task providing for ability to negotiate a trade-off
            between autoscaling speed and resource efficiency (i.e.,
            making tasks wait instead of immediately triggering execution).
        actor_pool_util_downscaling_threshold: Actor Pool utilization threshold for downscaling.
        actor_pool_max_upscaling_delta: Maximum number of actors to scale up in a single scaling decision.
            This limits how many actors can be added at once to prevent resource contention
            and scheduling pressure. Defaults to 1 for conservative scaling.
    """

    actor_pool_util_upscaling_threshold: float = (
        DEFAULT_ACTOR_POOL_UTIL_UPSCALING_THRESHOLD
    )

    # Actor Pool utilization threshold for downscaling
    actor_pool_util_downscaling_threshold: float = (
        DEFAULT_ACTOR_POOL_UTIL_DOWNSCALING_THRESHOLD
    )

    # Maximum number of actors to scale up in a single scaling decision
    actor_pool_max_upscaling_delta: int = DEFAULT_ACTOR_POOL_MAX_UPSCALING_DELTA


def _execution_options_factory() -> "ExecutionOptions":
    # Lazily import to avoid circular dependencies.
    from ray.data._internal.execution.interfaces import ExecutionOptions

    return ExecutionOptions()


def _deduce_default_shuffle_algorithm() -> ShuffleStrategy:
    if DEFAULT_USE_PUSH_BASED_SHUFFLE:
        logger.warning(
            "RAY_DATA_PUSH_BASED_SHUFFLE is deprecated, please use "
            "RAY_DATA_DEFAULT_SHUFFLE_STRATEGY to set shuffling strategy"
        )

        return ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED
    else:
        vs = [s for s in ShuffleStrategy]  # noqa: C416

        assert DEFAULT_SHUFFLE_STRATEGY in vs, (
            f"RAY_DATA_DEFAULT_SHUFFLE_STRATEGY has to be one of the [{','.join(vs)}] "
            f"(got {DEFAULT_SHUFFLE_STRATEGY})"
        )

        return DEFAULT_SHUFFLE_STRATEGY


def _issue_detectors_config_factory() -> "IssueDetectorsConfiguration":
    # Lazily import to avoid circular dependencies.
    from ray.data._internal.issue_detection.issue_detector_configuration import (
        IssueDetectorsConfiguration,
    )

    return IssueDetectorsConfiguration()


@DeveloperAPI
@dataclass
class DataContext:
    """Global settings for Ray Data.

    Configure this class to enable advanced features and tune performance.

    .. warning::
        Apply changes before creating a :class:`~ray.data.Dataset`. Changes made after
        won't take effect.

    .. note::
        This object is automatically propagated to workers. Access it from the driver
        and remote workers with :meth:`DataContext.get_current()`.

    Examples:
        >>> from ray.data import DataContext
        >>> DataContext.get_current().enable_progress_bars = False

    Args:
        target_max_block_size: The max target block size in bytes for reads and
            transformations. If `None`, this means the block size is infinite.
        target_min_block_size: Ray Data avoids creating blocks smaller than this
            size in bytes on read. This takes precedence over
            ``read_op_min_num_blocks``.
        streaming_read_buffer_size: Buffer size when doing streaming reads from local or
            remote storage.
        enable_pandas_block: Whether pandas block format is enabled.
        actor_prefetcher_enabled: Whether to use actor based block prefetcher.
        iter_get_block_batch_size: Maximum number of block object references to resolve
            in a single ``ray.get()`` call when iterating over datasets.
        autoscaling_config: Autoscaling configuration.
        use_push_based_shuffle: Whether to use push-based shuffle.
        pipeline_push_based_shuffle_reduce_tasks:
        scheduling_strategy: The global scheduling strategy. For tasks with large args,
            ``scheduling_strategy_large_args`` takes precedence.
        scheduling_strategy_large_args: Scheduling strategy for tasks with large args.
        large_args_threshold: Size in bytes after which point task arguments are
            considered large. Choose a value so that the data transfer overhead is
            significant in comparison to task scheduling (i.e., low tens of ms).
        use_polars: Whether to use Polars for tabular dataset sorts, groupbys, and
            aggregations.
        eager_free: Whether to eagerly free memory.
        decoding_size_estimation: Whether to estimate in-memory decoding data size for
            data source.
        min_parallelism: This setting is deprecated. Use ``read_op_min_num_blocks``
            instead.
        read_op_min_num_blocks: Minimum number of read output blocks for a dataset.
        enable_tensor_extension_casting: Whether to automatically cast NumPy ndarray
            columns in Pandas DataFrames to tensor extension columns.
        use_arrow_tensor_v2: Config enabling V2 version of ArrowTensorArray supporting
            tensors > 2Gb in size (off by default)
        enable_fallback_to_arrow_object_ext_type: Enables fallback to serialize column
            values not suppported by Arrow natively (like user-defined custom Python
            classes for ex, etc) using `ArrowPythonObjectType` (simply serializing
            these as bytes)
        enable_auto_log_stats: Whether to automatically log stats after execution. If
            disabled, you can still manually print stats with ``Dataset.stats()``.
        verbose_stats_logs: Whether stats logs should be verbose. This includes fields
            such as `extra_metrics` in the stats output, which are excluded by default.
        trace_allocations: Whether to trace allocations / eager free. This adds
            significant performance overheads and should only be used for debugging.
        execution_options: The
            :class:`~ray.data._internal.execution.interfaces.execution_options.ExecutionOptions`
            to use.
        use_ray_tqdm: Whether to enable distributed tqdm.
        enable_progress_bars: Whether to enable progress bars.
        enable_operator_progress_bars: Whether to enable progress bars for individual
            operators during execution.
        enable_progress_bar_name_truncation: If True, the name of the progress bar
            (often the operator name) will be truncated if it exceeds
            `ProgressBar.MAX_NAME_LENGTH`. Otherwise, the full operator name is shown.
        enable_rich_progress_bars: Whether to use the new rich progress bars instead
            of the tqdm TUI.
        progress_bar_log_interval: The interval in seconds for logging progress bar
            updates in non-interactive terminals.
        enable_get_object_locations_for_metrics: Whether to enable
            ``get_object_locations`` for metrics. This is useful for tracking whether
            the object input of a task is local (cache hit) or not local (cache miss)
            to the node that task is running on.
        write_file_retry_on_errors: A list of substrings of error messages that should
            trigger a retry when writing files. This is useful for handling transient
            errors when writing to remote storage systems.
        warn_on_driver_memory_usage_bytes: If driver memory exceeds this threshold,
            Ray Data warns you. For now, this only applies to shuffle ops because most
            other ops are unlikely to use as much driver memory.
        actor_task_retry_on_errors: The application-level errors that actor task should
            retry. This follows same format as :ref:`retry_exceptions <task-retries>` in
            Ray Core. Default to `False` to not retry on any errors. Set to `True` to
            retry all errors, or set to a list of errors to retry.
        actor_init_retry_on_errors: Whether to retry when actor initialization fails.
            Default to `False` to not retry on any errors. Set to `True` to retry
            all errors.
        actor_init_max_retries: Maximum number of consecutive retries for actor
            initialization failures. The counter resets when an actor successfully
            initializes. Default is 3. Set to -1 for infinite retries.
        op_resource_reservation_enabled: Whether to enable resource reservation for
            operators to prevent resource contention.
        op_resource_reservation_ratio: The ratio of the total resources to reserve for
            each operator.
        max_errored_blocks: Max number of blocks that are allowed to have errors,
            unlimited if negative. This option allows application-level exceptions in
            block processing tasks. These exceptions may be caused by UDFs (e.g., due to
            corrupted data samples) or IO errors. Data in the failed blocks are dropped.
            This option can be useful to prevent a long-running job from failing due to
            a small number of bad blocks.
        log_internal_stack_trace_to_stdout: Whether to include internal Ray Data/Ray
            Core code stack frames when logging to stdout. The full stack trace is
            always written to the Ray Data log file.
        raise_original_map_exception: Whether to raise the original exception
            encountered in map UDF instead of wrapping it in a `UserCodeException`.
        print_on_execution_start: If ``True``, print execution information when
            execution starts.
        s3_try_create_dir: If ``True``, try to create directories on S3 when a write
            call is made with a S3 URI.
        wait_for_min_actors_s: The default time to wait for minimum requested
            actors to start before raising a timeout, in seconds.
        max_tasks_in_flight_per_actor: Max number of tasks that could be submitted
            for execution to individual actor at the same time. Note that only up to
            `max_concurrency` number of these tasks will be executing concurrently
            while remaining ones will be waiting in the Actor's queue. Buffering
            tasks in the queue allows us to overlap pulling of the blocks (which are
            tasks arguments) with the execution of the prior tasks maximizing
            individual Actor's utilization
        retried_io_errors: A list of substrings of error messages that should
            trigger a retry when reading or writing files. This is useful for handling
            transient errors when reading from remote storage systems.
        default_hash_shuffle_parallelism: Default parallelism level for hash-based
            shuffle operations if the number of partitions is unspecifed.
        max_hash_shuffle_aggregators: Maximum number of aggregating actors that can be
            provisioned for hash-shuffle aggregations.
        min_hash_shuffle_aggregator_wait_time_in_s: Minimum time to wait for hash
            shuffle aggregators to become available, in seconds.
        hash_shuffle_aggregator_health_warning_interval_s: Interval for health warning
            checks on hash shuffle aggregators, in seconds.
        max_hash_shuffle_finalization_batch_size: Maximum batch size for concurrent
            hash-shuffle finalization tasks. If `None`, defaults to
            `max_hash_shuffle_aggregators`.
        join_operator_actor_num_cpus_per_partition_override: Override CPU allocation
            per partition for join operator actors.
        hash_shuffle_operator_actor_num_cpus_per_partition_override: Override CPU
            allocation per partition for hash shuffle operator actors.
        hash_aggregate_operator_actor_num_cpus_per_partition_override: Override CPU
            allocation per partition for hash aggregate operator actors.
        use_polars_sort: Whether to use Polars for tabular dataset sorting operations.
        enable_per_node_metrics: Enable per node metrics reporting for Ray Data,
            disabled by default.
        override_object_store_memory_limit_fraction: Override the fraction of object
            store memory limit. If `None`, uses Ray's default.
        memory_usage_poll_interval_s: The interval to poll the USS of map tasks. If `None`,
            map tasks won't record memory stats.
        dataset_logger_id: Optional logger ID for dataset operations. If `None`, uses
            default logging configuration.
        issue_detectors_config: Configuration for issue detection and monitoring during
            dataset operations.
        downstream_capacity_backpressure_ratio: Ratio for downstream capacity
            backpressure control. A higher ratio causes backpressure to kick-in
            later. If `None`, this type of backpressure is disabled.
        downstream_capacity_backpressure_max_queued_bundles: Maximum number of queued
            bundles before applying backpressure. If `None`, no limit is applied.
        enable_dynamic_output_queue_size_backpressure: Whether to cap the concurrency
        of an operator based on it's and downstream's queue size.
        enforce_schemas: Whether to enforce schema consistency across dataset operations.
        pandas_block_ignore_metadata: Whether to ignore pandas metadata when converting
            between Arrow and pandas formats for better type inference.
    """

    # `None` means the block size is infinite.
    target_max_block_size: Optional[int] = DEFAULT_TARGET_MAX_BLOCK_SIZE
    target_min_block_size: int = DEFAULT_TARGET_MIN_BLOCK_SIZE
    streaming_read_buffer_size: int = DEFAULT_STREAMING_READ_BUFFER_SIZE
    enable_pandas_block: bool = DEFAULT_ENABLE_PANDAS_BLOCK
    actor_prefetcher_enabled: bool = DEFAULT_ACTOR_PREFETCHER_ENABLED
    iter_get_block_batch_size: int = DEFAULT_ITER_GET_BLOCK_BATCH_SIZE

    autoscaling_config: AutoscalingConfig = field(default_factory=AutoscalingConfig)

    ################################################################
    # Sort-based shuffling configuration
    ################################################################

    use_push_based_shuffle: bool = DEFAULT_USE_PUSH_BASED_SHUFFLE

    _shuffle_strategy: ShuffleStrategy = _deduce_default_shuffle_algorithm()

    pipeline_push_based_shuffle_reduce_tasks: bool = True

    ################################################################
    # Hash-based shuffling configuration
    ################################################################

    # Default hash-shuffle parallelism level (will be used when not
    # provided explicitly)
    default_hash_shuffle_parallelism: int = DEFAULT_MIN_PARALLELISM

    # Max number of aggregators (actors) that could be provisioned
    # to perform aggregations on partitions produced during hash-shuffling
    #
    # When unset defaults to the smaller of
    #   - Total # of CPUs available in the cluster * 2
    #   - DEFAULT_MAX_HASH_SHUFFLE_AGGREGATORS (128 by default)
    max_hash_shuffle_aggregators: Optional[int] = None

    min_hash_shuffle_aggregator_wait_time_in_s: int = (
        DEFAULT_MIN_HASH_SHUFFLE_AGGREGATOR_WAIT_TIME_IN_S
    )

    hash_shuffle_aggregator_health_warning_interval_s: int = (
        DEFAULT_HASH_SHUFFLE_AGGREGATOR_HEALTH_WARNING_INTERVAL_S
    )

    # Max number of *concurrent* hash-shuffle finalization tasks running
    # at the same time. This config is helpful to control concurrency of
    # finalization tasks to prevent single aggregator running multiple tasks
    # concurrently (for ex, to prevent it failing w/ OOM)
    #
    # When unset defaults to `DataContext.max_hash_shuffle_aggregators`
    max_hash_shuffle_finalization_batch_size: Optional[int] = None

    # (Advanced) Following configuration allows to override `num_cpus` allocation for the
    # Join/Aggregate/Shuffle workers (utilizing hash-shuffle)
    join_operator_actor_num_cpus_override: float = None
    hash_shuffle_operator_actor_num_cpus_override: float = None
    hash_aggregate_operator_actor_num_cpus_override: float = None

    scheduling_strategy: SchedulingStrategyT = DEFAULT_SCHEDULING_STRATEGY
    scheduling_strategy_large_args: SchedulingStrategyT = (
        DEFAULT_SCHEDULING_STRATEGY_LARGE_ARGS
    )
    large_args_threshold: int = DEFAULT_LARGE_ARGS_THRESHOLD
    use_polars: bool = DEFAULT_USE_POLARS
    use_polars_sort: bool = DEFAULT_USE_POLARS_SORT
    eager_free: bool = DEFAULT_EAGER_FREE
    decoding_size_estimation: bool = DEFAULT_DECODING_SIZE_ESTIMATION_ENABLED
    min_parallelism: int = DEFAULT_MIN_PARALLELISM
    read_op_min_num_blocks: int = DEFAULT_READ_OP_MIN_NUM_BLOCKS
    enable_tensor_extension_casting: bool = DEFAULT_ENABLE_TENSOR_EXTENSION_CASTING
    use_arrow_tensor_v2: bool = DEFAULT_USE_ARROW_TENSOR_V2
    enable_fallback_to_arrow_object_ext_type: Optional[bool] = None
    enable_auto_log_stats: bool = DEFAULT_AUTO_LOG_STATS
    verbose_stats_logs: bool = DEFAULT_VERBOSE_STATS_LOG
    trace_allocations: bool = DEFAULT_TRACE_ALLOCATIONS
    execution_options: "ExecutionOptions" = field(
        default_factory=_execution_options_factory
    )
    use_ray_tqdm: bool = DEFAULT_USE_RAY_TQDM
    enable_progress_bars: bool = DEFAULT_ENABLE_PROGRESS_BARS
    # By default, enable the progress bar for operator-level progress.
    # In __post_init__(), we disable operator-level progress
    # bars when running in a Ray job.
    enable_operator_progress_bars: bool = True
    enable_progress_bar_name_truncation: bool = (
        DEFAULT_ENABLE_PROGRESS_BAR_NAME_TRUNCATION
    )
    enable_rich_progress_bars: bool = DEFAULT_ENABLE_RICH_PROGRESS_BARS
    progress_bar_log_interval: int = DEFAULT_PROGRESS_BAR_LOG_INTERVAL
    enable_get_object_locations_for_metrics: bool = (
        DEFAULT_ENABLE_GET_OBJECT_LOCATIONS_FOR_METRICS
    )
    write_file_retry_on_errors: List[str] = DEFAULT_WRITE_FILE_RETRY_ON_ERRORS
    warn_on_driver_memory_usage_bytes: int = DEFAULT_WARN_ON_DRIVER_MEMORY_USAGE_BYTES
    actor_task_retry_on_errors: Union[
        bool, List[BaseException]
    ] = DEFAULT_ACTOR_TASK_RETRY_ON_ERRORS
    actor_init_retry_on_errors: bool = DEFAULT_ACTOR_INIT_RETRY_ON_ERRORS
    actor_init_max_retries: int = DEFAULT_ACTOR_INIT_MAX_RETRIES
    op_resource_reservation_enabled: bool = DEFAULT_ENABLE_OP_RESOURCE_RESERVATION
    op_resource_reservation_ratio: float = DEFAULT_OP_RESOURCE_RESERVATION_RATIO
    max_errored_blocks: int = DEFAULT_MAX_ERRORED_BLOCKS
    log_internal_stack_trace_to_stdout: bool = (
        DEFAULT_LOG_INTERNAL_STACK_TRACE_TO_STDOUT
    )
    raise_original_map_exception: bool = DEFAULT_RAY_DATA_RAISE_ORIGINAL_MAP_EXCEPTION
    print_on_execution_start: bool = True
    s3_try_create_dir: bool = DEFAULT_S3_TRY_CREATE_DIR
    # Timeout threshold (in seconds) for how long it should take for actors in the
    # Actor Pool to start up. Exceeding this threshold will lead to execution being
    # terminated with exception due to inability to secure min required capacity.
    #
    # Setting non-positive value here (ie <= 0) disables this functionality
    # (defaults to -1).
    wait_for_min_actors_s: int = DEFAULT_WAIT_FOR_MIN_ACTORS_S
    max_tasks_in_flight_per_actor: Optional[int] = None
    retried_io_errors: List[str] = field(
        default_factory=lambda: list(DEFAULT_RETRIED_IO_ERRORS)
    )
    enable_per_node_metrics: bool = DEFAULT_ENABLE_PER_NODE_METRICS
    override_object_store_memory_limit_fraction: float = None
    memory_usage_poll_interval_s: Optional[float] = 1
    dataset_logger_id: Optional[str] = None
    # This is a temporary workaround to allow actors to perform cleanup
    # until https://github.com/ray-project/ray/issues/53169 is fixed.
    # This hook is known to have a race condition bug in fault tolerance.
    # I.E., after the hook is triggered and the UDF is deleted, another
    # retry task may still be scheduled to this actor and it will fail.
    _enable_actor_pool_on_exit_hook: bool = False

    issue_detectors_config: "IssueDetectorsConfiguration" = field(
        default_factory=_issue_detectors_config_factory
    )

    downstream_capacity_backpressure_ratio: float = None
    downstream_capacity_backpressure_max_queued_bundles: int = None

    enable_dynamic_output_queue_size_backpressure: bool = (
        DEFAULT_ENABLE_DYNAMIC_OUTPUT_QUEUE_SIZE_BACKPRESSURE
    )

    enforce_schemas: bool = DEFAULT_ENFORCE_SCHEMAS

    pandas_block_ignore_metadata: bool = DEFAULT_PANDAS_BLOCK_IGNORE_METADATA

    def __post_init__(self):
        # The additonal ray remote args that should be added to
        # the task-pool-based data tasks.
        self._task_pool_data_task_remote_args: Dict[str, Any] = {}
        # The extra key-value style configs.
        # These configs are managed by individual components or plugins via
        # `set_config`, `get_config` and `remove_config`.
        # The reason why we use a dict instead of individual fields is to decouple
        # the DataContext from the plugin implementations, as well as to avoid
        # circular dependencies.
        self._kv_configs: Dict[str, Any] = {}

        # Sync hash shuffle aggregator fields to its detector config
        self.issue_detectors_config.hash_shuffle_detector_config.detection_time_interval_s = (
            self.hash_shuffle_aggregator_health_warning_interval_s
        )
        self.issue_detectors_config.hash_shuffle_detector_config.min_wait_time_s = (
            self.min_hash_shuffle_aggregator_wait_time_in_s
        )

        self._max_num_blocks_in_streaming_gen_buffer = (
            DEFAULT_MAX_NUM_BLOCKS_IN_STREAMING_GEN_BUFFER
        )

        is_ray_job = os.environ.get("RAY_JOB_ID") is not None
        if is_ray_job:
            is_driver = ray.get_runtime_context().worker.mode != WORKER_MODE
            if is_driver and log_once(
                "ray_data_disable_operator_progress_bars_in_ray_jobs"
            ):
                logger.info(
                    "Disabling operator-level progress bars by default in Ray Jobs. "
                    "To enable progress bars for all operators, set "
                    "`ray.data.DataContext.get_current()"
                    ".enable_operator_progress_bars = True`."
                )
            # Disable operator-level progress bars by default in Ray jobs.
            # The global progress bar for the overall Dataset execution will
            # still be enabled, unless the user also sets
            # `ray.data.DataContext.get_current().enable_progress_bars = False`.
            self.enable_operator_progress_bars = False
        else:
            # When not running in Ray job, operator-level progress
            # bars are enabled by default.
            self.enable_operator_progress_bars = True

    def __setattr__(self, name: str, value: Any) -> None:
        if (
            name == "write_file_retry_on_errors"
            and value != DEFAULT_WRITE_FILE_RETRY_ON_ERRORS
        ):
            warnings.warn(
                "`write_file_retry_on_errors` is deprecated! Configure "
                "`retried_io_errors` instead.",
                DeprecationWarning,
            )

        elif name == "use_push_based_shuffle":
            warnings.warn(
                "`use_push_based_shuffle` is deprecated! Configure "
                "`shuffle_strategy` instead.",
                DeprecationWarning,
            )

        elif name == "target_shuffle_max_block_size":
            warnings.warn(
                "`target_shuffle_max_block_size` is deprecated! Configure `target_max_block_size` instead."
            )

            self.target_max_block_size = value

        elif name == "use_polars":
            warnings.warn(
                "`use_polars` is deprecated, please configure "
                "`use_polars_sort`  instead.",
                DeprecationWarning,
            )
            self.use_polars_sort = value

        super().__setattr__(name, value)

    @staticmethod
    def get_current() -> "DataContext":
        """Get or create the current DataContext.

        When a Dataset is created, the current DataContext will be sealed.
        Changes to `DataContext.get_current()` will not impact existing Datasets.

        Examples:

            .. testcode::
                import ray

                context = ray.data.DataContext.get_current()

                context.target_max_block_size = 100 * 1024 ** 2
                ds1 = ray.data.range(1)
                context.target_max_block_size = 1 * 1024 ** 2
                ds2 = ray.data.range(1)

                # ds1's target_max_block_size will be 100MB
                ds1.take_all()
                # ds2's target_max_block_size will be 1MB
                ds2.take_all()

        Developer notes: Avoid using `DataContext.get_current()` in data
        internal components, use the DataContext object captured in the
        Dataset and pass it around as arguments.
        """

        global _default_context

        with _context_lock:
            if _default_context is None:
                _default_context = DataContext()

            return _default_context

    @staticmethod
    def _set_current(context: "DataContext") -> None:
        """Set the current context in a remote worker.

        This is used internally by Dataset to propagate the driver context to
        remote workers used for parallelization.
        """
        global _default_context
        if (
            not _default_context
            or _default_context.dataset_logger_id != context.dataset_logger_id
        ):
            update_dataset_logger_for_worker(context.dataset_logger_id)
        _default_context = context

    @property
    def shuffle_strategy(self) -> ShuffleStrategy:
        if self.use_push_based_shuffle:
            logger.warning(
                "`use_push_based_shuffle` is deprecated, please configure "
                "`shuffle_strategy` instead.",
            )

            return ShuffleStrategy.SORT_SHUFFLE_PUSH_BASED

        return self._shuffle_strategy

    @shuffle_strategy.setter
    def shuffle_strategy(self, value: ShuffleStrategy) -> None:
        self._shuffle_strategy = value

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

    def copy(self) -> "DataContext":
        """Create a copy of the current DataContext."""
        return copy.deepcopy(self)

    def set_dataset_logger_id(self, dataset_id: str) -> None:
        """Set the current dataset logger id.

        This is used internally to propagate the current dataset logger id to remote
        workers.
        """
        self.dataset_logger_id = dataset_id


# Backwards compatibility alias.
DatasetContext = DataContext
