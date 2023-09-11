import importlib
import logging
import os
import pathlib
import sys
import threading
import urllib.parse
from collections import deque
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

import numpy as np

import ray
from ray._private.utils import _get_pyarrow_version
from ray.data._internal.arrow_ops.transform_pyarrow import unify_schemas
from ray.data.context import DataContext

if TYPE_CHECKING:
    import pandas
    import pyarrow

    from ray.data._internal.compute import ComputeStrategy
    from ray.data._internal.sort import SortKey
    from ray.data.block import Block, BlockMetadata, UserDefinedFunction
    from ray.data.datasource import Reader
    from ray.util.placement_group import PlacementGroup

logger = logging.getLogger(__name__)

# NOTE: Make sure that these lower and upper bounds stay in sync with version
# constraints given in python/setup.py.
# Inclusive minimum pyarrow version.
MIN_PYARROW_VERSION = "6.0.1"
RAY_DISABLE_PYARROW_VERSION_CHECK = "RAY_DISABLE_PYARROW_VERSION_CHECK"
_VERSION_VALIDATED = False
_LOCAL_SCHEME = "local"
_EXAMPLE_SCHEME = "example"


LazyModule = Union[None, bool, ModuleType]
_pyarrow_dataset: LazyModule = None


def _lazy_import_pyarrow_dataset() -> LazyModule:
    global _pyarrow_dataset
    if _pyarrow_dataset is None:
        try:
            from pyarrow import dataset as _pyarrow_dataset
        except ModuleNotFoundError:
            # If module is not found, set _pyarrow to False so we won't
            # keep trying to import it on every _lazy_import_pyarrow() call.
            _pyarrow_dataset = False
    return _pyarrow_dataset


def _check_pyarrow_version():
    """Check that pyarrow's version is within the supported bounds."""
    global _VERSION_VALIDATED

    if not _VERSION_VALIDATED:
        if os.environ.get(RAY_DISABLE_PYARROW_VERSION_CHECK, "0") == "1":
            _VERSION_VALIDATED = True
            return

        version = _get_pyarrow_version()
        if version is not None:
            from pkg_resources._vendor.packaging.version import parse as parse_version

            if parse_version(version) < parse_version(MIN_PYARROW_VERSION):
                raise ImportError(
                    f"Dataset requires pyarrow >= {MIN_PYARROW_VERSION}, but "
                    f"{version} is installed. Reinstall with "
                    f'`pip install -U "pyarrow"`. '
                    "If you want to disable this pyarrow version check, set the "
                    f"environment variable {RAY_DISABLE_PYARROW_VERSION_CHECK}=1."
                )
        else:
            logger.warning(
                "You are using the 'pyarrow' module, but the exact version is unknown "
                "(possibly carried as an internal component by another module). Please "
                f"make sure you are using pyarrow >= {MIN_PYARROW_VERSION} to ensure "
                "compatibility with Ray Dataset. "
                "If you want to disable this pyarrow version check, set the "
                f"environment variable {RAY_DISABLE_PYARROW_VERSION_CHECK}=1."
            )
        _VERSION_VALIDATED = True


def _autodetect_parallelism(
    parallelism: int,
    cur_pg: Optional["PlacementGroup"],
    ctx: DataContext,
    reader: Optional["Reader"] = None,
    avail_cpus: Optional[int] = None,
) -> (int, int, Optional[int]):
    """Returns parallelism to use and the min safe parallelism to avoid OOMs.

    This detects parallelism using the following heuristics, applied in order:

     1) We start with the default parallelism of 200.
     2) Min block size. If the parallelism would make blocks smaller than this
        threshold, the parallelism is reduced to avoid the overhead of tiny blocks.
     3) Max block size. If the parallelism would make blocks larger than this
        threshold, the parallelism is increased to avoid OOMs during processing.
     4) Available CPUs. If the parallelism cannot make use of all the available
        CPUs in the cluster, the parallelism is increased until it can.

    Args:
        parallelism: The user-requested parallelism, or -1 for auto-detection.
        cur_pg: The current placement group, to be used for avail cpu calculation.
        ctx: The current Dataset context to use for configs.
        reader: The datasource reader, to be used for data size estimation.
        avail_cpus: Override avail cpus detection (for testing only).

    Returns:
        Tuple of detected parallelism (only if -1 was specified), the min safe
        parallelism (which can be used to generate warnings about large blocks),
        and the estimated inmemory size of the dataset.
    """
    min_safe_parallelism = 1
    max_reasonable_parallelism = sys.maxsize
    if reader:
        mem_size = reader.estimate_inmemory_data_size()
        if mem_size is not None and not np.isnan(mem_size):
            min_safe_parallelism = max(1, int(mem_size / ctx.target_max_block_size))
            max_reasonable_parallelism = max(
                1, int(mem_size / ctx.target_min_block_size)
            )
    else:
        mem_size = None
    if parallelism < 0:
        if parallelism != -1:
            raise ValueError("`parallelism` must either be -1 or a positive integer.")
        # Start with 2x the number of cores as a baseline, with a min floor.
        avail_cpus = avail_cpus or _estimate_avail_cpus(cur_pg)
        parallelism = max(
            min(ctx.min_parallelism, max_reasonable_parallelism),
            min_safe_parallelism,
            avail_cpus * 2,
        )
        logger.debug(
            f"Autodetected parallelism={parallelism} based on "
            f"estimated_available_cpus={avail_cpus} and "
            f"estimated_data_size={mem_size}."
        )
    return parallelism, min_safe_parallelism, mem_size


def _estimate_avail_cpus(cur_pg: Optional["PlacementGroup"]) -> int:
    """Estimates the available CPU parallelism for this Dataset in the cluster.

    If we aren't in a placement group, this is trivially the number of CPUs in the
    cluster. Otherwise, we try to calculate how large the placement group is relative
    to the size of the cluster.

    Args:
        cur_pg: The current placement group, if any.
    """
    cluster_cpus = int(ray.cluster_resources().get("CPU", 1))
    cluster_gpus = int(ray.cluster_resources().get("GPU", 0))

    # If we're in a placement group, we shouldn't assume the entire cluster's
    # resources are available for us to use. Estimate an upper bound on what's
    # reasonable to assume is available for datasets to use.
    if cur_pg:
        pg_cpus = 0
        for bundle in cur_pg.bundle_specs:
            # Calculate the proportion of the cluster this placement group "takes up".
            # Then scale our cluster_cpus proportionally to avoid over-parallelizing
            # if there are many parallel Tune trials using the cluster.
            cpu_fraction = bundle.get("CPU", 0) / max(1, cluster_cpus)
            gpu_fraction = bundle.get("GPU", 0) / max(1, cluster_gpus)
            max_fraction = max(cpu_fraction, gpu_fraction)
            # Over-parallelize by up to a factor of 2, but no more than that. It's
            # preferrable to over-estimate than under-estimate.
            pg_cpus += 2 * int(max_fraction * cluster_cpus)

        return min(cluster_cpus, pg_cpus)

    return cluster_cpus


def _estimate_available_parallelism() -> int:
    """Estimates the available CPU parallelism for this Dataset in the cluster.
    If we are currently in a placement group, take that into account."""
    cur_pg = ray.util.get_current_placement_group()
    return _estimate_avail_cpus(cur_pg)


def _check_import(obj, *, module: str, package: str) -> None:
    """Check if a required dependency is installed.

    If `module` can't be imported, this function raises an `ImportError` instructing
    the user to install `package` from PyPI.

    Args:
        obj: The object that has a dependency.
        module: The name of the module to import.
        package: The name of the package on PyPI.
    """
    try:
        importlib.import_module(module)
    except ImportError:
        raise ImportError(
            f"`{obj.__class__.__name__}` depends on '{package}', but '{package}' "
            f"couldn't be imported. You can install '{package}' by running `pip "
            f"install {package}`."
        )


def _resolve_custom_scheme(path: str) -> str:
    """Returns the resolved path if the given path follows a Ray-specific custom
    scheme. Othewise, returns the path unchanged.

    The supported custom schemes are: "local", "example".
    """
    parsed_uri = urllib.parse.urlparse(path)
    if parsed_uri.scheme == _LOCAL_SCHEME:
        path = parsed_uri.netloc + parsed_uri.path
    elif parsed_uri.scheme == _EXAMPLE_SCHEME:
        example_data_path = pathlib.Path(__file__).parent.parent / "examples" / "data"
        path = example_data_path / (parsed_uri.netloc + parsed_uri.path)
        path = str(path.resolve())
    return path


def _is_local_scheme(paths: Union[str, List[str]]) -> bool:
    """Returns True if the given paths are in local scheme.
    Note: The paths must be in same scheme, i.e. it's invalid and
    will raise error if paths are mixed with different schemes.
    """
    if isinstance(paths, str):
        paths = [paths]
    if isinstance(paths, pathlib.Path):
        paths = [str(paths)]
    elif not isinstance(paths, list) or any(not isinstance(p, str) for p in paths):
        raise ValueError("paths must be a path string or a list of path strings.")
    elif len(paths) == 0:
        raise ValueError("Must provide at least one path.")
    num = sum(urllib.parse.urlparse(path).scheme == _LOCAL_SCHEME for path in paths)
    if num > 0 and num < len(paths):
        raise ValueError(
            "The paths must all be local-scheme or not local-scheme, "
            f"but found mixed {paths}"
        )
    return num == len(paths)


def _truncated_repr(obj: Any) -> str:
    """Utility to return a truncated object representation for error messages."""
    msg = str(obj)
    if len(msg) > 200:
        msg = msg[:200] + "..."
    return msg


def _insert_doc_at_pattern(
    obj,
    *,
    message: str,
    pattern: str,
    insert_after: bool = True,
    directive: Optional[str] = None,
    skip_matches: int = 0,
) -> str:
    if "\n" in message:
        raise ValueError(
            "message shouldn't contain any newlines, since this function will insert "
            f"its own linebreaks when text wrapping: {message}"
        )

    doc = obj.__doc__.strip()
    if not doc:
        doc = ""

    if pattern == "" and insert_after:
        # Empty pattern + insert_after means that we want to append the message to the
        # end of the docstring.
        head = doc
        tail = ""
    else:
        tail = doc
        i = tail.find(pattern)
        skip_matches_left = skip_matches
        while i != -1:
            if insert_after:
                # Set offset to the first character after the pattern.
                offset = i + len(pattern)
            else:
                # Set offset to the first character in the matched line.
                offset = tail[:i].rfind("\n") + 1
            head = tail[:offset]
            tail = tail[offset:]
            skip_matches_left -= 1
            if skip_matches_left <= 0:
                break
            elif not insert_after:
                # Move past the found pattern, since we're skipping it.
                tail = tail[i - offset + len(pattern) :]
            i = tail.find(pattern)
        else:
            raise ValueError(
                f"Pattern {pattern} not found after {skip_matches} skips in docstring "
                f"{doc}"
            )
    # Get indentation of the to-be-inserted text.
    after_lines = list(filter(bool, tail.splitlines()))
    if len(after_lines) > 0:
        lines = after_lines
    else:
        lines = list(filter(bool, reversed(head.splitlines())))
    # Should always have at least one non-empty line in the docstring.
    assert len(lines) > 0
    indent = " " * (len(lines[0]) - len(lines[0].lstrip()))
    # Handle directive.
    message = message.strip("\n")
    if directive is not None:
        base = f"{indent}.. {directive}::\n"
        message = message.replace("\n", "\n" + indent + " " * 4)
        message = base + indent + " " * 4 + message
    else:
        message = indent + message.replace("\n", "\n" + indent)
    # Add two blank lines before/after message, if necessary.
    if insert_after ^ (pattern == "\n\n"):
        # Only two blank lines before message if:
        # 1. Inserting message after pattern and pattern is not two blank lines.
        # 2. Inserting message before pattern and pattern is two blank lines.
        message = "\n\n" + message
    if (not insert_after) ^ (pattern == "\n\n"):
        # Only two blank lines after message if:
        # 1. Inserting message before pattern and pattern is not two blank lines.
        # 2. Inserting message after pattern and pattern is two blank lines.
        message = message + "\n\n"

    # Insert message before/after pattern.
    parts = [head, message, tail]
    # Build new docstring.
    obj.__doc__ = "".join(parts)


def _consumption_api(
    if_more_than_read: bool = False,
    datasource_metadata: Optional[str] = None,
    extra_condition: Optional[str] = None,
    delegate: Optional[str] = None,
    pattern="Examples:",
    insert_after=False,
):
    """Annotate the function with an indication that it's a consumption API, and that it
    will trigger Dataset execution.
    """
    base = (
        " will trigger execution of the lazy transformations performed on "
        "this dataset."
    )
    if delegate:
        message = delegate + base
    elif not if_more_than_read:
        message = "This operation" + base
    else:
        condition = "If this dataset consists of more than a read, "
        if datasource_metadata is not None:
            condition += (
                f"or if the {datasource_metadata} can't be determined from the "
                "metadata provided by the datasource, "
            )
        if extra_condition is not None:
            condition += extra_condition + ", "
        message = condition + "then this operation" + base

    def wrap(obj):
        _insert_doc_at_pattern(
            obj,
            message=message,
            pattern=pattern,
            insert_after=insert_after,
            directive="note",
        )
        return obj

    return wrap


def ConsumptionAPI(*args, **kwargs):
    """Annotate the function with an indication that it's a consumption API, and that it
    will trigger Dataset execution.
    """
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        return _consumption_api()(args[0])
    return _consumption_api(*args, **kwargs)


def _split_list(arr: List[Any], num_splits: int) -> List[List[Any]]:
    """Split the list into `num_splits` lists.

    The splits will be even if the `num_splits` divides the length of list, otherwise
    the remainder (suppose it's R) will be allocated to the first R splits (one for
    each).
    This is the same as numpy.array_split(). The reason we make this a separate
    implementation is to allow the heterogeneity in the elements in the list.
    """
    assert num_splits > 0
    q, r = divmod(len(arr), num_splits)
    splits = [
        arr[i * q + min(i, r) : (i + 1) * q + min(i + 1, r)] for i in range(num_splits)
    ]
    return splits


def validate_compute(
    fn: "UserDefinedFunction",
    compute: Optional[Union[str, "ComputeStrategy"]],
    fn_constructor_args: Optional[Iterable[Any]] = None,
) -> None:
    # Lazily import these objects to avoid circular imports.
    from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
    from ray.data.block import CallableClass

    if isinstance(fn, CallableClass) and (
        compute is None or compute == "tasks" or isinstance(compute, TaskPoolStrategy)
    ):
        raise ValueError(
            "``compute`` must be specified when using a CallableClass, and must "
            f"specify the actor compute strategy, but got: {compute}. "
            "For example, use ``compute=ray.data.ActorPoolStrategy(size=n)``."
        )

    if fn_constructor_args is not None:
        if compute is None or (
            compute != "actors" and not isinstance(compute, ActorPoolStrategy)
        ):
            raise ValueError(
                "fn_constructor_args can only be specified if using the actor "
                f"pool compute strategy, but got: {compute}"
            )
        if not isinstance(fn, CallableClass):
            raise ValueError(
                "fn_constructor_args can only be specified if providing a "
                f"CallableClass instance for fn, but got: {fn}"
            )


def capfirst(s: str):
    """Capitalize the first letter of a string

    Args:
        s: String to capitalize

    Returns:
       Capitalized string
    """
    return s[0].upper() + s[1:]


def capitalize(s: str):
    """Capitalize a string, removing '_' and keeping camelcase.

    Args:
        s: String to capitalize

    Returns:
        Capitalized string with no underscores.
    """
    return "".join(capfirst(x) for x in s.split("_"))


def pandas_df_to_arrow_block(df: "pandas.DataFrame") -> "Block":
    from ray.data.block import BlockAccessor, BlockExecStats

    stats = BlockExecStats.builder()
    import pyarrow as pa

    block = pa.table(df)
    return (
        block,
        BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=stats.build()
        ),
    )


def ndarray_to_block(ndarray: np.ndarray, ctx: DataContext) -> "Block":
    from ray.data.block import BlockAccessor, BlockExecStats

    DataContext._set_current(ctx)

    stats = BlockExecStats.builder()
    block = BlockAccessor.batch_to_block({"data": ndarray})
    metadata = BlockAccessor.for_block(block).get_metadata(
        input_files=None, exec_stats=stats.build()
    )
    return block, metadata


def get_table_block_metadata(
    table: Union["pyarrow.Table", "pandas.DataFrame"]
) -> "BlockMetadata":
    from ray.data.block import BlockAccessor, BlockExecStats

    stats = BlockExecStats.builder()
    return BlockAccessor.for_block(table).get_metadata(
        input_files=None, exec_stats=stats.build()
    )


def unify_block_metadata_schema(
    metadata: List["BlockMetadata"],
) -> Optional[Union[type, "pyarrow.lib.Schema"]]:
    """For the input list of BlockMetadata, return a unified schema of the
    corresponding blocks. If the metadata have no valid schema, returns None.
    """
    # Some blocks could be empty, in which case we cannot get their schema.
    # TODO(ekl) validate schema is the same across different blocks.

    # First check if there are blocks with computed schemas, then unify
    # valid schemas from all such blocks.
    schemas_to_unify = []
    for m in metadata:
        if m.schema is not None and (m.num_rows is None or m.num_rows > 0):
            schemas_to_unify.append(m.schema)
    if schemas_to_unify:
        # Check valid pyarrow installation before attempting schema unification
        try:
            import pyarrow as pa
        except ImportError:
            pa = None
        # If the result contains PyArrow schemas, unify them
        if pa is not None and any(isinstance(s, pa.Schema) for s in schemas_to_unify):
            return unify_schemas(schemas_to_unify)
        # Otherwise, if the resulting schemas are simple types (e.g. int),
        # return the first schema.
        return schemas_to_unify[0]
    return None


def find_partition_index(
    table: Union["pyarrow.Table", "pandas.DataFrame"],
    desired: List[Any],
    sort_key: "SortKey",
) -> int:
    columns = sort_key.get_columns()
    descending = sort_key.get_descending()

    left, right = 0, len(table)
    for i in range(len(desired)):
        if left == right:
            return right
        col_name = columns[i]
        col_vals = table[col_name].to_numpy()[left:right]
        desired_val = desired[i]

        prevleft = left
        if descending is True:
            left = prevleft + (
                len(col_vals)
                - np.searchsorted(
                    col_vals,
                    desired_val,
                    side="right",
                    sorter=np.arange(len(col_vals) - 1, -1, -1),
                )
            )
            right = prevleft + (
                len(col_vals)
                - np.searchsorted(
                    col_vals,
                    desired_val,
                    side="left",
                    sorter=np.arange(len(col_vals) - 1, -1, -1),
                )
            )
        else:
            left = prevleft + np.searchsorted(col_vals, desired_val, side="left")
            right = prevleft + np.searchsorted(col_vals, desired_val, side="right")
    return right if descending is True else left


def find_partitions(table, boundaries, sort_key):
    partitions = []

    # For each boundary value, count the number of items that are less
    # than it. Since the block is sorted, these counts partition the items
    # such that boundaries[i] <= x < boundaries[i + 1] for each x in
    # partition[i]. If `descending` is true, `boundaries` would also be
    # in descending order and we only need to count the number of items
    # *greater than* the boundary value instead.
    bounds = [
        find_partition_index(table, boundary, sort_key) for boundary in boundaries
    ]

    last_idx = 0
    for idx in bounds:
        partitions.append(table[last_idx:idx])
        last_idx = idx
    partitions.append(table[last_idx:])
    return partitions


def get_attribute_from_class_name(class_name: str) -> Any:
    """Get Python attribute from the provided class name.

    The caller needs to make sure the provided class name includes
    full module name, and can be imported successfully.
    """
    from importlib import import_module

    paths = class_name.split(".")
    if len(paths) < 2:
        raise ValueError(f"Cannot create object from {class_name}.")

    module_name = ".".join(paths[:-1])
    attribute_name = paths[-1]
    return getattr(import_module(module_name), attribute_name)


class Queue:
    """A thread-safe queue implementation for multiple producers and consumers.

    Provide `release()` to exit producer threads cooperatively for resource release.
    """

    def __init__(self, queue_size: int):
        # The queue shared across multiple producer threads.
        self._queue = deque()
        # The boolean varilable to indicate whether producer threads should exit.
        self._threads_exit = False
        # The semaphore for producer threads to put item into queue.
        self._producer_semaphore = threading.Semaphore(queue_size)
        # The semaphore for consumer threads to get item from queue.
        self._consumer_semaphore = threading.Semaphore(0)
        # The mutex lock to guard access of `self._queue` and `self._threads_exit`.
        self._mutex = threading.Lock()

    def put(self, item: Any) -> bool:
        """Put an item into the queue.

        Block if necessary until a free slot is available in queue.
        This method is called by producer threads.

        Returns:
            True if the caller thread should exit immediately.
        """
        self._producer_semaphore.acquire()
        with self._mutex:
            if self._threads_exit:
                return True
            else:
                self._queue.append(item)
        self._consumer_semaphore.release()
        return False

    def get(self) -> Any:
        """Remove and return an item from the queue.

        Block if necessary until an item is available in queue.
        This method is called by consumer threads.
        """
        self._consumer_semaphore.acquire()
        with self._mutex:
            next_item = self._queue.popleft()
        self._producer_semaphore.release()
        return next_item

    def release(self, num_threads: int):
        """Release `num_threads` of producers so they would exit cooperatively."""
        with self._mutex:
            self._threads_exit = True
        for _ in range(num_threads):
            # NOTE: After Python 3.9+, Semaphore.release(n) can be used to
            # release all threads at once.
            self._producer_semaphore.release()

    def qsize(self):
        """Return the size of the queue."""
        with self._mutex:
            return len(self._queue)


T = TypeVar("T")
U = TypeVar("U")


def make_async_gen(
    base_iterator: Iterator[T],
    fn: Callable[[Iterator[T]], Iterator[U]],
    num_workers: int = 1,
) -> Iterator[U]:
    """Returns a new iterator with elements fetched from the base_iterator
    in an async fashion using a threadpool.

    Each thread in the threadpool will fetch data from the base_iterator in a
    thread-safe fashion, and apply the provided `fn` computation concurrently.

    Args:
        base_iterator: The iterator to asynchronously fetch from.
        fn: The function to run on the input iterator.
        num_workers: The number of threads to use in the threadpool. Defaults to 1.

    Returns:
        An iterator with the same elements as outputted from `fn`.
    """

    if num_workers < 1:
        raise ValueError("Size of threadpool must be at least 1.")

    # Use a lock to fetch from the base_iterator in a thread-safe fashion.
    def convert_to_threadsafe_iterator(base_iterator: Iterator[T]) -> Iterator[T]:
        class ThreadSafeIterator:
            def __init__(self, it):
                self.lock = threading.Lock()
                self.it = it

            def __next__(self):
                with self.lock:
                    return next(self.it)

            def __iter__(self):
                return self

        return ThreadSafeIterator(base_iterator)

    thread_safe_generator = convert_to_threadsafe_iterator(base_iterator)

    class Sentinel:
        def __init__(self, thread_index: int):
            self.thread_index = thread_index

    output_queue = Queue(1)

    # Because pulling from the base iterator cannot happen concurrently,
    # we must execute the expensive computation in a separate step which
    # can be parallelized via a threadpool.
    def execute_computation(thread_index: int):
        try:
            for item in fn(thread_safe_generator):
                if output_queue.put(item):
                    # Return early when it's instructed to do so.
                    return
            output_queue.put(Sentinel(thread_index))
        except Exception as e:
            output_queue.put(e)

    # Use separate threads to produce output batches.
    threads = [
        threading.Thread(target=execute_computation, args=(i,), daemon=True)
        for i in range(num_workers)
    ]

    for thread in threads:
        thread.start()

    # Use main thread to consume output batches.
    num_threads_finished = 0
    try:
        while True:
            next_item = output_queue.get()
            if isinstance(next_item, Exception):
                raise next_item
            if isinstance(next_item, Sentinel):
                logger.debug(f"Thread {next_item.thread_index} finished.")
                num_threads_finished += 1
            else:
                yield next_item
            if num_threads_finished >= num_workers:
                break
    finally:
        # Cooperatively exit all producer threads.
        # This is to avoid these daemon threads hanging there with holding batches in
        # memory, which can cause GRAM OOM easily. This can happen when caller breaks
        # in the middle of iteration.
        num_threads_alive = num_workers - num_threads_finished
        if num_threads_alive > 0:
            output_queue.release(num_threads_alive)
