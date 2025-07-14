import copy
import os
import posixpath
import time
from collections import defaultdict

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
import ray.util.state
from ray._common.test_utils import wait_for_condition
from ray._private.arrow_utils import get_pyarrow_version
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.tensor_extensions.arrow import ArrowTensorArray
from ray.data import Schema
from ray.data.block import BlockExecStats, BlockMetadata
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.mock_server import *  # noqa

# Trigger pytest hook to automatically zip test cluster logs to archive dir on failure
from ray.tests.conftest import *  # noqa
from ray.tests.conftest import _ray_start
from ray.util.debug import reset_log_once


@pytest.fixture(scope="module")
def data_context_override(request):
    overrides = getattr(request, "param", {})

    ctx = DataContext.get_current()
    copy = ctx.copy()

    for k, v in overrides.items():
        assert hasattr(ctx, k), f"Key '{k}' not found in DataContext"

        setattr(ctx, k, v)

    yield ctx

    DataContext._set_current(copy)


@pytest.fixture(scope="module")
def ray_start_2_cpus_shared(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=2, **param) as res:
        yield res


@pytest.fixture(scope="module")
def ray_start_10_cpus_shared(request):
    param = getattr(request, "param", {})
    with _ray_start(num_cpus=10, **param) as res:
        yield res


@pytest.fixture(scope="function")
def aws_credentials():
    import os

    # Credentials dict that can be passed as kwargs to pa.fs.S3FileSystem
    credentials = dict(
        access_key="testing", secret_key="testing", session_token="testing"
    )

    old_env = os.environ
    os.environ["AWS_ACCESS_KEY_ID"] = credentials["access_key"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials["secret_key"]
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = credentials["session_token"]

    yield credentials
    os.environ = old_env


@pytest.fixture(scope="function")
def data_dir():
    yield "test_data"


@pytest.fixture(scope="function")
def data_dir_with_space():
    yield "test data"


@pytest.fixture(scope="function")
def data_dir_with_special_chars():
    yield "test data#fragment?query=test/"


@pytest.fixture(scope="function")
def s3_path(tmp_path, data_dir):
    yield "s3://" + posixpath.join(tmp_path, data_dir).strip("/")


@pytest.fixture(scope="function")
def s3_path_with_space(tmp_path, data_dir_with_space):
    yield "s3://" + posixpath.join(tmp_path, data_dir_with_space).strip("/")


@pytest.fixture(scope="function")
def s3_path_with_special_chars(tmp_path, data_dir_with_special_chars):
    yield "s3://" + posixpath.join(tmp_path, data_dir_with_special_chars).lstrip("/")


@pytest.fixture(scope="function")
def s3_path_with_anonymous_crendential(tmp_path, data_dir):
    yield "s3://" + "anonymous@" + posixpath.join(tmp_path, data_dir).lstrip("/")


@pytest.fixture(scope="function")
def s3_fs(aws_credentials, s3_server, s3_path):
    yield from _s3_fs(aws_credentials, s3_server, s3_path)


@pytest.fixture(scope="function")
def s3_fs_with_space(aws_credentials, s3_server, s3_path_with_space):
    yield from _s3_fs(aws_credentials, s3_server, s3_path_with_space)


@pytest.fixture(scope="function")
def s3_fs_with_special_chars(aws_credentials, s3_server, s3_path_with_special_chars):
    yield from _s3_fs(aws_credentials, s3_server, s3_path_with_special_chars)


@pytest.fixture(scope="function")
def s3_fs_with_anonymous_crendential(
    aws_credentials, s3_server, s3_path_with_anonymous_crendential
):
    yield from _s3_fs(aws_credentials, s3_server, s3_path_with_anonymous_crendential)


def _s3_fs(aws_credentials, s3_server, s3_path):
    import urllib.parse

    from packaging.version import parse as parse_version

    kwargs = aws_credentials.copy()

    if get_pyarrow_version() >= parse_version("9.0.0"):
        kwargs["allow_bucket_creation"] = True
        kwargs["allow_bucket_deletion"] = True

    fs = pa.fs.S3FileSystem(
        region="us-west-2",
        endpoint_override=s3_server,
        **kwargs,
    )
    if s3_path.startswith("s3://"):
        if "@" in s3_path:
            s3_path = s3_path.split("@")[-1]
        else:
            s3_path = s3_path[len("s3://") :]
    s3_path = urllib.parse.quote(s3_path)
    fs.create_dir(s3_path)
    yield fs


@pytest.fixture(scope="function")
def local_path(tmp_path, data_dir):
    path = os.path.join(tmp_path, data_dir)
    os.mkdir(path)
    yield path


@pytest.fixture(scope="function")
def local_fs():
    yield pa.fs.LocalFileSystem()


@pytest.fixture(scope="function")
def base_partitioned_df():
    yield pd.DataFrame(
        {"one": [1, 1, 1, 3, 3, 3], "two": ["a", "b", "c", "e", "f", "g"]}
    )


@pytest.fixture(scope="function")
def write_partitioned_df():
    def _write_partitioned_df(
        df,
        partition_keys,
        partition_path_encoder,
        file_writer_fn,
        file_name_suffix="_1",
    ):
        import urllib.parse

        df_partitions = [df for _, df in df.groupby(partition_keys, as_index=False)]
        paths = []
        for df_partition in df_partitions:
            partition_values = []
            for key in partition_keys:
                partition_values.append(str(df_partition[key].iloc[0]))
            path = partition_path_encoder(partition_values)
            partition_path_encoder.scheme.resolved_filesystem.create_dir(path)
            base_dir = partition_path_encoder.scheme.base_dir
            parsed_base_dir = urllib.parse.urlparse(base_dir)
            file_name = f"test_{file_name_suffix}.tmp"
            if parsed_base_dir.scheme:
                # replace the protocol removed by the partition path generator
                path = posixpath.join(f"{parsed_base_dir.scheme}://{path}", file_name)
            else:
                path = os.path.join(path, file_name)
            file_writer_fn(df_partition, path)
            paths.append(path)
        return paths

    yield _write_partitioned_df


@pytest.fixture(scope="function")
def write_base_partitioned_df(base_partitioned_df, write_partitioned_df):
    def _write_base_partitioned_df(
        partition_keys,
        partition_path_encoder,
        file_writer_fn,
    ):
        write_partitioned_df(
            base_partitioned_df,
            partition_keys,
            partition_path_encoder,
            file_writer_fn,
        )

    yield _write_base_partitioned_df


@pytest.fixture(scope="function")
def assert_base_partitioned_ds():
    def _assert_base_partitioned_ds(
        ds,
        count=6,
        num_input_files=2,
        num_rows=6,
        schema=Schema(pa.schema([("one", pa.int64()), ("two", pa.string())])),
        sorted_values=None,
        ds_take_transform_fn=None,
        sorted_values_transform_fn=None,
    ):
        if ds_take_transform_fn is None:
            ds_take_transform_fn = lambda taken: [  # noqa: E731
                [s["one"], s["two"]] for s in taken
            ]

        if sorted_values_transform_fn is None:
            sorted_values_transform_fn = (  # noqa: E731
                lambda sorted_values: sorted_values
            )

        if sorted_values is None:
            sorted_values = [[1, "a"], [1, "b"], [1, "c"], [3, "e"], [3, "f"], [3, "g"]]
        # Test metadata ops.
        assert not ds._plan.has_started_execution
        assert ds.count() == count, f"{ds.count()} != {count}"
        assert ds.size_bytes() > 0, f"{ds.size_bytes()} <= 0"
        assert ds.schema() == schema
        actual_input_files = ds.input_files()
        assert len(actual_input_files) == num_input_files, actual_input_files

        # Force a data read.
        values = ds_take_transform_fn(ds.take_all())
        actual_sorted_values = sorted_values_transform_fn(sorted(values))
        assert (
            actual_sorted_values == sorted_values
        ), f"{actual_sorted_values} != {sorted_values}"

    yield _assert_base_partitioned_ds


@pytest.fixture
def restore_data_context(request):
    """Restore any DataContext changes after the test runs"""
    ctx = ray.data.context.DataContext.get_current()
    original = copy.deepcopy(ctx)
    yield ctx
    ray.data.context.DataContext._set_current(original)


@pytest.fixture
def disable_fallback_to_object_extension(request, restore_data_context):
    """Disables fallback to ArrowPythonObjectType"""
    ray.data.context.DataContext.get_current().enable_fallback_to_arrow_object_ext_type = (
        False
    )


@pytest.fixture(params=[s for s in ShuffleStrategy])  # noqa: C416
def configure_shuffle_method(request):
    shuffle_strategy = request.param

    ctx = ray.data.context.DataContext.get_current()

    original_shuffle_strategy = ctx.shuffle_strategy
    original_default_hash_shuffle_parallelism = ctx.default_hash_shuffle_parallelism

    ctx.shuffle_strategy = shuffle_strategy

    # NOTE: We override default parallelism for hash-based shuffling to
    #       avoid excessive partitioning of the data (to achieve desired
    #       parallelism
    if shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
        ctx.default_hash_shuffle_parallelism = 8

    yield request.param

    ctx.shuffle_strategy = original_shuffle_strategy
    ctx.default_hash_shuffle_parallelism = original_default_hash_shuffle_parallelism


@pytest.fixture(params=[True, False])
def use_polars_sort(request):
    use_polars_sort = request.param

    ctx = ray.data.context.DataContext.get_current()

    original_use_polars = ctx.use_polars_sort

    ctx.use_polars_sort = use_polars_sort

    yield request.param

    ctx.use_polars_sort = original_use_polars


@pytest.fixture(params=[True, False])
def enable_automatic_tensor_extension_cast(request):
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.enable_tensor_extension_casting
    ctx.enable_tensor_extension_casting = request.param
    yield request.param
    ctx.enable_tensor_extension_casting = original


@pytest.fixture(params=[True, False])
def enable_auto_log_stats(request):
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.enable_auto_log_stats
    ctx.enable_auto_log_stats = request.param
    yield request.param
    ctx.enable_auto_log_stats = original


@pytest.fixture(autouse=True)
def reset_log_once_fixture():
    reset_log_once()
    yield


@pytest.fixture(params=[1024])
def target_max_block_size(request):
    ctx = ray.data.context.DataContext.get_current()
    original = ctx.target_max_block_size
    ctx.target_max_block_size = request.param
    yield request.param
    ctx.target_max_block_size = original


# ===== Pandas dataset formats =====
@pytest.fixture(scope="function")
def ds_pandas_single_column_format(ray_start_regular_shared):
    in_df = pd.DataFrame({"column_1": [1, 2, 3, 4]})
    yield ray.data.from_pandas(in_df)


@pytest.fixture(scope="function")
def ds_pandas_multi_column_format(ray_start_regular_shared):
    in_df = pd.DataFrame({"column_1": [1, 2, 3, 4], "column_2": [1, -1, 1, -1]})
    yield ray.data.from_pandas(in_df)


@pytest.fixture(scope="function")
def ds_pandas_list_multi_column_format(ray_start_regular_shared):
    in_df = pd.DataFrame({"column_1": [1], "column_2": [1]})
    yield ray.data.from_pandas([in_df] * 4)


# ===== Arrow dataset formats =====
@pytest.fixture(scope="function")
def ds_arrow_single_column_format(ray_start_regular_shared):
    yield ray.data.from_arrow(pa.table({"column_1": [1, 2, 3, 4]}))


@pytest.fixture(scope="function")
def ds_arrow_single_column_tensor_format(ray_start_regular_shared):
    yield ray.data.from_arrow(
        pa.table(
            {
                TENSOR_COLUMN_NAME: ArrowTensorArray.from_numpy(
                    np.arange(16).reshape((4, 2, 2))
                )
            }
        )
    )


@pytest.fixture(scope="function")
def ds_arrow_multi_column_format(ray_start_regular_shared):
    yield ray.data.from_arrow(
        pa.table(
            {
                "column_1": [1, 2, 3, 4],
                "column_2": [1, -1, 1, -1],
            }
        )
    )


@pytest.fixture(scope="function")
def ds_list_arrow_multi_column_format(ray_start_regular_shared):
    yield ray.data.from_arrow([pa.table({"column_1": [1], "column_2": [1]})] * 4)


# ===== Numpy dataset formats =====
@pytest.fixture(scope="function")
def ds_numpy_single_column_tensor_format(ray_start_regular_shared):
    yield ray.data.from_numpy(np.arange(16).reshape((4, 2, 2)))


@pytest.fixture(scope="function")
def ds_numpy_list_of_ndarray_tensor_format(ray_start_regular_shared):
    yield ray.data.from_numpy([np.arange(4).reshape((1, 2, 2))] * 4)


@pytest.fixture(params=["5.0.0"])
def unsupported_pyarrow_version(request):
    orig_version = pa.__version__
    pa.__version__ = request.param
    # Unset pyarrow version cache.
    import ray._private.arrow_utils

    ray._private.arrow_utils._PYARROW_INSTALLED = None
    ray._private.arrow_utils._PYARROW_VERSION = None
    yield request.param
    pa.__version__ = orig_version


@pytest.fixture
def disable_pyarrow_version_check():
    os.environ["RAY_DISABLE_PYARROW_VERSION_CHECK"] = "1"
    yield
    del os.environ["RAY_DISABLE_PYARROW_VERSION_CHECK"]


# ===== Observability & Logging Fixtures =====
@pytest.fixture
def op_two_block():
    block_params = {
        "num_rows": [10000, 5000],
        "size_bytes": [100, 50],
        "uss_bytes": [1024 * 1024 * 2, 1024 * 1024 * 1],
        "wall_time": [5, 10],
        "cpu_time": [1.2, 3.4],
        "udf_time": [1.1, 1.7],
        "node_id": ["a1", "b2"],
        "task_idx": [0, 1],
    }

    block_delay = 20
    block_meta_list = []
    for i in range(len(block_params["num_rows"])):
        block_exec_stats = BlockExecStats()
        # The blocks are executing from [0, 5] and [20, 30].
        block_exec_stats.start_time_s = time.perf_counter() + i * block_delay
        block_exec_stats.end_time_s = (
            block_exec_stats.start_time_s + block_params["wall_time"][i]
        )
        block_exec_stats.wall_time_s = block_params["wall_time"][i]
        block_exec_stats.cpu_time_s = block_params["cpu_time"][i]
        block_exec_stats.udf_time_s = block_params["udf_time"][i]
        block_exec_stats.node_id = block_params["node_id"][i]
        block_exec_stats.max_uss_bytes = block_params["uss_bytes"][i]
        block_exec_stats.task_idx = block_params["task_idx"][i]
        block_meta_list.append(
            BlockMetadata(
                num_rows=block_params["num_rows"][i],
                size_bytes=block_params["size_bytes"][i],
                input_files=None,
                exec_stats=block_exec_stats,
            )
        )
    return block_params, block_meta_list


def equals_or_true(count, expected_count):
    if isinstance(expected_count, int):
        if count != expected_count:
            return False
    else:
        if not expected_count(count):
            return False
    return True


class CoreExecutionMetrics:
    def __init__(self, task_count=None, object_store_stats=None, actor_count=None):
        self.task_count = task_count
        self.object_store_stats = object_store_stats
        self.actor_count = actor_count

    def get_task_count(self):
        return self.task_count

    def get_object_store_stats(self):
        return self.object_store_stats

    def get_actor_count(self):
        return self.actor_count

    def _assert_count_equals(self, actual_count, expected_count):
        diff = {}
        # Check that all tasks in expected tasks match those in actual task
        # count.
        for name, count in expected_count.items():
            if not equals_or_true(actual_count[name], count):
                diff[name] = (actual_count[name], count)

        assert len(diff) == 0, "\nTask diff:\n" + "\n".join(
            f" - {key}: expected {val[1]}, got {val[0]}" for key, val in diff.items()
        )

    def assert_task_metrics(self, expected_metrics):
        """
        Assert equality to the given { <task name>: <task count> }.
        A lambda that takes in the count and returns a bool to assert can also
        be given instead of an integer task count.

        An empty dict means that we expected no tasks to run. Pass None to skip
        the check.
        """
        if expected_metrics.get_task_count() is None:
            return

        expected_task_count = expected_metrics.get_task_count()
        actual_task_count = self.get_task_count()
        self._assert_count_equals(actual_task_count, expected_task_count)

    def assert_object_store_metrics(self, expected_metrics):
        """
        By default this checks that no objects were spilled or restored.
        Collected stats only apply to plasma store objects and exclude inlined
        or in-memory objects.

        Caller can also override the following fields with a value or lambda to assert.
        - spilled_bytes_total
        - restored_bytes_total
        - cumulative_created_plasma_bytes
        - cumulative_created_plasma_objects
        """
        expected_object_store_stats = (
            CoreExecutionMetrics.get_default_object_store_stats()
        )
        if expected_metrics.get_object_store_stats() is not None:
            for key, val in expected_metrics.get_object_store_stats().items():
                expected_object_store_stats[key] = val

        actual_object_store_stats = self.get_object_store_stats()
        for key, val in expected_object_store_stats.items():
            print(f"{key}: Expect {val}, got {actual_object_store_stats[key]}")
            assert equals_or_true(
                actual_object_store_stats[key], val
            ), f"{key}: expected {val} got {actual_object_store_stats[key]}"

    def assert_actor_metrics(self, expected_metrics):
        if expected_metrics.get_actor_count() is None:
            return

        expected_actor_count = expected_metrics.get_actor_count()
        actual_actor_count = self.get_actor_count()
        self._assert_count_equals(actual_actor_count, expected_actor_count)

    @staticmethod
    def get_default_object_store_stats():
        return {
            "spilled_bytes_total": 0,
            "restored_bytes_total": 0,
        }


class PhysicalCoreExecutionMetrics(CoreExecutionMetrics):
    """Generated from a snapshot of the metrics collected by Ray Core during
    the physical execution.

    NOTE(swang): Currently object store stats only include objects stored in
    plasma shared memory.
    """

    def __init__(self, last_snapshot=None):
        self.task_metrics = ray.util.state.list_tasks(detail=True, limit=10_000)
        self.last_snapshot = last_snapshot

        memory_info = get_memory_info_reply(
            get_state_from_address(ray.get_runtime_context().gcs_address)
        )
        self.object_store_stats = {
            "spilled_bytes_total": memory_info.store_stats.spilled_bytes_total,
            "restored_bytes_total": memory_info.store_stats.restored_bytes_total,
            "cumulative_created_plasma_bytes": (
                memory_info.store_stats.cumulative_created_bytes
            ),
            "cumulative_created_plasma_objects": (
                memory_info.store_stats.cumulative_created_objects
            ),
        }

        self.actor_metrics = ray.util.state.list_actors(limit=10_000)

    def clear_task_count(self):
        self.task_metrics = []

    def clear_object_store_stats(self):
        self.object_store_stats = {}

    def clear_actor_count(self):
        self.actor_metrics = []

    def get_task_count(self):
        task_count = defaultdict(int)
        tasks = self.task_metrics
        tasks = [t for t in tasks if t.name != "barrier"]

        for task in tasks:
            task_count[task.name] += 1

        # Filter out previous and dummy tasks.
        if self.last_snapshot is not None:
            prev_task_count = self.last_snapshot.get_task_count()
            if prev_task_count is not None:
                for name, count in prev_task_count.items():
                    task_count[name] -= count
                    if task_count[name] < 0:
                        task_count[name] = 0
        return task_count

    def get_actor_count(self):
        actor_count = defaultdict(int)
        for actor in self.actor_metrics:
            actor_count[actor.class_name] += 1
        if self.last_snapshot is not None:
            prev_actor_count = self.last_snapshot.get_actor_count()
            if prev_actor_count is not None:
                for name, count in prev_actor_count.items():
                    actor_count[name] -= count
                    if actor_count[name] < 0:
                        actor_count[name] = 0
        return actor_count

    def get_object_store_stats(self):
        object_store_stats = self.object_store_stats.copy()
        if self.last_snapshot is not None:
            prev_object_store_stats = self.last_snapshot.get_object_store_stats()
            if prev_object_store_stats is not None:
                for key, val in prev_object_store_stats.items():
                    object_store_stats[key] -= val
        return object_store_stats


# Dummy task used to make sure that we wait until (most) stats are available.
@ray.remote
def barrier():
    time.sleep(1)
    return


@ray.remote
def warmup():
    time.sleep(1)
    return np.zeros(1024 * 1024, dtype=np.uint8)


def task_metrics_flushed(refs):
    task_ids = [t.task_id for t in ray.util.state.list_tasks(limit=10_000)]
    # All tasks appear in the metrics.
    return all(ref.task_id().hex() in task_ids for ref in refs)


def get_initial_core_execution_metrics_snapshot():
    # Warmup plasma store and workers.
    refs = [warmup.remote() for _ in range(int(ray.cluster_resources()["CPU"]))]
    ray.get(refs)
    wait_for_condition(lambda: task_metrics_flushed(refs))

    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(
            task_count={"warmup": lambda count: True}, object_store_stats={}
        ),
        last_snapshot=None,
    )
    return last_snapshot


def assert_core_execution_metrics_equals(
    expected_metrics: CoreExecutionMetrics,
    last_snapshot=None,
):
    # Wait for one task per CPU to finish to prevent a race condition where not
    # all of the task metrics have been collected yet.
    if expected_metrics.get_task_count() is not None:
        refs = [barrier.remote() for _ in range(int(ray.cluster_resources()["CPU"]))]
        ray.get(refs)
        wait_for_condition(lambda: task_metrics_flushed(refs))

    metrics = PhysicalCoreExecutionMetrics(last_snapshot)
    metrics.assert_task_metrics(expected_metrics)
    metrics.assert_object_store_metrics(expected_metrics)
    metrics.assert_actor_metrics(expected_metrics)

    # Return a last_snapshot to the current snapshot of metrics to make subsequent
    # queries easier. Don't return a last_snapshot for metrics that weren't asserted.
    last_snapshot = PhysicalCoreExecutionMetrics()
    if expected_metrics.get_task_count() is None:
        last_snapshot.clear_task_count()
    elif expected_metrics.get_object_store_stats() is None:
        last_snapshot.clear_object_store_stats()
    elif expected_metrics.get_actor_count() is None:
        last_snapshot.clear_actor_count()

    return last_snapshot


def assert_blocks_expected_in_plasma(
    last_snapshot,
    num_blocks_expected,
    block_size_expected=None,
):
    total_bytes_expected = None

    if block_size_expected is not None:
        total_bytes_expected = num_blocks_expected * block_size_expected

    print(f"Expecting {total_bytes_expected} bytes, {num_blocks_expected} blocks")

    def _assert(last_snapshot):
        assert_core_execution_metrics_equals(
            CoreExecutionMetrics(
                object_store_stats={
                    "cumulative_created_plasma_objects": (
                        lambda count: num_blocks_expected * 0.5
                        <= count
                        <= 1.5 * num_blocks_expected
                    ),
                    "cumulative_created_plasma_bytes": (
                        lambda count: total_bytes_expected is None
                        or total_bytes_expected * 0.5
                        <= count
                        <= 1.5 * total_bytes_expected
                    ),
                },
            ),
            last_snapshot,
        )
        return True

    wait_for_condition(lambda: _assert(last_snapshot))

    # Get the latest last_snapshot.
    last_snapshot = assert_core_execution_metrics_equals(
        CoreExecutionMetrics(
            object_store_stats={
                "cumulative_created_plasma_objects": lambda count: True,
                "cumulative_created_plasma_bytes": lambda count: True,
            }
        ),
        last_snapshot,
    )

    return last_snapshot


@pytest.fixture(autouse=True, scope="function")
def log_internal_stack_trace_to_stdout(restore_data_context):
    ray.data.context.DataContext.get_current().log_internal_stack_trace_to_stdout = True
