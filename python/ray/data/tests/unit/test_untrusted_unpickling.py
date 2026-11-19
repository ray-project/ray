"""The untrusted-unpickling guard, without a Ray cluster.

Covers the audit-hook mechanism, generator scoping, the pickled-object
extension-type check, the datasource hooks and the
``RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR`` switch. The read-operator boundaries
and Ray-internal ``ray.get`` under the guard are covered end to end by
``python/ray/data/tests/test_untrusted_unpickling.py``.
"""

import io
import multiprocessing
import os
import pickle
import sys
import threading
import types

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.object_extensions.arrow import (
    AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR,
    ArrowPythonObjectType,
)
from ray.data._internal.untrusted_unpickling import (
    UntrustedUnpicklingError,
    allow_unsafe_unpickling,
    forbid_untrusted_unpickling,
    guard_datasource_call,
    guard_iterator,
    is_unpickling_forbidden,
)
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask
from ray.data.datasource.datasource import Reader
from ray.data.tests.conftest import *  # noqa: F401, F403


class _Plain:
    def __init__(self, value):
        self.value = value


PRIMITIVE_PICKLE = pickle.dumps({"a": [1, 2.0, "s", b"b", None]})
CLASS_PICKLE = pickle.dumps(_Plain(1))


def test_primitive_pickle_loads_inside_forbid():
    with forbid_untrusted_unpickling():
        assert pickle.loads(PRIMITIVE_PICKLE) == {"a": [1, 2.0, "s", b"b", None]}


def test_class_pickle_blocked_inside_forbid():
    with forbid_untrusted_unpickling("Opt in with allow_pickle=True."), pytest.raises(
        UntrustedUnpicklingError, match="allow_pickle=True"
    ) as exc_info:
        pickle.loads(CLASS_PICKLE)
    assert "_Plain" in str(exc_info.value)


def test_deserialization_blocked_before_it_runs(tmp_path):
    marker = tmp_path / "marker"

    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    payload = pickle.dumps(Exploit())
    with forbid_untrusted_unpickling(), pytest.raises(UntrustedUnpicklingError):
        pickle.loads(payload)
    assert not marker.exists()


def test_python_unpickler_subclass_blocked():
    class MyUnpickler(pickle.Unpickler):
        pass

    with forbid_untrusted_unpickling(), pytest.raises(UntrustedUnpicklingError):
        MyUnpickler(io.BytesIO(CLASS_PICKLE)).load()


def test_ray_cloudpickle_is_not_exempt():
    # Ray's own bytes are allowed per call site with
    # ``allow_unsafe_unpickling()``, not by exempting cloudpickle.
    import ray.cloudpickle as cloudpickle

    blob = cloudpickle.dumps(_Plain(2))
    with forbid_untrusted_unpickling(), pytest.raises(UntrustedUnpicklingError):
        cloudpickle.loads(blob)
    with forbid_untrusted_unpickling(), allow_unsafe_unpickling():
        assert cloudpickle.loads(blob).value == 2


def test_numpy_object_array_blocked_inside_forbid_allowed_outside():
    payload = pickle.dumps(np.array(["a", "b"], dtype=object))
    with forbid_untrusted_unpickling(), pytest.raises(UntrustedUnpicklingError):
        pickle.loads(payload)
    assert pickle.loads(payload).tolist() == ["a", "b"]


def test_allow_nested_inside_forbid_and_restores():
    with forbid_untrusted_unpickling():
        assert is_unpickling_forbidden()
        with allow_unsafe_unpickling():
            assert not is_unpickling_forbidden()
            assert pickle.loads(CLASS_PICKLE).value == 1
        assert is_unpickling_forbidden()
        with pytest.raises(UntrustedUnpicklingError):
            pickle.loads(CLASS_PICKLE)
    assert not is_unpickling_forbidden()


def test_guard_iterator_scopes_only_the_producer():
    def producer():
        assert is_unpickling_forbidden()
        with pytest.raises(UntrustedUnpicklingError):
            pickle.loads(CLASS_PICKLE)
        yield 1
        assert is_unpickling_forbidden()
        yield 2

    seen = []
    for item in guard_iterator(producer):
        # The consumer runs between yields and must not be blocked.
        assert not is_unpickling_forbidden()
        assert pickle.loads(CLASS_PICKLE).value == 1
        seen.append(item)
    assert seen == [1, 2]
    assert not is_unpickling_forbidden()


def test_guard_iterator_covers_eager_iterables():
    def eager():
        # Everything happens while building the list, before iteration.
        with pytest.raises(UntrustedUnpicklingError):
            pickle.loads(CLASS_PICKLE)
        return [1]

    assert list(guard_iterator(eager)) == [1]


def test_guard_iterator_propagates_producer_error():
    def failing():
        yield 1
        raise RuntimeError("boom")

    it = guard_iterator(failing)
    assert next(it) == 1
    with pytest.raises(RuntimeError, match="boom"):
        next(it)


def test_helper_thread_is_not_covered():
    # Known limit: the flag is per-thread. Readers must not start their own
    # threads for decoding (``make_async_gen`` copies the context).
    result = {}

    def unpickle_in_thread():
        try:
            result["value"] = pickle.loads(CLASS_PICKLE).value
        except UntrustedUnpicklingError:
            result["value"] = "blocked"

    with forbid_untrusted_unpickling():
        t = threading.Thread(target=unpickle_in_thread)
        t.start()
        t.join()
    assert result["value"] == 1


def _child_unpickles(blob):
    return pickle.loads(blob).value


@pytest.mark.skipif(sys.platform == "win32", reason="fork is POSIX only")
def test_forked_child_starts_unguarded():
    ctx = multiprocessing.get_context("fork")
    with forbid_untrusted_unpickling():
        with ctx.Pool(1) as pool:
            # The child inherits the flag through fork and must reset it, or it
            # would refuse to unpickle its own task.
            assert pool.apply(_child_unpickles, (CLASS_PICKLE,)) == 1


class _Payload:
    pass


def _object_array() -> pa.ExtensionArray:
    ext_type = ArrowPythonObjectType()
    storage = pa.array([pickle.dumps({"key": "value"})], type=ext_type.storage_type)
    return pa.ExtensionArray.from_storage(ext_type, storage)


def _nest(shape: str, obj: pa.Array) -> pa.Array:
    if shape == "top_level":
        return obj
    if shape == "list":
        return pa.ListArray.from_arrays([0, len(obj)], obj)
    if shape == "large_list":
        return pa.LargeListArray.from_arrays([0, len(obj)], obj)
    if shape == "fixed_size_list":
        return pa.FixedSizeListArray.from_arrays(obj, len(obj))
    if shape == "struct":
        return pa.StructArray.from_arrays([obj], ["field"])
    if shape == "list_of_struct":
        struct = pa.StructArray.from_arrays([obj], ["field"])
        return pa.ListArray.from_arrays([0, len(struct)], struct)
    if shape == "map":
        keys = pa.array([str(i) for i in range(len(obj))])
        return pa.MapArray.from_arrays([0, len(obj)], keys, obj)
    raise AssertionError(shape)


# pyarrow cannot write a dictionary-encoded extension column to parquet or IPC,
# so no file can carry that shape. The check runs when pyarrow resolves the type,
# however deeply it is nested, so nesting shapes are what matter here.
SHAPES = [
    "top_level",
    "list",
    "large_list",
    "fixed_size_list",
    "struct",
    "list_of_struct",
    "map",
]


@pytest.mark.parametrize("shape", SHAPES)
def test_pickled_column_in_parquet_refused_inside_read(tmp_path, shape):
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"col": _nest(shape, _object_array())}), path)

    # pyarrow rebuilds the extension type while parsing the schema, which is
    # where the check fires: before any row exists.
    with forbid_untrusted_unpickling(), pytest.raises(
        UntrustedUnpicklingError, match="arrow_pickled_object"
    ):
        pq.read_table(path)

    # Outside a read (Ray's own transport, a user's own pyarrow call) the type
    # still resolves.
    assert pq.read_table(path).num_rows == 1


@pytest.mark.parametrize("shape", SHAPES)
def test_pickled_column_in_ipc_stream_refused_inside_read(shape):
    table = pa.table({"col": _nest(shape, _object_array())})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    buf = sink.getvalue()

    with forbid_untrusted_unpickling(), pytest.raises(
        UntrustedUnpicklingError, match="arrow_pickled_object"
    ):
        pa.ipc.open_stream(buf).read_all()

    assert pa.ipc.open_stream(buf).read_all().num_rows == 1


@pytest.mark.parametrize("shape", SHAPES)
def test_pickled_column_via_c_data_interface_refused_inside_read(shape):
    # Native readers (e.g. Hudi, Lance) hand batches to pyarrow through the
    # Arrow C Data Interface, which also rebuilds the extension type on import.
    batch = pa.RecordBatch.from_arrays([_nest(shape, _object_array())], ["col"])

    class _NativeBatch:
        def __arrow_c_array__(self, requested_schema=None):
            return batch.__arrow_c_array__(requested_schema)

    with forbid_untrusted_unpickling(), pytest.raises(
        UntrustedUnpicklingError, match="arrow_pickled_object"
    ):
        pa.record_batch(_NativeBatch())

    assert pa.record_batch(_NativeBatch()).num_rows == 1


def test_plain_columns_read_inside_read(tmp_path):
    path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "ints": pa.array([1, 2]),
                "strings": pa.array(["a", "b"]),
                "lists": pa.array([[1], [2]], type=pa.list_(pa.int64())),
            }
        ),
        path,
    )
    with forbid_untrusted_unpickling():
        assert pq.read_table(path).num_rows == 2


def test_autoload_env_var_disables_guard(tmp_path, monkeypatch):
    # RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR=1 turns the guard off: the flag is
    # never set, so pickled columns read and libraries may unpickle.
    monkeypatch.setenv(AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR, "1")
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"col": _object_array()}), path)

    with forbid_untrusted_unpickling():
        assert not is_unpickling_forbidden()
        assert pq.read_table(path)["col"][0].as_py() == {"key": "value"}
        assert isinstance(pickle.loads(pickle.dumps(_Payload())), _Payload)
    assert list(guard_iterator(lambda: [1, 2])) == [1, 2]


def test_guard_iterator_covers_producer_not_consumer():
    def producer():
        with pytest.raises(UntrustedUnpicklingError):
            pickle.loads(pickle.dumps(_Payload()))
        yield 1
        yield 2

    seen = []
    for item in guard_iterator(producer):
        # A fused UDF runs here, between yields, and must not be blocked.
        assert isinstance(pickle.loads(pickle.dumps(_Payload())), _Payload)
        seen.append(item)
    assert seen == [1, 2]


def test_guard_iterator_covers_threaded_reads():
    # Readers fan fragments out to worker threads via ``make_async_gen``; the
    # flag must follow them there.
    from ray.data._internal.util import make_async_gen

    def read_fragments(fragments):
        for _ in fragments:
            with pytest.raises(UntrustedUnpicklingError):
                pickle.loads(pickle.dumps(_Payload()))
            yield is_unpickling_forbidden()

    def reader():
        return make_async_gen(
            iter(range(4)), read_fragments, preserve_ordering=True, num_workers=2
        )

    assert list(guard_iterator(reader)) == [True] * 4


def test_guard_iterator_closes_producer_on_early_exit():
    closed_guarded = []

    def producer():
        try:
            yield 1
            yield 2
        finally:
            closed_guarded.append(is_unpickling_forbidden())

    gen = guard_iterator(producer)
    assert isinstance(gen, types.GeneratorType)
    assert next(gen) == 1
    gen.close()
    assert closed_guarded == [True]


class _RecordingDatasource(Datasource):
    """Records whether unpickling was forbidden in each driver-side hook."""

    def __init__(self):
        super().__init__()
        self.seen = {"__init__": is_unpickling_forbidden()}

    def estimate_inmemory_data_size(self):
        self.seen["estimate_inmemory_data_size"] = is_unpickling_forbidden()
        return None

    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        self.seen["get_read_tasks"] = is_unpickling_forbidden()
        return [
            ReadTask(
                lambda: [pa.table({"a": [1]})],
                BlockMetadata(
                    num_rows=1, size_bytes=8, input_files=None, exec_stats=None
                ),
            )
        ]


class _RecordingSubclass(_RecordingDatasource):
    def __init__(self):
        super().__init__()
        self.seen["subclass__init__"] = is_unpickling_forbidden()


class _ReadTasksMixin:
    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        return [is_unpickling_forbidden()]


class _MixinDatasource(_ReadTasksMixin, Datasource):
    def estimate_inmemory_data_size(self):
        return None


def test_datasource_hooks_from_mixin_run_guarded():
    assert _MixinDatasource().get_read_tasks(1) == [True]


def test_legacy_reader_datasource_still_detected():
    # Wrapping must not make the base ``get_read_tasks`` stub look implemented.
    class _LegacyDatasource(Datasource):
        def create_reader(self, **read_args) -> Reader:
            raise AssertionError("not called")

    ds = _LegacyDatasource()
    assert ds.should_create_reader
    assert _LegacyDatasource.get_read_tasks is Datasource.get_read_tasks


def test_datasource_hooks_run_guarded():
    ds = _RecordingSubclass()
    ds.estimate_inmemory_data_size()
    ds.get_read_tasks(1)
    assert ds.seen == {
        "__init__": True,
        "subclass__init__": True,
        "estimate_inmemory_data_size": True,
        "get_read_tasks": True,
    }
    # Wrapping happens once per defining class and never double-wraps.
    for fn in (_RecordingSubclass.__init__, _RecordingDatasource.__init__):
        assert guard_datasource_call(fn) is fn
    assert not is_unpickling_forbidden()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
