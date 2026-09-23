"""End-to-end checks for the untrusted-unpickling guard around Ray Data reads.

The read operator runs every datasource read function with the forbid flag set;
Ray's own transport stays exempt; ``RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR=1``
turns the guard off for a job. Unit-level coverage lives in
``tests/unit/test_untrusted_unpickling.py``.
"""

import pickle
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.object_extensions.arrow import (
    AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR,
    ArrowPythonObjectArray,
    ArrowPythonObjectType,
)
from ray.data._internal.untrusted_unpickling import (
    allow_unsafe_unpickling,
    forbid_untrusted_unpickling,
)
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403


def _read_task(read_fn) -> ReadTask:
    return ReadTask(
        read_fn,
        BlockMetadata(num_rows=1, size_bytes=8, input_files=None, exec_stats=None),
    )


class _UnpicklingDatasource(Datasource):
    """A reader whose "file format" is a pickle of a numpy array."""

    def __init__(self, payload: bytes, opt_in: bool = False):
        super().__init__()
        self._payload = payload
        self._opt_in = opt_in

    def estimate_inmemory_data_size(self):
        return None

    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        payload, opt_in = self._payload, self._opt_in

        def read_fn():
            if opt_in:
                # The reader's documented opt-in: only this call is exempt.
                with allow_unsafe_unpickling():
                    arr = pickle.loads(payload)
            else:
                arr = pickle.loads(payload)
            yield pa.table({"a": arr})

        return [_read_task(read_fn)]


def test_read_task_unpickling_is_refused(ray_start_regular_shared):
    ds = ray.data.read_datasource(_UnpicklingDatasource(pickle.dumps(np.arange(3))))
    with pytest.raises(Exception, match="Refusing to unpickle") as exc_info:
        ds.take_all()
    # The error tells the user how to opt in.
    assert "allow_pickle" in str(exc_info.value)


def test_read_task_opt_in_allows_unpickling(ray_start_regular_shared):
    ds = ray.data.read_datasource(
        _UnpicklingDatasource(pickle.dumps(np.arange(3)), opt_in=True)
    )
    assert ds.take_all() == [{"a": 0}, {"a": 1}, {"a": 2}]


def test_fused_udf_may_unpickle(ray_start_regular_shared):
    # The guard covers the read function only; a fused map UDF runs between the
    # read's yields and may unpickle freely.
    ds = ray.data.range(4).map(
        lambda row: {"x": int(pickle.loads(pickle.dumps(np.arange(3))).sum())}
    )
    assert ds.take_all() == [{"x": 3}] * 4


def test_ray_transported_pickled_column_readable_inside_guard(
    ray_start_regular_shared,
):
    # A block Ray built in-process from Python objects travels through the
    # object store as a pickled Table. Rebuilding its type on the way back is
    # Ray's own transport and stays allowed inside a read.
    class Payload:
        pass

    table = pa.table({"col": ArrowPythonObjectArray.from_objects([Payload()])})
    ref = ray.put(table)
    with forbid_untrusted_unpickling():
        out = ray.get(ref)
    assert (
        out.schema.field("col").type.extension_name == "ray.data.arrow_pickled_object"
    )
    assert isinstance(out["col"][0].as_py(), Payload)


def test_autoload_env_var_disables_guard_end_to_end(
    ray_start_regular_shared, monkeypatch, tmp_path
):
    # The job-wide switch: set on the driver and handed to the read tasks
    # through their runtime_env. Both a library unpickle and a pickled column
    # then go through.
    monkeypatch.setenv(AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR, "1")
    env = {"env_vars": {AUTOLOAD_PICKLE_OBJECT_SCALAR_ENV_VAR: "1"}}

    ds = ray.data.read_datasource(
        _UnpicklingDatasource(pickle.dumps(np.arange(3))), runtime_env=env
    )
    assert ds.take_all() == [{"a": 0}, {"a": 1}, {"a": 2}]

    ext_type = ArrowPythonObjectType()
    storage = pa.array([pickle.dumps({"key": "value"})], type=ext_type.storage_type)
    pq.write_table(
        pa.table({"col": pa.ExtensionArray.from_storage(ext_type, storage)}),
        tmp_path / "data.parquet",
    )
    rows = ray.data.read_parquet(str(tmp_path / "data.parquet"), runtime_env=env)
    assert rows.take_all() == [{"col": {"key": "value"}}]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
