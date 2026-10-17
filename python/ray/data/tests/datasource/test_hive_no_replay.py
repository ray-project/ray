"""Verify Hive error tolerance and that failed reads don't replay queries."""

from pathlib import Path

import pyarrow as pa
import pytest

import ray
import ray.data

_FAKE_IMPYLA = """
import os


class Cursor:
    description = [("id", "BIGINT", None, None, None, None, None)]

    def __init__(self):
        self.fetch_calls = 0

    def execute(self, statement):
        with open(
            os.environ["HIVE_TEST_EXECUTE_COUNT_FILE"], "a", encoding="utf-8"
        ) as log:
            log.write("execute\\n")

    def fetchmany(self, size):
        self.fetch_calls += 1
        mode = os.environ["HIVE_TEST_FAILURE_MODE"]
        if mode == "success":
            return [(1,), (2,)] if self.fetch_calls == 1 else []
        if mode == "after_first_block" and self.fetch_calls == 1:
            return [(1,)]
        if mode == "worker_crash":
            os._exit(23)
        raise RuntimeError("injected fetch failure")

    def cancel_operation(self):
        pass

    def close(self):
        pass


class Connection:
    def cursor(self, user=None):
        return Cursor()

    def close(self):
        pass


def connect(**kwargs):
    return Connection()
"""


def _write_fake_impyla(tmp_path: Path):
    working_dir = tmp_path / "worker_modules"
    impala_package = working_dir / "impala"
    impala_package.mkdir(parents=True)
    (impala_package / "__init__.py").write_text("")
    (impala_package / "dbapi.py").write_text(_FAKE_IMPYLA)

    count_file = tmp_path / "execute_count.txt"
    return working_dir, count_file


@pytest.mark.parametrize("max_errored_blocks", [0, 1, -1])
@pytest.mark.parametrize(
    "failure_mode", ["before_first_block", "after_first_block", "worker_crash"]
)
def test_failed_read_submits_one_data_query(
    tmp_path: Path, failure_mode: str, max_errored_blocks: int, restore_data_context
):
    def connection_factory():
        from impala.dbapi import connect

        return connect()

    working_dir, count_file = _write_fake_impyla(tmp_path)
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={
            "working_dir": str(working_dir),
            "env_vars": {
                "HIVE_TEST_EXECUTE_COUNT_FILE": str(count_file),
                "HIVE_TEST_FAILURE_MODE": failure_mode,
            },
        },
    )
    try:
        context = ray.data.DataContext.get_current()
        context.max_errored_blocks = max_errored_blocks
        if failure_mode == "after_first_block":
            # Flush the first fetched rows as their own output block so the
            # read task produces output before the injected failure.
            context.target_max_block_size = 1
        dataset = ray.data.read_hive(
            query="SELECT id",
            connection_factory=connection_factory,
            schema=pa.schema([("id", pa.int64())]),
        )
        assert dataset.context.max_errored_blocks == max_errored_blocks
        assert context.max_errored_blocks == max_errored_blocks
        if max_errored_blocks != 0:
            rows = dataset.take_all()
            if failure_mode == "after_first_block":
                # Output emitted before a task failure can remain visible.
                assert rows in ([], [{"id": 1}])
            else:
                assert rows == []
        elif failure_mode == "worker_crash":
            with pytest.raises(Exception):
                dataset.take_all()
        else:
            with pytest.raises(Exception, match="HiveServer2 read failed"):
                dataset.take_all()
    finally:
        ray.shutdown()

    assert count_file.read_text().splitlines() == ["execute"]


@pytest.mark.parametrize("max_errored_blocks", [0, 1, -1])
def test_downstream_map_preserves_error_tolerance(
    tmp_path: Path, max_errored_blocks: int, restore_data_context
):
    def connection_factory():
        from impala.dbapi import connect

        return connect()

    def reject_first_row(row):
        if row["id"] == 1:
            raise ValueError("injected downstream failure")
        return row

    working_dir, count_file = _write_fake_impyla(tmp_path)
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={
            "working_dir": str(working_dir),
            "env_vars": {
                "HIVE_TEST_EXECUTE_COUNT_FILE": str(count_file),
                "HIVE_TEST_FAILURE_MODE": "success",
            },
        },
    )
    try:
        context = ray.data.DataContext.get_current()
        context.max_errored_blocks = max_errored_blocks
        dataset = ray.data.read_hive(
            query="SELECT id",
            connection_factory=connection_factory,
            schema=pa.schema([("id", pa.int64())]),
        )
        # Use separate input blocks so only the invalid row's map task fails.
        materialized = dataset.repartition(2).materialize()
        downstream = materialized.map(reject_first_row)
        assert dataset.context.max_errored_blocks == max_errored_blocks
        assert downstream.context.max_errored_blocks == max_errored_blocks
        assert context.max_errored_blocks == max_errored_blocks
        if max_errored_blocks == 0:
            with pytest.raises(Exception, match="injected downstream failure"):
                downstream.take_all()
        else:
            assert downstream.take_all() == [{"id": 2}]
    finally:
        ray.shutdown()

    assert count_file.read_text().splitlines() == ["execute"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
