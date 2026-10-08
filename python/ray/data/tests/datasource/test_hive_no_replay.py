"""Verify that a failed Hive read task does not submit another data query."""

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


@pytest.mark.parametrize(
    "failure_mode", ["before_first_block", "after_first_block", "worker_crash"]
)
def test_failed_read_submits_one_data_query(tmp_path: Path, failure_mode: str):
    def connection_factory():
        from impala.dbapi import connect

        return connect()

    working_dir = tmp_path / "worker_modules"
    impala_package = working_dir / "impala"
    impala_package.mkdir(parents=True)
    (impala_package / "__init__.py").write_text("")
    (impala_package / "dbapi.py").write_text(_FAKE_IMPYLA)

    count_file = tmp_path / "execute_count.txt"
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
        if failure_mode == "after_first_block":
            # Flush the first fetched rows as their own output block so the
            # read task produces output before the injected failure. Note the
            # block only becomes visible downstream once the task ends, so
            # consume with take_all like the other failure modes.
            ray.data.DataContext.get_current().target_max_block_size = 1
        dataset = ray.data.read_hive(
            query="SELECT id",
            connection_factory=connection_factory,
            schema=pa.schema([("id", pa.int64())]),
        )
        if failure_mode == "worker_crash":
            with pytest.raises(Exception):
                dataset.take_all()
        else:
            with pytest.raises(Exception, match="HiveServer2 read failed"):
                dataset.take_all()
    finally:
        ray.shutdown()

    assert count_file.read_text().splitlines() == ["execute"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
