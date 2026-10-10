"""Check Hive cleanup through actual Ray Dataset consumption."""

from pathlib import Path
from typing import Generator, List, Tuple, cast

import pyarrow as pa
import pytest

import ray
import ray.data
from ray._common.test_utils import wait_for_condition
from ray.data._internal.execution.streaming_executor import StreamingExecutor

import psutil

_CLEANUP_EVENTS = ["cancel", "cursor_close", "connection_close"]
_FAKE_IMPYLA = """
import os
import time
from pathlib import Path


def record(event):
    with open(os.environ["HIVE_TEST_EVENTS_FILE"], "a", encoding="utf-8") as log:
        log.write(f"{event} {os.getpid()}\\n")


class Cursor:
    description = [("id", "BIGINT", None, None, None, None, None)]

    def __init__(self):
        self.fetch_calls = 0

    def execute(self, statement):
        record("execute")

    def fetchmany(self, size):
        self.fetch_calls += 1
        if self.fetch_calls == 1:
            record("first_batch")
            return [(1,)]
        if os.environ["HIVE_TEST_READ_MODE"] == "complete":
            record("eof")
            return []

        # Hold the source open after the first output. The release file is
        # created only by test teardown, after the lifecycle assertions.
        record("fetch_blocked")
        release_file = Path(os.environ["HIVE_TEST_RELEASE_FILE"])
        deadline = time.monotonic() + 30
        while not release_file.exists():
            if time.monotonic() >= deadline:
                record("gate_timeout")
                raise TimeoutError("Hive lifecycle test did not stop the read")
            time.sleep(0.01)
        record("eof")
        return []

    def cancel_operation(self):
        record("cancel")

    def close(self):
        record("cursor_close")


class Connection:
    def cursor(self, user=None):
        return Cursor()

    def close(self):
        record("connection_close")


def connect():
    record("connect")
    return Connection()
"""


def _records(path: Path) -> List[Tuple[str, int]]:
    if not path.exists():
        return []
    return [
        (event, int(pid))
        for event, pid in (line.split() for line in path.read_text().splitlines())
    ]


def _worker_stopped(pid: int) -> bool:
    try:
        process = psutil.Process(pid)
        return not process.is_running() or process.status() == psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return True


@pytest.mark.parametrize("termination", ["complete", "dataset_limit", "iterator_close"])
@pytest.mark.parametrize("block_count", [None, 4])
def test_hive_dataset_cleanup(
    tmp_path: Path, termination: str, block_count: int | None, restore_data_context
):
    def connection_factory():
        from impala.dbapi import connect

        return connect()

    working_dir = tmp_path / "worker_modules"
    impala_package = working_dir / "impala"
    impala_package.mkdir(parents=True)
    (impala_package / "__init__.py").write_text("")
    (impala_package / "dbapi.py").write_text(_FAKE_IMPYLA)
    events_file = tmp_path / "events.txt"
    release_file = tmp_path / "release"

    ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={
            "working_dir": str(working_dir),
            "env_vars": {
                "HIVE_TEST_EVENTS_FILE": str(events_file),
                "HIVE_TEST_RELEASE_FILE": str(release_file),
                "HIVE_TEST_READ_MODE": termination,
            },
        },
    )
    iterator = None
    try:
        context = ray.data.DataContext.get_current()
        context.max_errored_blocks = 0
        # Flush the first fetched row before asking the client for more rows.
        context.target_max_block_size = 1
        dataset = ray.data.read_hive(
            query="SELECT id",
            schema=pa.schema([("id", pa.int64())]),
            connection_factory=connection_factory,
            override_num_blocks=block_count,
        )
        consumed_dataset = dataset
        if termination == "dataset_limit":
            consumed_dataset = dataset.limit(1)
            assert consumed_dataset.take_all() == [{"id": 1}]
        elif termination == "complete":
            assert dataset.take_all() == [{"id": 1}]
        else:
            iterator = cast(
                Generator[pa.Table, None, None],
                iter(
                    dataset.iter_batches(
                        batch_size=1, batch_format="pyarrow", prefetch_batches=0
                    )
                ),
            )
            assert next(iterator).to_pylist() == [{"id": 1}]
            wait_for_condition(
                lambda: any(
                    event == "fetch_blocked" for event, _ in _records(events_file)
                ),
                timeout=20,
            )
            assert not release_file.exists()
            assert not any(event == "eof" for event, _ in _records(events_file))
            worker_pid = next(
                pid for event, pid in _records(events_file) if event == "execute"
            )
            iterator.close()
            wait_for_condition(lambda: _worker_stopped(worker_pid), timeout=20)

        if termination != "iterator_close":
            wait_for_condition(
                lambda: any(
                    event == "connection_close" for event, _ in _records(events_file)
                ),
                timeout=20,
            )

        records = _records(events_file)
        events = [event for event, _ in records]
        assert events.count("connect") == events.count("execute") == 1
        assert "first_batch" in events
        assert "gate_timeout" not in events
        assert not release_file.exists()
        executor = consumed_dataset._current_executor
        if termination == "iterator_close":
            assert isinstance(executor, StreamingExecutor) and executor._shutdown
        else:
            # take_all synchronizes and releases its executor before returning.
            assert executor is None

        cleanup = [event for event in events if event in _CLEANUP_EVENTS]
        if termination == "iterator_close":
            # The public iterator path force-kills the active task worker.
            # Ray resources stop, but Python finally cannot close the HS2 client.
            assert cleanup == []
        else:
            assert cleanup == _CLEANUP_EVENTS
        assert ("eof" in events) == (termination == "complete")
        assert all(pid != psutil.Process().pid for _, pid in records)
        print(f"Hive lifecycle {termination}: {records}")
    finally:
        try:
            release_file.touch()
            if iterator is not None:
                iterator.close()
        finally:
            ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
