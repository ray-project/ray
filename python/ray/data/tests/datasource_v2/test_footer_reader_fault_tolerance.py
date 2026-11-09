"""Fault tolerance of ``FooterReaderActor`` -- a Ray Core retry after the actor
dies must deliver every file exactly once.
"""

import os
import signal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

import ray
from ray.data._internal.datasource_v2.formats.parquet.footer_reader import (
    FooterReaderActor,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo


def _actor_pid(actor) -> int:
    # ``ActorProxy`` stubs don't model the built-in ``__ray_call__``.
    # pyrefly: ignore[missing-attribute]
    return ray.get(actor.__ray_call__.remote(lambda self: os.getpid()))


def test_retry_after_actor_death_delivers_each_file_once(tmp_path, shutdown_only):
    num_files = 12
    num_files_before_kill = 4
    killed_marker = str(tmp_path / "killed")
    file_infos = []
    for i in range(num_files):
        path = str(tmp_path / f"f_{i:03d}.parquet")
        pq.write_table(pa.table({"id": [i]}), path)
        file_infos.append(FileInfo(path=path, size=os.path.getsize(path)))
    listing_index_by_path = {info.path: i for i, info in enumerate(file_infos)}

    def make_attempts_complete_in_different_orders():
        # Runs in every worker, including the restarted actor's. Before the
        # kill, reads complete in reverse listing order. After it, in listing
        # order. A reader that yielded in completion order would then put
        # different files at the positions the first attempt already delivered.
        import time

        from ray.data._internal.datasource_v2.formats.parquet.footer_reader import (
            FooterReader,
        )

        read_and_chunk = FooterReader._read_and_chunk

        def delayed_read_and_chunk(self, file):
            listing_index = listing_index_by_path[file.path]
            if os.path.exists(killed_marker):
                delay_steps = listing_index
            else:
                delay_steps = num_files - listing_index
            time.sleep(0.05 * delay_steps)
            return read_and_chunk(self, file)

        FooterReader._read_and_chunk = delayed_read_and_chunk

    ray.init(
        num_cpus=2,
        runtime_env={
            "worker_process_setup_hook": make_attempts_complete_in_different_orders
        },
    )
    actor = FooterReaderActor.remote(LocalFileSystem(), io_concurrency=num_files)
    first_pid = _actor_pid(actor)

    delivered_paths = []
    # Streaming ``@ray.method``; stub types ``.remote`` as ``ObjectRef`` and
    # don't preserve the method's keyword args.
    # pyrefly: ignore[not-iterable]
    for ref in actor.read_footers.remote(
        file_infos, result_batch_size=1  # pyrefly: ignore[unexpected-keyword]
    ):
        delivered_paths.extend(chunked.file.path for chunked in ray.get(ref))
        if len(delivered_paths) == num_files_before_kill:
            open(killed_marker, "w").close()
            os.kill(first_pid, signal.SIGKILL)

    assert _actor_pid(actor) != first_pid, "the actor was not restarted"
    assert delivered_paths == [info.path for info in file_infos]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
