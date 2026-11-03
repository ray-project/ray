"""Observe actual ORC workers and share independently checked micro fixtures."""

import asyncio
import copy
import os
from collections import Counter, defaultdict
from dataclasses import fields
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import pyarrow as pa
from pyarrow import orc

import ray
from ray.actor import ActorHandle
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer import (
    _OrcMetadataReader,
)
from ray.data._internal.execution.interfaces import TaskContext


@ray.remote(num_cpus=0)
class OrcTrace:
    def __init__(self):
        self.events, self.workers = [], set()
        self.signals, self.counts = defaultdict(asyncio.Event), Counter()

    def record(self, kind, path="", details=None):
        self.events.append((kind, os.path.basename(path), details))
        self.signals[kind, os.path.basename(path)].set()

    async def wait(self, kind, name=""):
        await asyncio.wait_for(self.signals[kind, name].wait(), 30)

    def count(self, key, threshold=1):
        self.counts[key] += 1
        if self.counts[key] >= threshold:
            self.record(key)
        return self.counts[key]

    def snapshot(self, clear=False):
        result = list(self.events)
        if clear:
            self.events.clear()
            self.signals.clear()
        return result

    async def arrive(self, task, pid):
        self.workers.add((task, pid))
        self.record("parallel_enter", "single.orc", (task, pid))
        if len(self.workers) >= 2:
            self.record("parallel")
        await self.wait("parallel")


def traced_indexer(
    indexer,
    trace,
    blocked_file=None,
    footer_order=None,
    failing_file=None,
    gate_first_miss=False,
):
    import ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer as module

    class MetadataReader(_OrcMetadataReader):
        def read(self, path):
            name = os.path.basename(path)
            ray.get(trace.record.remote("started", path, os.getpid()))
            if gate_first_miss and name == "f0017.orc":
                ray.get(trace.wait.remote("started", "f0018.orc"))
            if name == failing_file:
                raise OSError(f"Injected metadata failure: {path}")
            if name == blocked_file:
                ray.get(trace.wait.remote("release"))
            waits = {
                "18-first": ("f0019.orc", "f0018.orc"),
                "19-first": ("f0018.orc", "f0019.orc"),
            }
            if footer_order and name == waits[footer_order][0]:
                ray.get(trace.wait.remote("finished", waits[footer_order][1]))
            result = super().read(path)
            ray.get(trace.record.remote("finished", path, os.getpid()))
            return result

    actor = ray.remote(num_cpus=0)(MetadataReader)

    def options(**kwargs):
        def remote(*args):
            handle = cast(ActorHandle, actor.options(**kwargs).remote(*args))
            ray.get(
                trace.record.remote("metadata_actor", "pool", handle._actor_id.hex())
            )
            return handle

        return SimpleNamespace(remote=remote)

    result = copy.copy(indexer)
    original = result._iter_layouts

    def layouts(files, filesystem):
        with patch.object(module, "OrcMetadataActor", SimpleNamespace(options=options)):
            for file, layout in original(files, filesystem):
                ray.get(trace.record.remote("delivered", file.path))
                yield file, layout

    result._iter_layouts = layouts
    return result


def traced_scanner(scanner, trace, parallel_barrier=False):
    class TracedScanner(OrcScanner):
        def create_reader(self):
            reader = super().create_reader()
            original = reader._iter_stripe_tables

            def scan(reader, path, stripe, scanner_kwargs):
                task, pid = str(ray.get_runtime_context().get_task_id()), os.getpid()
                if parallel_barrier:
                    ray.get(trace.arrive.remote(task, pid))
                for n, table in enumerate(
                    original(reader, path, stripe, scanner_kwargs)
                ):
                    if n == 0:
                        ray.get(
                            trace.record.remote(
                                "decoded",
                                path,
                                (stripe.index, pid, task),
                            )
                        )
                    yield table

            reader._iter_stripe_tables = scan
            return reader

    return TracedScanner(
        **{f.name: getattr(scanner, f.name) for f in fields(scanner) if f.init}
    )


def partitioned(manifests, partitioner):
    from ray.data._internal.datasource_v2.common.listing_utils import partition_files

    yield from partition_files(
        (m.as_block() for m in manifests), cast(TaskContext, None), partitioner
    )


def prepare_orc(paths):
    from ray.data._internal.datasource_v2.common.listing_utils import sample_files
    from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
        OrcDatasourceV2,
    )
    from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
        PartitionHints,
    )

    source = OrcDatasourceV2([str(p) for p in paths])
    indexer = source._get_file_indexer()
    schema, indexer, packer = source.prepare_read(
        sample_files(indexer, source.paths, source.filesystem),
        indexer,
        PartitionHints(0, 4096, 4),
    )
    return source, schema, indexer, packer


def write_orc_stripes(path, table: pa.Table, counts=(5, 7, 4)):
    """Use Arrow's writer and decoder to check exact, small stripe boundaries."""
    assert sum(counts) == table.num_rows
    with orc.ORCWriter(
        str(path), stripe_size=1, batch_size=max(counts), compression="uncompressed"
    ) as writer:
        start = 0
        for count in counts:
            writer.write(table.slice(start, count))
            start += count
    reader = orc.ORCFile(str(path))
    assert [reader.read_stripe(i).num_rows for i in range(reader.nstripes)] == list(
        counts
    )
    assert reader.read().equals(table)
    return path


def write_stripe_pipeline_files(directory, file_count=40):
    import random

    generator = random.Random(66999)
    paths = []
    for file_index in range(file_count):
        path = directory / f"f{file_index+1:04d}.orc"
        write_orc_stripes(
            path,
            pa.table(
                {
                    "id": range(file_index * 8, (file_index + 1) * 8),
                    "payload": [generator.randbytes(1024) for _ in range(8)],
                }
            ),
            counts=(2,) * 4,
        )
        paths.append(path)
    return paths
