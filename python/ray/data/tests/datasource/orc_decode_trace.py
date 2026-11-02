"""Observe actual ORC workers and share independently checked micro fixtures."""

import asyncio
import copy
import os
from collections import Counter, defaultdict
from contextlib import nullcontext
from dataclasses import fields
from itertools import islice
from types import SimpleNamespace
from typing import Protocol, cast
from unittest.mock import patch

import pytest

import ray
from ray.actor import ActorHandle
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    supports_stripe_reads,
)
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer import (
    _OrcMetadataReader,
)
from ray.data._internal.execution.interfaces import TaskContext


class _IntermediateFooterWriter(Protocol):
    def write_intermediate_footer(self) -> object:
        ...


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
            original = reader._iter_native_tables

            def scan(reader, path, stripe_index, scanner_kwargs):
                task, pid = str(ray.get_runtime_context().get_task_id()), os.getpid()
                if parallel_barrier:
                    ray.get(trace.arrive.remote(task, pid))
                for n, table in enumerate(
                    original(reader, path, stripe_index, scanner_kwargs)
                ):
                    if n == 0:
                        ray.get(
                            trace.record.remote(
                                "decoded"
                                if stripe_index is not None
                                else "decoded_whole",
                                path,
                                (stripe_index, pid, task),
                            )
                        )
                    yield table

            reader._iter_native_tables = scan
            return reader

    return TracedScanner(
        **{f.name: getattr(scanner, f.name) for f in fields(scanner) if f.init}
    )


def native_stripe_read():
    """Use the real API, or assert that an unsupported assignment fails closed."""
    if supports_stripe_reads():
        return nullcontext()
    return pytest.raises(RuntimeError, match="requires Arrow native")


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


def write_orc_stripes(path, schema, rows, counts=(5, 7, 4), **options):
    """Write tiny, independently checked stripes without buffered boundary guesses."""
    import pyorc

    settings = dict(
        batch_size=1, stripe_size=1 << 30, compression=pyorc.CompressionKind.NONE
    )
    settings.update(options)
    rows = iter(rows)
    with path.open("wb") as stream:
        writer = pyorc.Writer(stream, schema, **settings)
        for count in counts:
            writer.writerows(islice(rows, count))
            cast(_IntermediateFooterWriter, writer).write_intermediate_footer()
        writer.close()
    with path.open("rb") as stream:
        assert [len(stripe) for stripe in pyorc.Reader(stream).iter_stripes()] == list(
            counts
        )
    return path


def write_stripe_pipeline_files(directory, file_count=40):
    import random

    generator = random.Random(66999)
    paths = []
    for file_index in range(file_count):
        path = directory / f"f{file_index+1:04d}.orc"
        write_orc_stripes(
            path,
            "struct<id:bigint,payload:binary>",
            ((file_index * 8 + i, generator.randbytes(1024)) for i in range(8)),
            counts=(2,) * 4,
        )
        paths.append(path)
    return paths
