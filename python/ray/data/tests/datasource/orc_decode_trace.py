"""Observe real ORC metadata and decode workers without replacing decoding."""

import asyncio
import os
from dataclasses import fields
from typing import Protocol, cast
from unittest.mock import patch

import ray
from ray.actor import ActorHandle
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer import (
    OrcStripeIndexer,
    _OrcMetadataReader,
)


class _IntermediateFooterWriter(Protocol):
    """The native writer exposes this method, but its stubs omit it."""

    def write_intermediate_footer(self) -> object:
        ...


@ray.remote(num_cpus=0)
class OrcTrace:
    def __init__(self):
        self.events = []
        self.signals = {}
        self.released = asyncio.Event()
        self.consumed = 0
        self.consumption = asyncio.Event()
        self.attempts = 0
        self.read_workers = set()
        self.read_barrier = asyncio.Event()

    def record(self, kind, path, details=None):
        name = os.path.basename(path)
        self.events.append((kind, name, details))
        self.signals.setdefault((kind, name), asyncio.Event()).set()

    async def wait(self, kind, name):
        await asyncio.wait_for(
            self.signals.setdefault((kind, name), asyncio.Event()).wait(), 30
        )

    async def wait_release(self):
        await asyncio.wait_for(self.released.wait(), 30)

    def release(self):
        self.released.set()

    async def enter_read(self, task_id, pid):
        self.read_workers.add((task_id, pid))
        self.record("parallel_enter", "single.orc", (task_id, pid))
        if len(self.read_workers) >= 2:
            self.read_barrier.set()
        await asyncio.wait_for(self.read_barrier.wait(), 30)

    def clear_events(self):
        self.events = []
        self.signals = {}

    def snapshot(self):
        return list(self.events)

    def begin(self):
        self.attempts += 1
        return self.attempts

    def acknowledge(self):
        self.consumed += 1
        if self.consumed >= 2:
            self.consumption.set()

    async def wait_consumed(self):
        await asyncio.wait_for(self.consumption.wait(), 30)


def traced_indexer(
    indexer,
    trace,
    blocked_file=None,
    footer_order=None,
    failing_file=None,
    gate_first_miss=False,
):
    class MetadataReader(_OrcMetadataReader):
        def read(self, path):
            ray.get(trace.record.remote("started", path, os.getpid()))
            if gate_first_miss and os.path.basename(path) == "f0017.orc":
                ray.get(trace.wait.remote("started", "f0018.orc"))
            if os.path.basename(path) == failing_file:
                raise OSError(f"Injected metadata failure: {path}")
            if os.path.basename(path) == blocked_file:
                ray.get(trace.wait_release.remote())
            if footer_order == "18-first" and os.path.basename(path) == "f0019.orc":
                ray.get(trace.wait.remote("finished", "f0018.orc"))
            if footer_order == "19-first" and os.path.basename(path) == "f0018.orc":
                ray.get(trace.wait.remote("finished", "f0019.orc"))
            result = super().read(path)
            ray.get(trace.record.remote("finished", path, os.getpid()))
            return result

    actor_class = ray.remote(num_cpus=0)(MetadataReader)

    class ActorFactory:
        def __init__(self, options=None):
            self._options = options or {}

        def options(self, **options):
            return ActorFactory(options)

        def remote(self, *args):
            actor = cast(
                "ActorHandle[MetadataReader]",
                actor_class.options(**self._options).remote(*args),
            )
            # Record handles at submission, including actors killed before their
            # constructor finishes, so cleanup assertions cover the entire pool.
            ray.get(
                trace.record.remote("metadata_actor", "pool", actor._actor_id.hex())
            )
            return actor

    class TracedIndexer(OrcStripeIndexer):
        def _iter_layouts(self, files, filesystem):
            import ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer as module

            with patch.object(module, "OrcMetadataActor", ActorFactory()):
                for file, layout in super()._iter_layouts(files, filesystem):
                    ray.get(trace.record.remote("delivered", file.path))
                    yield file, layout

    return TracedIndexer(
        ignore_missing_paths=indexer._ignore_missing_paths,
        seeds=indexer._seeds,
        local_only=indexer._local_only,
        local_node_id=indexer._local_node_id,
        shuffle_seed=indexer._shuffle_seed,
        skip_paths=indexer._skip_paths,
        num_workers=indexer._num_workers,
        max_paths_per_output=indexer._max_paths_per_output,
    )


def traced_scanner(scanner, trace, parallel_barrier=False):
    class TracedScanner(OrcScanner):
        def create_reader(self):
            reader = super().create_reader()
            original = reader._scan_stripe

            def scan(reader, path, stripe_index, scanner_kwargs, num_rows):
                if parallel_barrier:
                    ray.get(
                        trace.enter_read.remote(
                            str(ray.get_runtime_context().get_task_id()), os.getpid()
                        )
                    )
                for i, table in enumerate(
                    original(reader, path, stripe_index, scanner_kwargs, num_rows)
                ):
                    if i == 0:
                        ray.get(
                            trace.record.remote(
                                "decoded",
                                path,
                                (
                                    int(stripe_index),
                                    os.getpid(),
                                    str(ray.get_runtime_context().get_task_id()),
                                ),
                            )
                        )
                    yield table

            reader._scan_stripe = scan
            return reader

    return TracedScanner(
        **{
            field.name: getattr(scanner, field.name)
            for field in fields(scanner)
            if field.init
        }
    )


def write_stripe_pipeline_files(directory, file_count=40):
    import random

    import pyorc

    generator = random.Random(66999)
    paths = []
    for file_index in range(file_count):
        path = directory / f"f{file_index+1:04d}.orc"
        with path.open("wb") as stream:
            writer = pyorc.Writer(
                stream,
                "struct<id:bigint,payload:binary>",
                batch_size=1,
                stripe_size=1 << 30,
                compression=pyorc.CompressionKind.NONE,
            )
            for stripe in range(4):
                writer.writerows(
                    (file_index * 8 + stripe * 2 + i, generator.randbytes(1024))
                    for i in range(2)
                )
                cast(_IntermediateFooterWriter, writer).write_intermediate_footer()
            writer.close()
        with path.open("rb") as stream:
            assert [len(stripe) for stripe in pyorc.Reader(stream).iter_stripes()] == [
                2
            ] * 4
        paths.append(path)
    return paths
