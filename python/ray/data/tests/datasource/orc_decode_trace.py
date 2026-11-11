"""Observe actual ORC workers and share independently checked micro fixtures."""

import asyncio
import os
from dataclasses import fields

import pyarrow as pa
from pyarrow import orc

import ray
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner


@ray.remote(num_cpus=0)
class OrcTrace:
    def __init__(self):
        self.workers, self.stripes = set(), []
        self.ready = asyncio.Event()

    def snapshot(self):
        return list(self.workers), list(self.stripes)

    def decoded(self, index):
        self.stripes.append(index)

    async def arrive(self, task, pid):
        self.workers.add((task, pid))
        if len(self.workers) >= 2:
            self.ready.set()
        await asyncio.wait_for(self.ready.wait(), 30)


def traced_scanner(scanner, trace):
    class TracedScanner(OrcScanner):
        def create_reader(self):
            reader = super().create_reader()
            original = reader._iter_stripe_tables

            def scan(reader, path, stripe, scanner_kwargs):
                task, pid = str(ray.get_runtime_context().get_task_id()), os.getpid()
                ray.get(trace.arrive.remote(task, pid))
                for n, table in enumerate(
                    original(reader, path, stripe, scanner_kwargs)
                ):
                    if n == 0:
                        ray.get(trace.decoded.remote(stripe.index))
                    yield table

            reader._iter_stripe_tables = scan
            return reader

    return TracedScanner(
        **{f.name: getattr(scanner, f.name) for f in fields(scanner) if f.init}
    )


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
