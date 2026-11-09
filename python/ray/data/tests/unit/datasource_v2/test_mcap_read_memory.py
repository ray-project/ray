"""Unit tests for the estimate of a read task's peak memory (``chunk_runs``).

Each constant is pinned by measuring the objects it stands for, and the
estimate is checked against the measured peak of a real read.
"""

import gc
import importlib.util
import os
import random
import sys
import tracemalloc
from collections import deque

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap import mcap_datasource_v2
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import read_chunk
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
    _BUFFER_GROWTH,
    _HEAP_ENTRY_BYTES,
    _MESSAGE_RECORD_HEADER_BYTES,
    _PARSED_MESSAGE_BYTES,
    ReadMemory,
    read_overhead,
)
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    as_read_task,
    list_manifests,
    pack,
    scanner_for,
    summary_of,
    write_mcap,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

PAYLOAD_BYTES = 48  # small messages, where per-message overhead dominates
NUM_MESSAGES = 20_000
# Log times in nanoseconds since the epoch, which Python stores in larger ints
# than small numbers.
EPOCH_NS = 1_700_000_000 * 10**9
SMALL_CHUNKS = 64 << 10  # chunk size that splits the file into many chunks
ONE_CHUNK = 1 << 30


def write_small_messages(path: str, chunk_size: int) -> str:
    """Write ``NUM_MESSAGES`` payloads of ``PAYLOAD_BYTES`` on one channel, half
    random so that zstd compresses them partly, timed and numbered as a recorder
    writes them."""
    rng = random.Random(0)
    half = PAYLOAD_BYTES // 2
    messages = [
        {
            "topic": "/a",
            "log_time": EPOCH_NS + i * 1000,
            "sequence": i,
            "data": rng.randbytes(half) + bytes(half),
        }
        for i in range(NUM_MESSAGES)
    ]
    write_mcap(path, messages, chunk_size=chunk_size, message_encoding="cdr")
    return path


def traced(run):
    """``(kept, peak)``: bytes ``run()`` leaves allocated, and its peak."""
    gc.collect()
    tracemalloc.start()
    before = tracemalloc.get_traced_memory()[0]
    result = run()
    kept, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    del result
    return kept - before, peak - before


def listed_overhead(path: str, manifest, log_time_order: bool) -> int:
    """W + R of the listing of every chunk of ``path``."""
    counts = [int(md["num_rows"]) for md in manifest.file_chunk_metadatas]
    memory = ReadMemory(mcap_datasource_v2._BIN_PACKING_BYTES, log_time_order)
    return read_overhead(path, summary_of(path).chunk_indexes, counts, memory)


def test_message_record_header_is_31_bytes():
    """A message record is its payload plus the header the estimate subtracts."""
    from mcap.data_stream import RecordBuilder
    from mcap.records import Message

    builder = RecordBuilder()
    message = Message(channel_id=1, log_time=2, data=b"x", publish_time=3, sequence=4)
    message.write(builder)

    assert len(builder.end()) - 1 == _MESSAGE_RECORD_HEADER_BYTES


def test_parsed_message_and_heap_entry_sizes_are_bounded(tmp_path):
    """A parsed message and a log-time heap entry take at most their constants
    beyond their payload and path, and at least 80% of them."""
    from mcap.stream_reader import breakup_chunk

    path = write_small_messages(os.path.join(tmp_path, "one.mcap"), ONE_CHUNK)
    summary = summary_of(path)
    with open(path, "rb") as f:
        chunk = read_chunk(f, summary.chunk_indexes[0])

    parsed, _ = traced(lambda: breakup_chunk(chunk))
    # The unordered read peaks at the end of parsing, with the decompressed
    # records. The ordered read peaks with every message in its heap, after
    # those records are freed.
    peaks = {}
    for ordered in (True, False):
        reader = SelectedMessageReader(MCAPSelection(), log_time_order=ordered)
        with open(path, "rb") as f:
            messages = reader.iter_chunks(f, path, summary, None)
            peaks[ordered] = traced(lambda: deque(messages, maxlen=0))[1]
    heap = peaks[True] - peaks[False] + summary.chunk_indexes[0].uncompressed_size

    message_overhead = parsed / NUM_MESSAGES - PAYLOAD_BYTES
    heap_entry = heap / NUM_MESSAGES - len(path)
    assert 0.8 * _PARSED_MESSAGE_BYTES <= message_overhead <= _PARSED_MESSAGE_BYTES
    assert 0.8 * _HEAP_ENTRY_BYTES <= heap_entry <= _HEAP_ENTRY_BYTES


def test_bytearray_spare_capacity_is_an_eighth():
    """A ``bytearray`` grown by ``+=``, as the table builder grows its payload
    buffer, holds up to ``_BUFFER_GROWTH`` of its bytes in spare capacity."""
    empty = sys.getsizeof(bytearray())
    buffer, worst = bytearray(), 0.0

    while len(buffer) < 4 << 20:
        buffer += b"y" * PAYLOAD_BYTES
        if len(buffer) >= 1 << 20:  # past the fixed slack of small sizes
            worst = max(worst, (sys.getsizeof(buffer) - empty) / len(buffer))

    assert 1 + _BUFFER_GROWTH - 0.01 <= worst <= 1 + _BUFFER_GROWTH + 1e-4


@pytest.mark.parametrize("log_time_order", [True, False])
@pytest.mark.parametrize("chunk_size", [ONE_CHUNK, SMALL_CHUNKS])
def test_estimate_bounds_the_measured_peak_of_a_read(
    tmp_path, log_time_order, chunk_size
):
    """The estimate T + W + R for a task that reads the whole file is at least
    the measured peak of reading it, and less than twice that."""
    path = write_small_messages(os.path.join(tmp_path, "small.mcap"), chunk_size)
    datasource = MCAPDatasourceV2([path], log_time_order=log_time_order)
    (manifest,) = list_manifests(datasource)
    task = as_read_task(manifest)
    reader = scanner_for(datasource).create_reader()
    deque(reader.read(task), maxlen=0)  # Warm imports outside the measurement.
    pool = pa.default_memory_pool()
    arrow_before = max(pool.bytes_allocated(), pool.max_memory())

    _, python_peak = traced(lambda: deque(reader.read(task), maxlen=0))

    measured = python_peak + max(0, pool.max_memory() - arrow_before)
    # Rows weigh T * budget / (budget - W - R), which gives back T.
    budget = mcap_datasource_v2._BIN_PACKING_BYTES
    overhead = listed_overhead(path, manifest, log_time_order)
    weights = sum(int(md["size_bytes"]) for md in manifest.file_chunk_metadatas)
    estimate = weights * (budget - overhead) / budget + overhead
    assert measured <= estimate < 2 * measured


def test_packer_leaves_room_for_the_working_set(tmp_path, monkeypatch):
    """With a budget of W + R plus 3.5 chunks' tables, four chunks would fit by
    their tables alone. Weighed with room for W + R, a fourth full chunk does
    not fit, and every task stays within the budget."""
    path = write_small_messages(os.path.join(tmp_path, "small.mcap"), SMALL_CHUNKS)
    datasource = MCAPDatasourceV2([path], log_time_order=False)
    (manifest,) = list_manifests(datasource)
    overhead = listed_overhead(path, manifest, log_time_order=False)
    heaviest = max(int(md["size_bytes"]) for md in manifest.file_chunk_metadatas)
    table = heaviest * (1 - overhead / mcap_datasource_v2._BIN_PACKING_BYTES)
    budget = int(overhead + 3.5 * table)
    monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", budget)

    (relisted,) = list_manifests(datasource)
    tasks = pack(datasource)

    weights = [int(md["size_bytes"]) for md in relisted.file_chunk_metadatas]
    assert 4 * table < budget < 4 * max(weights)
    task_weights = [
        sum(int(md["size_bytes"]) for md in task.file_chunk_metadatas) for task in tasks
    ]
    assert max(task_weights) <= budget
    chunks = [sum(len(md["unit_ids"]) for md in t.file_chunk_metadatas) for t in tasks]
    assert sum(chunks) == len(summary_of(path).chunk_indexes)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
