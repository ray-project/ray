"""Tests for the `BlockMetadataWithSchema` pickle round-trip path and its
schema-deserialization cache (`_read_arrow_schema_cached`).

The cache is on the StreamingExecutor scheduler thread's hot path; we want to
verify:
  1. Round-tripping preserves schema equality and field metadata.
  2. Repeated unpickles of identical bytes reuse the cached `pa.Schema`.
  3. Distinct schema bytes produce distinct cached entries.
  4. Pandas / None schemas still work (no caching path taken).
"""

import pickle

import pyarrow as pa
import pytest

from ray.data.block import (
    BlockMetadata,
    BlockMetadataWithSchema,
    _read_arrow_schema_cached,
)


def _wide_arrow_schema(num_cols: int = 50) -> pa.Schema:
    fields = []
    for i in range(num_cols):
        if i % 3 == 0:
            fields.append(pa.field(f"scalar_{i}", pa.float32()))
        elif i % 3 == 1:
            fields.append(pa.field(f"vec64_{i}", pa.list_(pa.float32(), 64)))
        else:
            fields.append(pa.field(f"vec32_{i}", pa.list_(pa.float32(), 32)))
    return pa.schema(fields)


@pytest.fixture(autouse=True)
def _clear_cache():
    _read_arrow_schema_cached.cache_clear()
    yield
    _read_arrow_schema_cached.cache_clear()


def _make_bm(schema: "pa.Schema | None") -> BlockMetadataWithSchema:
    md = BlockMetadata(
        num_rows=10,
        size_bytes=1024,
        exec_stats=None,
        input_files=None,
        task_exec_stats=None,
    )
    return BlockMetadataWithSchema.from_metadata(md, schema=schema)


def test_round_trip_preserves_schema():
    schema = _wide_arrow_schema(20)
    bm = _make_bm(schema)
    restored = pickle.loads(pickle.dumps(bm))
    assert restored.schema.equals(schema)
    assert restored.num_rows == 10
    assert restored.size_bytes == 1024


def test_cache_hit_on_repeated_pickle_loads():
    schema = _wide_arrow_schema(20)
    payload = pickle.dumps(_make_bm(schema))

    info_before = _read_arrow_schema_cached.cache_info()

    restored = [pickle.loads(payload) for _ in range(50)]

    info_after = _read_arrow_schema_cached.cache_info()
    # Exactly one miss for the first load, the rest are hits.
    assert info_after.misses - info_before.misses == 1
    assert info_after.hits - info_before.hits == 49

    # All decoded schemas are identical and reference-equal to the cached one.
    first = restored[0].schema
    for r in restored[1:]:
        assert r.schema is first


def test_distinct_schemas_distinct_cache_entries():
    s1 = _wide_arrow_schema(10)
    s2 = _wide_arrow_schema(20)
    p1 = pickle.dumps(_make_bm(s1))
    p2 = pickle.dumps(_make_bm(s2))

    info_before = _read_arrow_schema_cached.cache_info()
    a = pickle.loads(p1)
    b = pickle.loads(p2)
    c = pickle.loads(p1)
    d = pickle.loads(p2)
    info_after = _read_arrow_schema_cached.cache_info()

    assert info_after.misses - info_before.misses == 2
    assert info_after.hits - info_before.hits == 2
    assert a.schema is c.schema
    assert b.schema is d.schema
    assert a.schema is not b.schema
    assert a.schema.equals(s1)
    assert b.schema.equals(s2)


def test_none_schema_unaffected_by_cache():
    bm = _make_bm(None)
    info_before = _read_arrow_schema_cached.cache_info()
    restored = pickle.loads(pickle.dumps(bm))
    info_after = _read_arrow_schema_cached.cache_info()
    assert restored.schema is None
    # No cache traffic at all.
    assert info_after.misses == info_before.misses
    assert info_after.hits == info_before.hits


def test_bytearray_schema_payload_is_decoded():
    """If state["schema"] arrives as bytearray (e.g. from an alternative
    serialization path), __setstate__ must still decode it to a pa.Schema —
    not store it raw — and must reuse the LRU-cached entry keyed by the
    equivalent bytes payload."""
    schema = _wide_arrow_schema(20)
    bm = _make_bm(schema)
    state = bm.__getstate__()
    assert isinstance(state["schema"], bytes)

    # Prime the cache via the normal bytes path.
    bytes_restored = pickle.loads(pickle.dumps(bm))
    assert bytes_restored.schema.equals(schema)
    info_after_bytes = _read_arrow_schema_cached.cache_info()

    # Now feed __setstate__ a bytearray with the same contents.
    bytearray_state = dict(state)
    bytearray_state["schema"] = bytearray(state["schema"])
    bytearray_restored = BlockMetadataWithSchema.from_metadata(bm.metadata, schema=None)
    bytearray_restored.__setstate__(bytearray_state)

    # It must be decoded to a real pa.Schema, not stored raw.
    assert isinstance(bytearray_restored.schema, pa.Schema)
    assert bytearray_restored.schema.equals(schema)

    # And it must hit the same cache entry as the bytes path (no extra miss).
    info_after_bytearray = _read_arrow_schema_cached.cache_info()
    assert info_after_bytearray.misses == info_after_bytes.misses
    assert info_after_bytearray.hits == info_after_bytes.hits + 1
    assert bytearray_restored.schema is bytes_restored.schema


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
