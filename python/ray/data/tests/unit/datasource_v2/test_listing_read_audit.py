"""The read observer must not own fragments or break same-fragment retries."""
import gc
import json
import weakref
from functools import partial
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from ray.data._internal.datasource_v2.common.file_reader import FileReader
from ray.data._internal.datasource_v2.formats.parquet import parquet_file_reader
from ray.data._internal.datasource_v2.interfaces.read_units import (
    ReadUnit,
    ReadUnitFragment,
)
from ray.data._internal.util import iterate_with_retry
from ray.data.expressions import col
from ray.data.tests.datasource_v2.test_listing_pruning import _install_read_audit


@pytest.fixture
def audit(tmp_path, monkeypatch):
    (tmp_path / "label").write_text("lifetime")
    tokens = [object(), object()]
    error = RuntimeError("audit-lifetime-sentinel")
    state = {"mode": "success", "calls": 0, "closed": 0}

    def fragments(fragment, row_group_ids, **kwargs):
        return [ReadUnitFragment(fragment, ReadUnit("unit", "file.parquet"))]

    def native(self, fragment_to_read, scanner_kwargs):
        state["calls"] += 1
        assert scanner_kwargs is options
        try:
            if state["mode"] == "before":
                raise error
            yield tokens[0]
            if state["mode"] == "after" or (
                state["mode"] == "retry" and state["calls"] == 1
            ):
                raise error
            yield tokens[1]
        finally:
            state["closed"] += 1

    monkeypatch.setattr(parquet_file_reader, "_fragments_from_row_group_ids", fragments)
    monkeypatch.setattr(FileReader, "_iter_unit_tables", native)
    _install_read_audit(str(tmp_path))
    options = {"batch_size": 1}
    predicate = col("x") == 1
    reader = FileReader(ds.ParquetFileFormat(), predicate=predicate)

    def make_item():
        fragment = next(ds.dataset(pa.table({"x": [1]})).get_fragments())
        return parquet_file_reader._fragments_from_row_group_ids(
            fragment, [1, 3], per_row_group_offsets=False, file_size=None
        )[0]

    def read(item):
        return FileReader._iter_unit_tables(reader, item, options)

    def events():
        return [
            json.loads(line)
            for path in tmp_path.glob("*.jsonl")
            for line in path.read_text().splitlines()
        ]

    yield SimpleNamespace(
        state=state,
        error=error,
        tokens=tokens,
        make_item=make_item,
        read=read,
        events=events,
        directory=tmp_path,
        predicate=str(predicate),
    )
    error.__traceback__ = None


def test_audit_success_preserves_values_and_events(audit):
    item = audit.make_item()
    ref = weakref.ref(item)
    assert list(audit.read(item)) == audit.tokens
    records = audit.events()
    assert [r["event"] for r in records] == ["start", "complete"]
    assert all(r["groups"] == [1, 3] and r["q"] == audit.predicate for r in records)
    assert audit.state["closed"] == 1
    del item
    gc.collect()
    assert ref() is None


@pytest.mark.parametrize("mode", ["before", "after"])
def test_audit_failed_read_releases_fragment_without_complete(audit, mode):
    audit.state["mode"] = mode
    item = audit.make_item()
    ref = weakref.ref(item)
    with pytest.raises(RuntimeError) as caught:
        list(audit.read(item))
    assert caught.value is audit.error
    assert [r["event"] for r in audit.events()] == ["start"]
    audit.error.__traceback__ = None
    del caught, item
    gc.collect()
    assert ref() is None


@pytest.mark.parametrize("started", [False, True])
def test_audit_closed_generator_releases_fragment(audit, started):
    item = audit.make_item()
    ref = weakref.ref(item)
    iterator = audit.read(item)
    if started:
        assert next(iterator) is audit.tokens[0]
    iterator.close()
    assert [r["event"] for r in audit.events()] == (["start"] if started else [])
    assert audit.state["closed"] == int(started)
    del iterator, item
    gc.collect()
    assert ref() is None


def test_audit_unread_fragment_is_not_owned(audit):
    item = audit.make_item()
    ref = weakref.ref(item)
    del item
    gc.collect()
    assert ref() is None
    assert audit.events() == []


def test_audit_preserves_retry_of_same_fragment(audit, monkeypatch):
    sleeps = []
    monkeypatch.setattr("ray.data._internal.util.time.sleep", sleeps.append)
    audit.state["mode"] = "retry"
    item = audit.make_item()
    ref = weakref.ref(item)
    result = list(
        iterate_with_retry(
            partial(audit.read, item),
            "audit retry",
            match=["audit-lifetime-sentinel"],
            max_attempts=2,
            max_backoff_s=0,
        )
    )
    assert result == audit.tokens
    assert sleeps == [0]
    assert audit.state["calls"] == 2
    assert [r["event"] for r in audit.events()] == ["start", "start", "complete"]
    audit.error.__traceback__ = None
    del item
    gc.collect()
    assert ref() is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
