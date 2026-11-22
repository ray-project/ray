import sys
from types import ModuleType
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from ray.data._internal.datasource.mongo_datasink import MongoDatasink
from ray.data._internal.datasource.mongo_datasource import MongoDatasource
from ray.data._internal.execution.interfaces import TaskContext


def test_mongo_datasink_reuses_and_closes_client_and_skips_empty_blocks(monkeypatch):
    created_clients = []
    write_calls = []

    class FakeDatabase:
        def __init__(self, collections):
            self._collections = collections

        def list_collection_names(self):
            return list(self._collections)

        def __getitem__(self, name):
            return f"collection:{name}"

    class FakeMongoClient:
        def __init__(self, uri):
            self.uri = uri
            self.closed = False
            created_clients.append(self)

        def list_database_names(self):
            return ["test_db"]

        def __getitem__(self, db_name):
            return FakeDatabase(["test_coll"])

        def close(self):
            self.closed = True

    fake_pymongo = ModuleType("pymongo")
    fake_pymongo.MongoClient = FakeMongoClient
    fake_pymongoarrow = ModuleType("pymongoarrow")
    fake_pymongoarrow_api = ModuleType("pymongoarrow.api")
    fake_pymongoarrow_api.write = lambda coll, tbl: write_calls.append((coll, tbl))
    fake_pymongoarrow.api = fake_pymongoarrow_api

    monkeypatch.setitem(sys.modules, "pymongo", fake_pymongo)
    monkeypatch.setitem(sys.modules, "pymongoarrow", fake_pymongoarrow)
    monkeypatch.setitem(sys.modules, "pymongoarrow.api", fake_pymongoarrow_api)

    ctx = MagicMock(spec=TaskContext)
    sink = MongoDatasink("mongodb://example:27017", "test_db", "test_coll")

    # 1. Non-empty write reuses a single MongoClient and closes it.
    non_empty_block = pa.table({"x": [1, 2, 3]})
    sink.write([non_empty_block], ctx)
    assert len(created_clients) == 1
    assert created_clients[0].closed is True
    assert len(write_calls) == 1
    assert write_calls[0][0] == "collection:test_coll"
    assert write_calls[0][1].num_rows == 3

    # 2. Empty block skips pymongoarrow.api.write and still closes client.
    empty_block = pa.table({"x": pa.array([], type=pa.int64())})
    sink.write([empty_block], ctx)
    assert len(created_clients) == 2
    assert created_clients[1].closed is True
    assert len(write_calls) == 1

    # 3. Validation failure still closes the client.
    bad_sink = MongoDatasink("mongodb://example:27017", "missing_db", "test_coll")
    with pytest.raises(ValueError, match="doesn't exist"):
        bad_sink.write([non_empty_block], ctx)
    assert len(created_clients) == 3
    assert created_clients[2].closed is True


def test_mongo_datasource_read_task_closes_client(monkeypatch):
    created_clients = []

    class FakeCollection:
        def aggregate(self, pipeline, allowDiskUse=False):
            return [{"_id": {"min": "a", "max": "z"}, "count": 2}]

    class FakeDatabase:
        def list_collection_names(self):
            return ["test_coll"]

        def command(self, cmd, coll):
            return {"avgObjSize": 64}

        def __getitem__(self, name):
            return FakeCollection()

    class FakeMongoClient:
        def __init__(self, uri):
            self.uri = uri
            self.closed = False
            created_clients.append(self)

        def list_database_names(self):
            return ["test_db"]

        def __getitem__(self, db_name):
            return FakeDatabase()

        def close(self):
            self.closed = True

    fake_pymongo = ModuleType("pymongo")
    fake_pymongo.MongoClient = FakeMongoClient
    fake_bson = ModuleType("bson")
    fake_bson_objectid = ModuleType("bson.objectid")
    fake_bson_objectid.ObjectId = str
    fake_bson.objectid = fake_bson_objectid
    fake_pymongoarrow = ModuleType("pymongoarrow")
    fake_pymongoarrow_api = ModuleType("pymongoarrow.api")
    expected_table = pa.table({"x": [10, 20]})
    fake_pymongoarrow_api.aggregate_arrow_all = (
        lambda coll, pipeline, schema=None, **kw: expected_table
    )
    fake_pymongoarrow.api = fake_pymongoarrow_api

    monkeypatch.setitem(sys.modules, "pymongo", fake_pymongo)
    monkeypatch.setitem(sys.modules, "bson", fake_pymongo)
    monkeypatch.setitem(sys.modules, "bson.objectid", fake_bson_objectid)
    monkeypatch.setitem(sys.modules, "pymongoarrow", fake_pymongoarrow)
    monkeypatch.setitem(sys.modules, "pymongoarrow.api", fake_pymongoarrow_api)

    ds = MongoDatasource(
        uri="mongodb://example:27017",
        database="test_db",
        collection="test_coll",
    )
    read_tasks = ds.get_read_tasks(parallelism=1)
    assert len(read_tasks) == 1
    assert len(created_clients) == 1

    blocks = list(read_tasks[0]())
    assert len(blocks) == 1
    assert blocks[0].equals(expected_table)
    assert len(created_clients) == 2
    assert created_clients[1].closed is True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
