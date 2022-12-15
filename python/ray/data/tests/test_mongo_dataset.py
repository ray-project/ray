import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.datasource import MongoDatasource

from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa

# To run tests locally, make sure you install mongodb
# and start a local service:
# sudo apt-get install -y mongodb
# sudo service mongodb start


@pytest.fixture
def start_mongo():
    import pymongo

    mongo_url = "mongodb://localhost:27017"
    client = pymongo.MongoClient(mongo_url)
    # Make sure a clean slate for each test by dropping
    # previously created ones (if any).
    for db in client.list_database_names():
        # Keep the MongoDB default databases.
        if db not in ("admin", "local", "config"):
            client.drop_database(db)
    return client, mongo_url


def test_read_write_mongo(ray_start_regular_shared, start_mongo):
    from pymongoarrow.api import Schema
    from pymongo.errors import ServerSelectionTimeoutError

    client, mongo_url = start_mongo
    foo_db = "foo-db"
    foo_collection = "foo-collection"
    foo = client[foo_db][foo_collection]
    foo.delete_many({})

    # Read nonexistent URI.
    with pytest.raises(ServerSelectionTimeoutError):
        ds = ray.data.read_mongo(
            uri="nonexistent-uri",
            database=foo_db,
            collection=foo_collection,
        )
    # Read nonexistent database.
    with pytest.raises(ValueError):
        ds = ray.data.read_mongo(
            uri=mongo_url,
            database="nonexistent-db",
            collection=foo_collection,
        )
    # Read nonexistent collection.
    with pytest.raises(ValueError):
        ds = ray.data.read_mongo(
            uri=mongo_url,
            database=foo_db,
            collection="nonexistent-collection",
        )

    # Inject 5 test docs.
    docs = [{"float_field": 2.0 * val, "int_field": val} for val in range(5)]
    df = pd.DataFrame(docs).astype({"int_field": "int32"})
    foo.insert_many(docs)

    # Read a non-empty database, with schema specified.
    schema = Schema({"float_field": pa.float64(), "int_field": pa.int32()})
    ds = ray.data.read_mongo(
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
        schema=schema,
        parallelism=2,
    )
    assert ds._block_num_rows() == [3, 2]
    assert str(ds) == (
        "Dataset(num_blocks=2, num_rows=5, "
        "schema={float_field: double, int_field: int32})"
    )
    assert df.equals(ds.to_pandas())

    # Read with schema inference, which will read all columns (including the auto
    # generated internal column "_id").
    ds = ray.data.read_mongo(
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
        parallelism=2,
    )
    assert ds._block_num_rows() == [3, 2]
    assert str(ds) == (
        "Dataset(num_blocks=2, num_rows=5, schema={_id: fixed_size_binary[12], "
        "float_field: double, int_field: int32})"
    )
    assert df.equals(ds.drop_columns(["_id"]).to_pandas())

    # Read a subset of the collection.
    ds = ray.data.read_mongo(
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
        pipeline=[{"$match": {"int_field": {"$gte": 0, "$lt": 3}}}],
        parallelism=2,
    )
    assert ds._block_num_rows() == [2, 1]
    assert str(ds) == (
        "Dataset(num_blocks=2, num_rows=3, schema={_id: fixed_size_binary[12], "
        "float_field: double, int_field: int32})"
    )
    df[df["int_field"] < 3].equals(ds.drop_columns(["_id"]).to_pandas())

    # Read with auto-tuned parallelism.
    ds = ray.data.read_mongo(
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
    )
    assert str(ds) == (
        "Dataset(num_blocks=5, num_rows=5, schema={_id: fixed_size_binary[12], "
        "float_field: double, int_field: int32})"
    )
    assert df.equals(ds.drop_columns(["_id"]).to_pandas())

    # Read with a parallelism larger than number of rows.
    ds = ray.data.read_mongo(
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
        parallelism=1000,
    )
    assert str(ds) == (
        "Dataset(num_blocks=5, num_rows=5, schema={_id: fixed_size_binary[12], "
        "float_field: double, int_field: int32})"
    )
    assert df.equals(ds.drop_columns(["_id"]).to_pandas())

    # Add a column and then write back to MongoDB.
    # Inject 2 more test docs.
    new_docs = [{"float_field": 2.0 * val, "int_field": val} for val in range(5, 7)]
    new_df = pd.DataFrame(new_docs).astype({"int_field": "int32"})
    ds2 = ray.data.from_pandas(new_df)
    ds2.write_mongo(uri=mongo_url, database=foo_db, collection=foo_collection)

    # Read again to verify the content.
    expected_ds = ds.drop_columns(["_id"]).union(ds2)
    ds3 = ray.data.read_mongo(
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
    )
    ds3.drop_columns(["_id"]).to_pandas().equals(expected_ds.to_pandas())

    # Destination database doesn't exist.
    with pytest.raises(ValueError):
        ray.data.range(10).write_mongo(
            uri=mongo_url, database="nonexistent-db", collection=foo_collection
        )
    # Destination collection doesn't exist.
    with pytest.raises(ValueError):
        ray.data.range(10).write_mongo(
            uri=mongo_url, database=foo_db, collection="nonexistent-collection"
        )


def test_mongo_datasource(ray_start_regular_shared, start_mongo):
    from pymongoarrow.api import Schema

    client, mongo_url = start_mongo
    foo_db = "foo-db"
    foo_collection = "foo-collection"
    foo = client[foo_db][foo_collection]
    foo.delete_many({})

    # Inject 5 test docs.
    docs = [{"float_field": 2.0 * key, "int_field": key} for key in range(5)]
    df = pd.DataFrame(docs).astype({"int_field": "int32"})
    foo.insert_many(docs)

    # Read non-empty datasource with a specified schema.
    schema = Schema({"float_field": pa.float64(), "int_field": pa.int32()})
    ds = ray.data.read_datasource(
        MongoDatasource(),
        parallelism=2,
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
        schema=schema,
    ).fully_executed()
    assert ds._block_num_rows() == [3, 2]
    assert str(ds) == (
        "Dataset(num_blocks=2, num_rows=5, "
        "schema={float_field: double, int_field: int32})"
    )
    assert df.equals(ds.to_pandas())

    # Read with schema inference, which will read all columns (including the auto
    # generated internal column "_id").
    ds = ray.data.read_datasource(
        MongoDatasource(),
        parallelism=2,
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
    ).fully_executed()
    assert ds._block_num_rows() == [3, 2]
    assert str(ds) == (
        "Dataset(num_blocks=2, num_rows=5, "
        "schema={_id: fixed_size_binary[12], float_field: double, int_field: int32})"
    )
    assert df.equals(ds.drop_columns(["_id"]).to_pandas())

    # Read with auto-tuned parallelism.
    ds = ray.data.read_datasource(
        MongoDatasource(),
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
    ).fully_executed()
    assert str(ds) == (
        "Dataset(num_blocks=5, num_rows=5, schema={_id: fixed_size_binary[12], "
        "float_field: double, int_field: int32})"
    )
    assert df.equals(ds.drop_columns(["_id"]).to_pandas())

    # Read with a parallelism larger than number of rows.
    ds = ray.data.read_datasource(
        MongoDatasource(),
        parallelism=1000,
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
    )
    assert str(ds) == (
        "Dataset(num_blocks=5, num_rows=5, schema={_id: fixed_size_binary[12], "
        "float_field: double, int_field: int32})"
    )
    assert df.equals(ds.drop_columns(["_id"]).to_pandas())

    # Read a subset of the collection.
    ds = ray.data.read_datasource(
        MongoDatasource(),
        parallelism=2,
        uri=mongo_url,
        database=foo_db,
        collection=foo_collection,
        pipeline=[{"$match": {"int_field": {"$gte": 0, "$lt": 3}}}],
    )
    assert ds._block_num_rows() == [2, 1]
    assert str(ds) == (
        "Dataset(num_blocks=2, num_rows=3, schema={_id: fixed_size_binary[12], "
        "float_field: double, int_field: int32})"
    )
    df[df["int_field"] < 3].equals(ds.drop_columns(["_id"]).to_pandas())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
