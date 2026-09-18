import datetime
import decimal
import pickle
import sys
import time
import types
import uuid
from typing import Optional
from unittest.mock import MagicMock, patch

import numpy as np
import pyarrow as pa
import pytest

from ray.data import Dataset
from ray.data._internal.datasource.qdrant_datasink import QdrantDatasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.tensor_extensions.arrow import ArrowTensorArray


@pytest.fixture(autouse=True)
def mock_qdrant_module():
    fake_module = MagicMock()
    fake_module.models.Batch = lambda **kwargs: types.SimpleNamespace(**kwargs)
    fake_module.models.VectorParams = lambda **kwargs: types.SimpleNamespace(**kwargs)
    fake_module.models.Distance = lambda value: value
    with patch.dict(sys.modules, {"qdrant_client": fake_module}):
        yield fake_module


@pytest.fixture(autouse=True)
def no_retry_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda _: None)


def make_sink(**kwargs) -> QdrantDatasink:
    params = {"collection": "c", "url": "http://localhost:6333", "api_key": "key"}
    params.update(kwargs)
    return QdrantDatasink(**params)


def make_client(collection_exists: bool = True) -> MagicMock:
    client = MagicMock()
    client.collection_exists.return_value = collection_exists
    return client


def write(sink: QdrantDatasink, *tables: pa.Table, client: Optional[MagicMock] = None):
    client = client or make_client()
    with patch.object(sink, "_get_client", return_value=client):
        sink.write(list(tables), TaskContext(task_idx=0, op_name="Write"))
    return client


def upserted(client: MagicMock) -> list:
    return [
        (c.kwargs["collection_name"], c.kwargs["points"])
        for c in client.upsert.call_args_list
    ]


class UnexpectedResponse(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"Unexpected Response: {status_code} (Error)")


class ResponseHandlingException(Exception):
    pass


class RpcError(Exception):
    def __init__(self, code: str):
        super().__init__(f"<RPC terminated with: status = StatusCode.{code}>")


class TestConstructorValidation:
    @pytest.mark.parametrize(
        "kwargs,match",
        [
            ({"collection": None}, "Either 'collection' or 'collection_column'"),
            ({"collection_column": "t"}, "exactly one"),
            ({"url": None}, "'url' must be provided"),
            ({"batch_size": 0}, "batch_size must be positive"),
            ({"distance": "cosine"}, "distance must be one of"),
            ({"id_column": "v", "vector_column": "v"}, "must be distinct"),
            ({"collection": None, "collection_column": "id"}, "must not be the"),
            ({"collection": None, "collection_column": "vector"}, "must not be the"),
        ],
    )
    def test_invalid_arguments_raise(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            make_sink(**kwargs)

    def test_api_key_from_env(self, monkeypatch):
        monkeypatch.setenv("QDRANT_API_KEY", "env-key")
        assert make_sink(api_key=None).api_key == "env-key"

    def test_api_key_is_optional(self, monkeypatch):
        monkeypatch.delenv("QDRANT_API_KEY", raising=False)
        assert make_sink(api_key=None).api_key is None

    def test_client_arguments(self, mock_qdrant_module):
        sink = make_sink(client_kwargs={"prefer_grpc": True})
        sink._get_client()
        mock_qdrant_module.QdrantClient.assert_called_once_with(
            url="http://localhost:6333", api_key="key", prefer_grpc=True
        )


def test_write_qdrant_passes_arguments_to_sink():
    kwargs = dict(
        url="http://localhost:6333",
        collection="c",
        api_key="key",
        id_column="pk",
        vector_column="emb",
        vector_name="dense",
        batch_size=64,
        distance="Dot",
        client_kwargs={"prefer_grpc": True},
    )
    ds = MagicMock()

    Dataset.write_qdrant(ds, ray_remote_args={"num_cpus": 1}, concurrency=2, **kwargs)

    [(args, call_kwargs)] = ds.write_datasink.call_args_list
    assert vars(args[0]) == vars(make_sink(**kwargs))
    assert call_kwargs == {"ray_remote_args": {"num_cpus": 1}, "concurrency": 2}


class TestWrite:
    def test_writes_ids_vectors_and_payloads(self):
        table = pa.table(
            {"id": [1, 2], "vector": [[0.1, 0.2], [0.3, 0.4]], "text": ["a", "b"]}
        )

        client = write(make_sink(), table)

        [(collection, points)] = upserted(client)
        assert collection == "c"
        assert points.ids == [1, 2]
        assert points.vectors == [[0.1, 0.2], [0.3, 0.4]]
        assert points.payloads == [{"text": "a"}, {"text": "b"}]
        client.upsert.assert_called_once_with(
            collection_name="c", points=points, wait=True
        )

    def test_custom_columns_and_named_vector(self):
        table = pa.table({"pk": [1], "emb": [[0.1, 0.2]], "id": ["external"]})
        sink = make_sink(id_column="pk", vector_column="emb", vector_name="dense")

        [(_, points)] = upserted(write(sink, table))

        assert points.ids == [1]
        assert points.vectors == {"dense": [[0.1, 0.2]]}
        assert points.payloads == [{"id": "external"}]

    def test_no_payload_columns(self):
        table = pa.table({"id": [1], "vector": [[1.0]]})
        [(_, points)] = upserted(write(make_sink(), table))
        assert points.payloads is None

    def test_batches_by_batch_size(self):
        table = pa.table({"id": list(range(25)), "vector": [[1.0]] * 25})

        client = write(make_sink(batch_size=10), table)

        assert [len(points.ids) for _, points in upserted(client)] == [10, 10, 5]

    def test_writes_each_block(self):
        blocks = [
            pa.table({"id": [1, 2], "vector": [[1.0], [2.0]]}),
            pa.table({"id": [3], "vector": [[3.0]]}),
        ]

        client = write(make_sink(), *blocks)

        assert [points.ids for _, points in upserted(client)] == [[1, 2], [3]]

    def test_skips_empty_blocks_and_null_ids(self):
        blocks = [
            pa.table(
                {
                    "id": pa.array([], pa.int64()),
                    "vector": pa.array([], pa.list_(pa.float64())),
                }
            ),
            pa.table({"id": pa.array([None], pa.int64()), "vector": [[1.0]]}),
            pa.table({"id": pa.array([1, None], pa.int64()), "vector": [[1.0], [2.0]]}),
        ]

        client = write(make_sink(), *blocks)

        [(_, points)] = upserted(client)
        assert points.ids == [1]

    @pytest.mark.parametrize(
        "table,match",
        [
            (pa.table({"vector": [[1.0]]}), "ID column 'id' not found"),
            (pa.table({"id": [1]}), "Vector column 'vector' not found"),
            (pa.table({"id": [1, 2], "vector": [[1.0], None]}), "contains null values"),
        ],
    )
    def test_missing_or_null_columns_raise(self, table, match):
        with pytest.raises(ValueError, match=match):
            write(make_sink(), table)

    def test_closes_client(self):
        client = write(make_sink(), pa.table({"id": [1], "vector": [[1.0]]}))
        client.close.assert_called_once()

        client = make_client()
        client.upsert.side_effect = UnexpectedResponse(400)
        with pytest.raises(UnexpectedResponse):
            write(make_sink(), pa.table({"id": [1], "vector": [[1.0]]}), client=client)
        client.close.assert_called_once()


class TestPointIds:
    def test_uuid_formats_are_normalized(self):
        u = uuid.uuid4()
        values = [str(u), u.hex, str(u).upper(), "{" + str(u) + "}"]

        assert make_sink()._to_point_ids(pa.chunked_array([values])) == [str(u)] * 4

    def test_uuid_bytes(self):
        u = uuid.uuid4()
        column = pa.chunked_array([pa.array([u.bytes], pa.binary(16))])

        assert make_sink()._to_point_ids(column) == [str(u)]

    @pytest.mark.parametrize(
        "values,match",
        [
            ([1, -1], "negative values"),
            (["not-a-uuid"], "must be unsigned integers or UUIDs"),
            (["123"], "must be unsigned integers or UUIDs"),
            ([1.5], "must be unsigned integers or UUIDs"),
            ([True], "must be unsigned integers or UUIDs"),
            ([b"short"], "must be unsigned integers or UUIDs"),
        ],
    )
    def test_invalid_ids_raise(self, values, match):
        with pytest.raises(ValueError, match=match):
            make_sink()._to_point_ids(pa.chunked_array([values]))


class TestVectors:
    @pytest.mark.parametrize(
        "vectors",
        [
            pa.array([[9.0, 9.0], [1.0, 2.0], [3.0, 4.0]]).slice(1),
            pa.array([[1.0, 2.0], [3.0, 4.0]], pa.list_(pa.float32(), 2)),
            pa.array([[1.0, 2.0], [3.0, 4.0]], pa.large_list(pa.float16())),
            pa.array([[1, 2], [3, 4]]),
            ArrowTensorArray.from_numpy(np.array([[1.0, 2.0], [3.0, 4.0]])),
        ],
        ids=["sliced_list", "fixed_list", "large_list_f16", "ints", "tensor"],
    )
    def test_supported_layouts(self, vectors):
        result = make_sink()._to_vectors(pa.table({"vector": vectors}))

        assert result == [[1.0, 2.0], [3.0, 4.0]]
        assert all(type(v) is float for vector in result for v in vector)

    @pytest.mark.parametrize(
        "vectors,match",
        [
            (pa.array([[1.0, 2.0], [1.0]]), "same length"),
            (pa.array([[]], pa.list_(pa.float32())), "non-empty"),
            (pa.array([["a", "b"]]), "numeric"),
            (pa.array([[True, False]]), "numeric"),
            (pa.array([[[1.0]]]), "one-dimensional"),
            (pa.array([1.0, 2.0]), "one-dimensional"),
            (ArrowTensorArray.from_numpy(np.ones((1, 2, 2))), "one-dimensional"),
            (pa.array([[1.0, None]]), "NaN or infinite"),
            (pa.array([[1.0, float("nan")]]), "NaN or infinite"),
            (pa.array([[float("inf"), 1.0]]), "NaN or infinite"),
            (pa.array([[1e300, 1.0]]), "NaN or infinite"),
        ],
    )
    def test_invalid_vectors_raise(self, vectors, match):
        with pytest.raises(ValueError, match=match):
            make_sink()._to_vectors(pa.table({"vector": vectors}))


class TestPayloads:
    def test_converts_values_to_json(self):
        table = pa.table(
            {
                "id": [1],
                "vector": [[0.1]],
                "i": [1],
                "s": ["x"],
                "b": [True],
                "nan": [float("nan")],
                "nested": [{"a": [1.0, float("nan")]}],
                "date": [datetime.date(2026, 1, 2)],
                "ts": [datetime.datetime(2026, 1, 2, 3, 4, 5, 6)],
                "ts_ns": pa.array([1_700_000_000_123_456_789], pa.timestamp("ns")),
                "time": [datetime.time(1, 2, 3)],
                "tensor": ArrowTensorArray.from_numpy(np.array([[1.0, 2.0]])),
                "map": pa.array([[("k", 1)]], pa.map_(pa.string(), pa.int64())),
                "dict": pa.array(["cat"]).dictionary_encode(),
            }
        )

        assert make_sink()._to_payloads(table) == [
            {
                "i": 1,
                "s": "x",
                "b": True,
                "nan": None,
                "nested": {"a": [1.0, None]},
                "date": "2026-01-02",
                "ts": "2026-01-02T03:04:05.000006",
                "ts_ns": "2023-11-14T22:13:20.123456789",
                "time": "01:02:03",
                "tensor": [1.0, 2.0],
                "map": [["k", 1]],
                "dict": "cat",
            }
        ]

    @pytest.mark.parametrize(
        "values,match",
        [
            ([b"\x00"], "type bytes"),
            ([decimal.Decimal("1.5")], "type Decimal"),
            ([datetime.timedelta(seconds=1)], "type timedelta"),
            ([float("inf")], "infinite values"),
            ([{"a": b"x"}], "type bytes"),
        ],
    )
    def test_unsupported_values_raise(self, values, match):
        table = pa.table({"id": [1], "vector": [[0.1]], "p": values})

        with pytest.raises(ValueError, match=f"Payload column 'p' .*{match}"):
            make_sink()._to_payloads(table)


class TestCollectionCreation:
    def test_creates_missing_collection_before_first_upsert(self):
        client = write(
            make_sink(distance="Dot"),
            pa.table({"id": [1], "vector": [[1.0, 2.0]]}),
            client=make_client(collection_exists=False),
        )

        assert [c[0] for c in client.method_calls] == [
            "collection_exists",
            "create_collection",
            "upsert",
            "close",
        ]
        kwargs = client.create_collection.call_args.kwargs
        assert kwargs["collection_name"] == "c"
        assert kwargs["vectors_config"].size == 2
        assert kwargs["vectors_config"].distance == "Dot"

    def test_named_vector_config(self):
        client = write(
            make_sink(vector_name="dense"),
            pa.table({"id": [1], "vector": [[1.0, 2.0, 3.0]]}),
            client=make_client(collection_exists=False),
        )

        config = client.create_collection.call_args.kwargs["vectors_config"]
        assert config["dense"].size == 3

    def test_existing_collection_is_used_as_is(self):
        client = write(make_sink(), pa.table({"id": [1], "vector": [[1.0]]}))
        client.create_collection.assert_not_called()

    def test_checks_existence_once_per_collection(self):
        table = pa.table({"id": list(range(5)), "vector": [[1.0]] * 5})
        client = write(make_sink(batch_size=1), table, table)
        assert client.collection_exists.call_count == 1

    def test_invalid_data_creates_no_collection(self):
        client = make_client(collection_exists=False)
        table = pa.table({"id": [1], "vector": [[float("nan")]]})

        with pytest.raises(ValueError, match="NaN"):
            write(make_sink(), table, client=client)
        client.create_collection.assert_not_called()

    def test_tolerates_concurrent_creation(self):
        client = make_client()
        client.collection_exists.side_effect = [False, True]
        client.create_collection.side_effect = UnexpectedResponse(409)

        write(make_sink(), pa.table({"id": [1], "vector": [[1.0]]}), client=client)

        client.upsert.assert_called_once()

    def test_create_failure_raises(self):
        client = make_client(collection_exists=False)
        client.create_collection.side_effect = UnexpectedResponse(403)

        with pytest.raises(UnexpectedResponse):
            write(make_sink(), pa.table({"id": [1], "vector": [[1.0]]}), client=client)
        client.create_collection.assert_called_once()


class TestRetries:
    @pytest.mark.parametrize(
        "error",
        [
            ResponseHandlingException("Connection refused"),
            UnexpectedResponse(429),
            UnexpectedResponse(503),
            RpcError("UNAVAILABLE"),
            RpcError("DEADLINE_EXCEEDED"),
        ],
        ids=["connection", "http_429", "http_503", "grpc_unavailable", "grpc_deadline"],
    )
    def test_transient_errors_are_retried(self, error):
        client = make_client()
        client.upsert.side_effect = [error, error, None]

        write(make_sink(), pa.table({"id": [1], "vector": [[1.0]]}), client=client)

        assert client.upsert.call_count == 3

    def test_retries_are_limited(self):
        client = make_client()
        client.upsert.side_effect = UnexpectedResponse(503)

        with pytest.raises(UnexpectedResponse):
            write(make_sink(), pa.table({"id": [1], "vector": [[1.0]]}), client=client)
        assert client.upsert.call_count == 5

    @pytest.mark.parametrize(
        "error",
        [
            UnexpectedResponse(400),
            UnexpectedResponse(401),
            RpcError("INVALID_ARGUMENT"),
            RpcError("UNAUTHENTICATED"),
            ValueError("bad input"),
        ],
        ids=["http_400", "http_401", "grpc_invalid", "grpc_unauthenticated", "value"],
    )
    def test_permanent_errors_are_not_retried(self, error):
        client = make_client()
        client.upsert.side_effect = error

        with pytest.raises(type(error)):
            write(make_sink(), pa.table({"id": [1], "vector": [[1.0]]}), client=client)
        client.upsert.assert_called_once()

    def test_collection_check_is_retried(self):
        client = make_client()
        client.collection_exists.side_effect = [RpcError("UNAVAILABLE"), True]

        write(make_sink(), pa.table({"id": [1], "vector": [[1.0]]}), client=client)

        assert client.collection_exists.call_count == 2
        client.upsert.assert_called_once()


class TestMultiCollectionWrites:
    @pytest.mark.parametrize(
        "tenants",
        [
            pa.array(["b", "a", "b"]),
            pa.array(["b", "a", "b"]).dictionary_encode(),
            pa.array(["b", "a", "b"], pa.large_string()),
        ],
        ids=["string", "dictionary", "large_string"],
    )
    def test_routes_rows_and_drops_column(self, tenants):
        table = pa.table(
            {"tenant": tenants, "id": [1, 2, 3], "vector": [[1.0]] * 3, "x": [1, 2, 3]}
        )

        client = write(make_sink(collection=None, collection_column="tenant"), table)

        writes = dict(upserted(client))
        assert writes["a"].ids == [2]
        assert writes["b"].ids == [1, 3]
        assert writes["b"].payloads == [{"x": 1}, {"x": 3}]

    def test_integer_collection_names_become_strings(self):
        table = pa.table({"tenant": [7], "id": [1], "vector": [[1.0]]})

        client = write(make_sink(collection=None, collection_column="tenant"), table)

        assert [collection for collection, _ in upserted(client)] == ["7"]

    @pytest.mark.parametrize(
        "table,match",
        [
            (
                pa.table({"id": [1], "vector": [[1.0]]}),
                "Collection column 'tenant' not found",
            ),
            (
                pa.table(
                    {
                        "tenant": pa.array([None], pa.string()),
                        "id": [1],
                        "vector": [[1.0]],
                    }
                ),
                "contains null values",
            ),
        ],
    )
    def test_invalid_collection_column_raises(self, table, match):
        with pytest.raises(ValueError, match=match):
            write(make_sink(collection=None, collection_column="tenant"), table)


class TestSerialization:
    def test_pickle_round_trip(self):
        sink = make_sink(
            collection=None,
            collection_column="tenant",
            vector_name="dense",
            batch_size=64,
            distance="Dot",
            client_kwargs={"prefer_grpc": True},
        )

        unpickled = pickle.loads(pickle.dumps(sink))

        assert vars(unpickled) == vars(sink)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
