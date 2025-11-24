import sys
import uuid
from typing import List

import pyarrow as pa
import pytest
from unittest.mock import MagicMock, patch

from ray.data._internal.datasource.turbopuffer_datasink import TurbopufferDatasink


@pytest.fixture(autouse=True)
def mock_turbopuffer_module(monkeypatch):
    """Provide a fake turbopuffer module so imports in the datasink succeed."""
    fake_module = MagicMock()
    fake_module.Turbopuffer = MagicMock()
    with patch.dict(sys.modules, {"turbopuffer": fake_module}):
        yield fake_module


def make_sink(**kwargs) -> TurbopufferDatasink:
    """Helper to construct a sink with minimal required arguments."""
    params = {
        "namespace": "default_ns",
        "api_key": "test-api-key",
    }
    params.update(kwargs)
    return TurbopufferDatasink(**params)


### 1. Constructor and configuration validation


def test_init_requires_either_namespace_or_namespace_column():
    # Neither namespace nor namespace_column provided.
    with pytest.raises(ValueError):
        TurbopufferDatasink(api_key="k")

    # Both namespace and namespace_column provided.
    with pytest.raises(ValueError):
        TurbopufferDatasink(
            namespace="ns",
            namespace_column="space_id",
            api_key="k",
        )


def test_init_requires_namespace_format_placeholder_when_using_namespace_column():
    with pytest.raises(ValueError):
        TurbopufferDatasink(
            namespace_column="space_id",
            namespace_format="space-{id}",
            api_key="k",
        )


def test_init_requires_api_key(monkeypatch):
    # Ensure env var is not set.
    monkeypatch.delenv("TURBOPUFFER_API_KEY", raising=False)

    # No api_key and no env var -> error.
    with pytest.raises(ValueError):
        TurbopufferDatasink(namespace="ns")

    # With env var, init should succeed.
    monkeypatch.setenv("TURBOPUFFER_API_KEY", "env-api-key")
    sink = TurbopufferDatasink(namespace="ns")
    assert sink.api_key == "env-api-key"


### 2. Turbopuffer client initialization


def test_get_client_lazy_initialization_and_region_default(mock_turbopuffer_module):
    sink = make_sink()

    client1 = sink._get_client()
    client2 = sink._get_client()

    # Client is cached.
    assert client1 is client2
    mock_turbopuffer_module.Turbopuffer.assert_called_once_with(
        api_key="test-api-key",
        region="gcp-us-central1",
    )


def test_get_client_uses_explicit_region(mock_turbopuffer_module):
    sink = make_sink(region="custom-region")

    client = sink._get_client()
    assert client is not None
    mock_turbopuffer_module.Turbopuffer.assert_called_once_with(
        api_key="test-api-key",
        region="custom-region",
    )


### 3. Arrow table preparation


def test_prepare_arrow_table_renames_id_and_vector_and_filters_null_ids():
    table = pa.table(
        {
            "doc_id": [1, 2, None],
            "emb": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
            "other": ["a", "b", "c"],
        }
    )
    sink = make_sink(id_column="doc_id", vector_column="emb")

    prepared = sink._prepare_arrow_table(table)

    assert "id" in prepared.column_names
    assert "vector" in prepared.column_names
    assert "doc_id" not in prepared.column_names
    assert "emb" not in prepared.column_names
    # One row had a null id; it should be filtered out.
    assert prepared.num_rows == 2


def test_prepare_arrow_table_missing_custom_id_column_raises():
    table = pa.table({"other": [1, 2, 3]})
    sink = make_sink(id_column="doc_id")

    with pytest.raises(ValueError):
        sink._prepare_arrow_table(table)


def test_prepare_arrow_table_raises_on_conflicting_id_column_name():
    # Table already has an 'id' column and a separate custom id column.
    table = pa.table({"id": [1, 2], "doc_id": [10, 20]})
    sink = make_sink(id_column="doc_id")

    with pytest.raises(ValueError, match="already has an 'id' column"):
        sink._prepare_arrow_table(table)


def test_prepare_arrow_table_raises_on_conflicting_vector_column_name():
    # Table already has a 'vector' column and a separate custom vector column.
    table = pa.table(
        {
            "id": [1, 2],
            "vector": [[0.1], [0.2]],
            "emb": [[0.3], [0.4]],
        }
    )
    sink = make_sink(vector_column="emb")

    with pytest.raises(ValueError, match="already has a 'vector' column"):
        sink._prepare_arrow_table(table)


### 4. Multi-namespace behavior


def test_write_multi_namespace_groups_by_column_and_formats_names():
    table = pa.table(
        {
            "space_id": ["a", "b", "a"],
            "id": [1, 2, 3],
            "vector": [[1.0], [2.0], [3.0]],
        }
    )
    sink = make_sink(
        namespace=None,
        namespace_column="space_id",
        namespace_format="ns-{namespace}",
    )
    client = MagicMock()

    with patch.object(sink, "_write_single_namespace") as mock_write_single:
        sink._write_multi_namespace(client, table)

    assert mock_write_single.call_count == 2

    seen_namespaces = set()
    rows_per_namespace = {}

    for call in mock_write_single.call_args_list:
        client_arg, group_table, ns_name = call.args
        assert client_arg is client
        seen_namespaces.add(ns_name)
        rows_per_namespace[ns_name] = group_table.num_rows

    assert seen_namespaces == {"ns-a", "ns-b"}
    # space_id == "a" appears twice, "b" once.
    assert rows_per_namespace["ns-a"] == 2
    assert rows_per_namespace["ns-b"] == 1


def test_write_multi_namespace_converts_uuid_bytes_to_string():
    u1 = uuid.uuid4()
    u2 = uuid.uuid4()
    table = pa.table(
        {
            "space_id": [u1.bytes, u2.bytes, u1.bytes],
            "id": [1, 2, 3],
            "vector": [[1.0], [2.0], [3.0]],
        }
    )
    sink = make_sink(
        namespace=None,
        namespace_column="space_id",
        namespace_format="ns-{namespace}",
    )
    client = MagicMock()

    with patch.object(sink, "_write_single_namespace") as mock_write_single:
        sink._write_multi_namespace(client, table)

    ns_names = {call.args[2] for call in mock_write_single.call_args_list}
    expected = {f"ns-{str(u1)}", f"ns-{str(u2)}"}
    assert ns_names == expected


def test_write_multi_namespace_missing_column_raises():
    table = pa.table({"id": [1, 2], "vector": [[1.0], [2.0]]})
    sink = make_sink(
        namespace=None,
        namespace_column="space_id",
        namespace_format="ns-{namespace}",
    )
    client = MagicMock()

    with pytest.raises(ValueError):
        sink._write_multi_namespace(client, table)


def test_write_multi_namespace_raises_on_null_namespace_values():
    # When the namespace column contains nulls, we should raise a clear error
    # instead of silently dropping those rows.
    table = pa.table(
        {
            "space_id": ["a", None, "b"],
            "id": [1, 2, 3],
            "vector": [[1.0], [2.0], [3.0]],
        }
    )
    sink = make_sink(
        namespace=None,
        namespace_column="space_id",
        namespace_format="ns-{namespace}",
    )
    client = MagicMock()

    with pytest.raises(ValueError, match="contains null values"):
        sink._write_multi_namespace(client, table)


### 5. Single-namespace batching and write flow


def test_write_single_namespace_batches_by_batch_size():
    num_rows = 25
    table = pa.table(
        {
            "id": list(range(num_rows)),
            "vector": [[float(i)] for i in range(num_rows)],
        }
    )
    sink = make_sink(batch_size=10)
    client = MagicMock()
    namespace_obj = MagicMock()
    client.namespace.return_value = namespace_obj

    batch_sizes: List[int] = []

    def fake_transform(batch_table: pa.Table):
        # Return one dict per row so we can infer batch size from upsert_rows.
        return [
            {"id": int(row["id"]), "vector": row["vector"]}
            for row in batch_table.to_pylist()
        ]

    def fake_write_batch(ns, batch_data, namespace_name):
        assert ns is namespace_obj
        batch_sizes.append(len(batch_data))

    with patch.object(
        sink, "_transform_to_turbopuffer_format", side_effect=fake_transform
    ), patch.object(
        sink, "_write_batch_with_retry", side_effect=fake_write_batch
    ):
        sink._write_single_namespace(client, table, "ns")

    assert batch_sizes == [10, 10, 5]


def test_write_skips_on_empty_blocks_or_tables():
    sink = make_sink()

    # No blocks at all: client still gets initialized once.
    with patch.object(sink, "_get_client") as mock_get_client:
        sink.write([], ctx=None)
    mock_get_client.assert_called_once()

    # Blocks whose concatenated table has 0 rows: no namespace writes.
    empty_table = pa.table({"id": [], "vector": []})
    with patch.object(sink, "_get_client") as mock_get_client, patch.object(
        sink, "_write_single_namespace"
    ) as mock_single, patch.object(sink, "_write_multi_namespace") as mock_multi:
        mock_get_client.return_value = MagicMock()
        sink.write([empty_table], ctx=None)

    mock_single.assert_not_called()
    mock_multi.assert_not_called()


### 6. Transform-to-Turbopuffer-row behavior


def test_transform_to_turbopuffer_format_requires_id_column():
    table = pa.table({"col": [1, 2, 3]})
    sink = make_sink()

    with pytest.raises(ValueError):
        sink._transform_to_turbopuffer_format(table)


def test_transform_converts_uuid_bytes_and_hex_and_lists():
    u = uuid.uuid4()
    uuid_bytes = u.bytes
    other_bytes = b"\x01\x02\x03"

    table = pa.table(
        {
            "id": [1],
            "uuid_col": [uuid_bytes],
            "bytes_col": [other_bytes],
            "list_col": [[uuid_bytes, other_bytes]],
        }
    )

    sink = make_sink()
    rows = sink._transform_to_turbopuffer_format(table)
    assert len(rows) == 1
    row = rows[0]

    assert row["uuid_col"] == str(u)
    assert row["bytes_col"] == other_bytes.hex()
    assert row["list_col"][0] == str(u)
    # Non-UUID bytes inside lists should also be converted to hex for consistency.
    assert row["list_col"][1] == other_bytes.hex()


### 7. Retry logic and backoff


def test_write_batch_with_retry_success_first_try(monkeypatch):
    sink = make_sink(schema={"field": "value"})
    namespace = MagicMock()
    batch_data = [{"id": 1}]

    # Make sleep and randomness deterministic/no-op.
    monkeypatch.setattr(
        "ray.data._internal.datasource.turbopuffer_datasink.time.sleep", lambda _: None
    )
    monkeypatch.setattr(
        "ray.data._internal.datasource.turbopuffer_datasink.random.uniform",
        lambda a, b: 0.0,
    )

    sink._write_batch_with_retry(namespace, batch_data, "ns")

    namespace.write.assert_called_once_with(
        upsert_rows=batch_data,
        schema={"field": "value"},
        distance_metric="cosine_distance",
    )


def test_write_batch_with_retry_retries_then_succeeds(monkeypatch):
    sink = make_sink()
    namespace = MagicMock()
    batch_data = [{"id": 1}]

    attempts = {"count": 0}

    def flaky_write(*args, **kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("temporary error")

    namespace.write.side_effect = flaky_write

    sleep_calls = []

    def fake_sleep(duration):
        sleep_calls.append(duration)

    monkeypatch.setattr(
        "ray.data._internal.datasource.turbopuffer_datasink.time.sleep", fake_sleep
    )
    monkeypatch.setattr(
        "ray.data._internal.datasource.turbopuffer_datasink.random.uniform",
        lambda a, b: 0.0,
    )

    sink._write_batch_with_retry(namespace, batch_data, "ns")

    assert attempts["count"] == 3
    # Two failures => two sleeps.
    assert len(sleep_calls) == 2


def test_write_batch_with_retry_exhausts_retries_and_raises(monkeypatch):
    sink = make_sink()
    namespace = MagicMock()
    batch_data = [{"id": 1}]

    namespace.write.side_effect = RuntimeError("persistent error")

    sleep_calls = []

    def fake_sleep(duration):
        sleep_calls.append(duration)

    monkeypatch.setattr(
        "ray.data._internal.datasource.turbopuffer_datasink.time.sleep", fake_sleep
    )
    monkeypatch.setattr(
        "ray.data._internal.datasource.turbopuffer_datasink.random.uniform",
        lambda a, b: 0.0,
    )

    with pytest.raises(RuntimeError):
        sink._write_batch_with_retry(namespace, batch_data, "ns")

    # MAX_RETRIES is 5, sleeps occur on all but last attempt => 4 sleeps.
    assert len(sleep_calls) == 4


### 8. Top-level write orchestration


def test_write_single_namespace_end_to_end_calls_prepare_and_single_namespace():
    sink = make_sink()
    block1 = pa.table({"id": [1, 2], "vector": [[1.0], [2.0]]})
    block2 = pa.table({"id": [3], "vector": [[3.0]]})

    with patch.object(sink, "_get_client") as mock_get_client, patch.object(
        sink, "_prepare_arrow_table", wraps=sink._prepare_arrow_table
    ) as mock_prepare, patch.object(sink, "_write_single_namespace") as mock_single, patch.object(
        sink, "_write_multi_namespace"
    ) as mock_multi:
        mock_get_client.return_value = MagicMock()
        sink.write([block1, block2], ctx=None)

    # One concatenated table of 3 rows should be prepared.
    assert mock_prepare.call_count == 1
    prepared_input = mock_prepare.call_args[0][0]
    assert prepared_input.num_rows == 3

    # Single-namespace path should be taken.
    mock_multi.assert_not_called()
    mock_single.assert_called_once()

    client_arg, prepared_table_arg, ns_name = mock_single.call_args[0]
    assert ns_name == "default_ns"
    assert prepared_table_arg.num_rows == 3
    assert client_arg is mock_get_client.return_value


def test_write_multi_namespace_end_to_end_uses_write_multi_namespace():
    sink = TurbopufferDatasink(
        namespace=None,
        namespace_column="space_id",
        namespace_format="ns-{namespace}",
        api_key="test-api-key",
    )
    block1 = pa.table({"space_id": ["a"], "id": [1], "vector": [[1.0]]})
    block2 = pa.table({"space_id": ["b"], "id": [2], "vector": [[2.0]]})

    with patch.object(sink, "_get_client") as mock_get_client, patch.object(
        sink, "_prepare_arrow_table", wraps=sink._prepare_arrow_table
    ) as mock_prepare, patch.object(
        sink, "_write_multi_namespace"
    ) as mock_multi, patch.object(
        sink, "_write_single_namespace"
    ) as mock_single:
        mock_get_client.return_value = MagicMock()
        sink.write([block1, block2], ctx=None)

    # Prepared once with both blocks concatenated.
    assert mock_prepare.call_count == 1
    prepared_input = mock_prepare.call_args[0][0]
    assert prepared_input.num_rows == 2

    mock_single.assert_not_called()
    mock_multi.assert_called_once()

    client_arg, prepared_table_arg = mock_multi.call_args[0]
    assert prepared_table_arg.num_rows == 2
    assert client_arg is mock_get_client.return_value



