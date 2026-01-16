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
        "region": "gcp-us-central1",
        "namespace": "default_ns",
        "api_key": "test-api-key",
    }
    params.update(kwargs)
    return TurbopufferDatasink(**params)


### 1. Constructor and configuration validation


def test_init_requires_either_namespace_or_namespace_column():
    # Neither namespace nor namespace_column provided.
    with pytest.raises(ValueError):
        TurbopufferDatasink(region="gcp-us-central1", api_key="k")

    # Both namespace and namespace_column provided.
    with pytest.raises(ValueError):
        TurbopufferDatasink(
            region="gcp-us-central1",
            namespace="ns",
            namespace_column="space_id",
            api_key="k",
        )


def test_init_requires_namespace_format_placeholder_when_using_namespace_column():
    with pytest.raises(ValueError):
        TurbopufferDatasink(
            region="gcp-us-central1",
            namespace_column="space_id",
            namespace_format="space-{id}",
            api_key="k",
        )


def test_init_requires_api_key(monkeypatch):
    # Ensure env var is not set.
    monkeypatch.delenv("TURBOPUFFER_API_KEY", raising=False)

    # No api_key and no env var -> error.
    with pytest.raises(ValueError):
        TurbopufferDatasink(region="gcp-us-central1", namespace="ns")

    # With env var, init should succeed.
    monkeypatch.setenv("TURBOPUFFER_API_KEY", "env-api-key")
    sink = TurbopufferDatasink(region="gcp-us-central1", namespace="ns")
    assert sink.api_key == "env-api-key"


### 2. Turbopuffer client initialization


def test_get_client_lazy_initialization(mock_turbopuffer_module):
    sink = make_sink(region="gcp-us-central1")

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


def test_init_rejects_same_id_and_vector_column():
    # Using the same column for both IDs and vectors is ambiguous and should
    # be rejected up front with a clear error.
    with pytest.raises(ValueError, match="id_column and vector_column"):
        make_sink(id_column="doc_id", vector_column="doc_id")


def test_prepare_arrow_table_raises_on_conflicting_id_column_name():
    # Table already has an 'id' column and a separate custom id column.
    table = pa.table({"id": [1, 2], "doc_id": [10, 20]})
    sink = make_sink(id_column="doc_id")

    with pytest.raises(ValueError, match="already has.*'id' column"):
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

    with pytest.raises(ValueError, match="already has.*'vector' column"):
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


def test_write_multi_namespace_converts_non_uuid_bytes_to_hex():
    # Non-16-byte bytes should be converted to hex strings for consistency
    # with _transform_to_turbopuffer_format (which also uses .hex())
    bytes1 = b"\x01\x02\x03"
    bytes2 = b"\xde\xad\xbe\xef"
    table = pa.table(
        {
            "space_id": [bytes1, bytes2, bytes1],
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
    # Non-UUID bytes should be converted via .hex(), not str()
    expected = {f"ns-{bytes1.hex()}", f"ns-{bytes2.hex()}"}
    assert ns_names == expected
    # Verify they're clean hex strings, not Python bytes repr like "b'\\x01\\x02\\x03'"
    assert "ns-010203" in ns_names
    assert "ns-deadbeef" in ns_names


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


def test_init_rejects_namespace_column_equal_to_id_column():
    """Test that namespace_column == id_column raises ValueError."""
    with pytest.raises(ValueError, match="cannot be the same as id_column"):
        make_sink(
            namespace=None,
            namespace_column="doc_id",
            id_column="doc_id",
        )


def test_init_rejects_namespace_column_equal_to_vector_column():
    """Test that namespace_column == vector_column raises ValueError."""
    with pytest.raises(ValueError, match="cannot be the same as vector_column"):
        make_sink(
            namespace=None,
            namespace_column="embedding",
            vector_column="embedding",
        )


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
        # Return column-based dict format for upsert_columns
        return batch_table.to_pydict()

    def fake_write_batch(ns, batch_data, namespace_name):
        assert ns is namespace_obj
        # Get row count from first column in the dict
        num_rows = len(next(iter(batch_data.values())))
        batch_sizes.append(num_rows)

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


def test_transform_converts_id_uuid_bytes_to_native_uuid():
    """ID column with 16-byte UUID should become native uuid.UUID.

    Per Turbopuffer performance docs, native UUID (16 bytes) is more efficient
    than string UUID (36 bytes). Only the ID column is converted; other columns
    are passed through unchanged from to_pydict().
    """
    u = uuid.uuid4()
    other_bytes = b"\x01\x02\x03"

    table = pa.table(
        {
            "id": [u.bytes],
            "vector": [[0.1, 0.2]],
            "other_bytes": [other_bytes],
        }
    )

    sink = make_sink()
    columns = sink._transform_to_turbopuffer_format(table)

    # Should return a dict of columns, not a list of rows
    assert isinstance(columns, dict)
    assert "id" in columns
    assert "vector" in columns
    assert "other_bytes" in columns

    # ID should be native UUID, not string
    assert columns["id"][0] == u
    assert isinstance(columns["id"][0], uuid.UUID)
    # Other bytes columns should remain unchanged (not converted)
    assert columns["other_bytes"][0] == other_bytes
    # Vector should be passed through unchanged
    assert columns["vector"][0] == [0.1, 0.2]


### 7. Retry logic and backoff


def test_write_batch_with_retry_success_first_try(monkeypatch):
    sink = make_sink(schema={"field": "value"})
    namespace = MagicMock()
    batch_data = {"id": [1], "vector": [[0.1]]}

    # No need to mock sleep/randomness since call_with_retry handles it
    # and the first attempt should succeed without retry
    sink._write_batch_with_retry(namespace, batch_data, "ns")

    namespace.write.assert_called_once_with(
        upsert_columns=batch_data,
        schema={"field": "value"},
        distance_metric="cosine_distance",
    )


def test_write_batch_with_retry_retries_then_succeeds(monkeypatch):
    sink = make_sink()
    namespace = MagicMock()
    batch_data = {"id": [1], "vector": [[0.1]]}

    attempts = {"count": 0}

    def flaky_write(*args, **kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("temporary error")

    namespace.write.side_effect = flaky_write

    # Mock time.sleep to avoid actual delays during test
    import time
    monkeypatch.setattr(time, "sleep", lambda _: None)

    sink._write_batch_with_retry(namespace, batch_data, "ns")

    # Should succeed on the 3rd attempt
    assert attempts["count"] == 3


def test_write_batch_with_retry_exhausts_retries_and_raises(monkeypatch):
    sink = make_sink()
    namespace = MagicMock()
    batch_data = {"id": [1], "vector": [[0.1]]}

    namespace.write.side_effect = RuntimeError("persistent error")

    # Mock time.sleep to avoid actual delays during test
    import time
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="persistent error"):
        sink._write_batch_with_retry(namespace, batch_data, "ns")

    # Should be called 5 times (max_attempts=5)
    assert namespace.write.call_count == 5


def test_write_batch_with_retry_uses_configurable_distance_metric(monkeypatch):
    # Users can override the distance metric from the default "cosine_distance".
    sink = make_sink(schema={"field": "value"}, distance_metric="euclidean_squared")
    namespace = MagicMock()
    batch_data = {"id": [1], "vector": [[0.1]]}

    # No need to mock sleep/randomness since the first attempt should succeed
    sink._write_batch_with_retry(namespace, batch_data, "ns")

    namespace.write.assert_called_once_with(
        upsert_columns=batch_data,
        schema={"field": "value"},
        distance_metric="euclidean_squared",
    )


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

    # With streaming, _prepare_arrow_table is called once per block.
    assert mock_prepare.call_count == 2
    # First block has 2 rows, second block has 1 row.
    assert mock_prepare.call_args_list[0][0][0].num_rows == 2
    assert mock_prepare.call_args_list[1][0][0].num_rows == 1

    # Single-namespace path should be taken once per block.
    mock_multi.assert_not_called()
    assert mock_single.call_count == 2

    # All writes should go to the same namespace.
    for call in mock_single.call_args_list:
        client_arg, prepared_table_arg, ns_name = call[0]
        assert ns_name == "default_ns"
        assert client_arg is mock_get_client.return_value


def test_write_multi_namespace_end_to_end_uses_write_multi_namespace():
    sink = TurbopufferDatasink(
        region="gcp-us-central1",
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

    # With streaming, _prepare_arrow_table is called once per block.
    assert mock_prepare.call_count == 2
    assert mock_prepare.call_args_list[0][0][0].num_rows == 1
    assert mock_prepare.call_args_list[1][0][0].num_rows == 1

    mock_single.assert_not_called()
    # Multi-namespace path is called once per block.
    assert mock_multi.call_count == 2

    for call in mock_multi.call_args_list:
        client_arg, prepared_table_arg = call[0]
        assert prepared_table_arg.num_rows == 1
        assert client_arg is mock_get_client.return_value


### 9. Streaming write behavior (memory efficiency)


def test_write_processes_blocks_in_streaming_fashion():
    """Verify blocks are processed one at a time, not concatenated into one table.

    This test ensures memory efficiency by checking that each block is written
    independently rather than accumulating all blocks before writing.
    """
    sink = make_sink(batch_size=10)

    # Create multiple blocks
    blocks = [
        pa.table({"id": [i], "vector": [[float(i)]]})
        for i in range(5)
    ]

    write_calls = []

    def track_write_single_namespace(client, table, ns_name):
        # Record the table size at each call
        write_calls.append(table.num_rows)

    with patch.object(sink, "_get_client") as mock_get_client, patch.object(
        sink, "_write_single_namespace", side_effect=track_write_single_namespace
    ):
        mock_get_client.return_value = MagicMock()
        sink.write(blocks, ctx=None)

    # Should have 5 write calls, one per block
    assert len(write_calls) == 5
    # Each call should write 1 row (the size of each block)
    assert all(count == 1 for count in write_calls)


def test_write_multi_namespace_streaming_across_blocks():
    """Verify multi-namespace writes process blocks independently.

    Each block should be grouped and written separately, allowing the same
    namespace to receive multiple writes from different blocks.
    """
    sink = TurbopufferDatasink(
        region="gcp-us-central1",
        namespace=None,
        namespace_column="space_id",
        namespace_format="ns-{namespace}",
        api_key="test-api-key",
    )

    # Two blocks, each with rows for namespace "a"
    block1 = pa.table({"space_id": ["a", "a"], "id": [1, 2], "vector": [[1.0], [2.0]]})
    block2 = pa.table({"space_id": ["a", "b"], "id": [3, 4], "vector": [[3.0], [4.0]]})

    namespace_writes = {}

    def track_write_single_namespace(client, table, ns_name):
        if ns_name not in namespace_writes:
            namespace_writes[ns_name] = []
        namespace_writes[ns_name].append(table.num_rows)

    with patch.object(sink, "_get_client") as mock_get_client, patch.object(
        sink, "_write_single_namespace", side_effect=track_write_single_namespace
    ):
        mock_get_client.return_value = MagicMock()
        sink.write([block1, block2], ctx=None)

    # Namespace "a" should receive 2 writes (once from each block)
    # Block1: 2 rows to "a"
    # Block2: 1 row to "a", 1 row to "b"
    assert "ns-a" in namespace_writes
    assert "ns-b" in namespace_writes
    # "a" gets writes from both blocks
    assert namespace_writes["ns-a"] == [2, 1]
    # "b" only appears in block2
    assert namespace_writes["ns-b"] == [1]


### 10. Serialization behavior


def test_serialization_preserves_configuration(mock_turbopuffer_module):
    """Test that TurbopufferDatasink can be pickled and unpickled correctly.

    Verifies that configuration is preserved and lazy client initialization
    works after unpickling.
    """
    import pickle

    sink = make_sink()

    # Pickle and unpickle (before client initialization)
    pickled = pickle.dumps(sink)
    unpickled_sink = pickle.loads(pickled)

    # Verify all configuration is preserved
    assert unpickled_sink.namespace == sink.namespace
    assert unpickled_sink.api_key == sink.api_key
    assert unpickled_sink.region == sink.region
    assert unpickled_sink.batch_size == sink.batch_size
    assert unpickled_sink._client is None

    # Verify lazy initialization works after unpickling
    client = unpickled_sink._get_client()
    assert client is not None
    assert unpickled_sink._client is client
    mock_turbopuffer_module.Turbopuffer.assert_called()
