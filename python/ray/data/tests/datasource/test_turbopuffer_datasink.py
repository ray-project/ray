"""Tests for TurbopufferDatasink.

Organized by critical paths:
1. Constructor validation
2. Client initialization
3. Arrow table preparation
4. Multi-namespace behavior
5. Single-namespace batching
6. Transform to Turbopuffer format
7. Retry logic
8. End-to-end write orchestration
9. Streaming behavior
10. Serialization
"""

import pickle
import sys
import time
import uuid
from typing import List
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

from ray.data._internal.datasource.turbopuffer_datasink import TurbopufferDatasink
from ray.data._internal.utils.arrow_utils import get_pyarrow_version

# Skip all tests if PyArrow version is less than 19.0
pytestmark = pytest.mark.skipif(
    get_pyarrow_version() < parse_version("19.0.0"),
    reason="TurbopufferDatasink tests require PyArrow >= 19.0",
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def mock_turbopuffer_module(monkeypatch):
    """Provide a fake turbopuffer module so imports in the datasink succeed."""
    fake_module = MagicMock()
    fake_module.Turbopuffer = MagicMock()
    with patch.dict(sys.modules, {"turbopuffer": fake_module}):
        yield fake_module


@pytest.fixture
def sink():
    """Default sink with minimal required arguments."""
    return TurbopufferDatasink(
        region="gcp-us-central1",
        namespace="default_ns",
        api_key="test-api-key",
    )


@pytest.fixture
def multi_namespace_sink():
    """Sink configured for multi-namespace mode."""
    return TurbopufferDatasink(
        region="gcp-us-central1",
        namespace=None,
        namespace_column="space_id",
        namespace_format="ns-{namespace}",
        api_key="test-api-key",
    )


@pytest.fixture
def mock_client():
    """Mock Turbopuffer client with namespace support."""
    client = MagicMock()
    client.namespace.return_value = MagicMock()
    return client


@pytest.fixture
def sample_table():
    """Standard table with id and vector columns."""
    return pa.table(
        {
            "id": [1, 2, 3],
            "vector": [[0.1], [0.2], [0.3]],
        }
    )


@pytest.fixture
def sample_table_with_namespace():
    """Table with namespace column for multi-namespace testing."""
    return pa.table(
        {
            "space_id": ["a", "b", "a"],
            "id": [1, 2, 3],
            "vector": [[1.0], [2.0], [3.0]],
        }
    )


def make_sink(**kwargs) -> TurbopufferDatasink:
    """Helper to construct a sink with minimal required arguments."""
    params = {
        "region": "gcp-us-central1",
        "namespace": "default_ns",
        "api_key": "test-api-key",
    }
    params.update(kwargs)
    return TurbopufferDatasink(**params)


# =============================================================================
# 1. Constructor validation
# =============================================================================


class TestConstructorValidation:
    """Tests for constructor argument validation."""

    def test_requires_either_namespace_or_namespace_column(self):
        """Must provide exactly one of namespace or namespace_column."""
        # Neither provided
        with pytest.raises(ValueError):
            TurbopufferDatasink(region="gcp-us-central1", api_key="k")

        # Both provided
        with pytest.raises(ValueError):
            TurbopufferDatasink(
                region="gcp-us-central1",
                namespace="ns",
                namespace_column="space_id",
                api_key="k",
            )

    def test_namespace_format_requires_placeholder(self):
        """namespace_format must contain {namespace} placeholder."""
        with pytest.raises(ValueError):
            TurbopufferDatasink(
                region="gcp-us-central1",
                namespace_column="space_id",
                namespace_format="space-{id}",  # Wrong placeholder
                api_key="k",
            )

    def test_api_key_from_env(self, monkeypatch):
        """API key can come from environment variable."""
        monkeypatch.delenv("TURBOPUFFER_API_KEY", raising=False)

        # No api_key and no env var -> error
        with pytest.raises(ValueError):
            TurbopufferDatasink(region="gcp-us-central1", namespace="ns")

        # With env var, init should succeed
        monkeypatch.setenv("TURBOPUFFER_API_KEY", "env-api-key")
        sink = TurbopufferDatasink(region="gcp-us-central1", namespace="ns")
        assert sink.api_key == "env-api-key"

    @pytest.mark.parametrize(
        "id_col,vector_col,ns_col,expected_match",
        [
            ("doc_id", "doc_id", None, "id_column and vector_column"),
            ("doc_id", "vector", "doc_id", "cannot be the same as id_column"),
            ("id", "embedding", "embedding", "cannot be the same as vector_column"),
        ],
        ids=["id==vector", "namespace==id", "namespace==vector"],
    )
    def test_rejects_column_conflicts(self, id_col, vector_col, ns_col, expected_match):
        """Column names must be distinct."""
        kwargs = {"id_column": id_col, "vector_column": vector_col}
        if ns_col:
            kwargs["namespace"] = None
            kwargs["namespace_column"] = ns_col
        with pytest.raises(ValueError, match=expected_match):
            make_sink(**kwargs)


# =============================================================================
# 2. Client initialization
# =============================================================================


class TestClientInitialization:
    """Tests for Turbopuffer client lazy initialization."""

    def test_lazy_initialization(self, sink, mock_turbopuffer_module):
        """Client is created lazily and cached."""
        client1 = sink._get_client()
        client2 = sink._get_client()

        assert client1 is client2
        mock_turbopuffer_module.Turbopuffer.assert_called_once_with(
            api_key="test-api-key",
            region="gcp-us-central1",
        )

    def test_uses_explicit_region(self, mock_turbopuffer_module):
        """Client uses the configured region."""
        sink = make_sink(region="custom-region")
        sink._get_client()

        mock_turbopuffer_module.Turbopuffer.assert_called_once_with(
            api_key="test-api-key",
            region="custom-region",
        )


# =============================================================================
# 3. Arrow table preparation
# =============================================================================


class TestArrowTablePreparation:
    """Tests for _prepare_arrow_table."""

    def test_renames_columns_and_filters_null_ids(self):
        """Custom columns are renamed and null IDs filtered."""
        table = pa.table(
            {
                "doc_id": [1, 2, None],
                "emb": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]],
            }
        )
        sink = make_sink(id_column="doc_id", vector_column="emb")

        prepared = sink._prepare_arrow_table(table)

        # Null ID row filtered, columns renamed to id/vector
        expected = pa.table(
            {
                "id": [1, 2],
                "vector": [[0.1, 0.2], [0.3, 0.4]],
            }
        )
        assert prepared.equals(expected)

    def test_missing_id_column_raises(self):
        """Missing custom ID column raises ValueError."""
        table = pa.table({"other": [1, 2, 3]})
        sink = make_sink(id_column="doc_id")

        with pytest.raises(ValueError):
            sink._prepare_arrow_table(table)

    @pytest.mark.parametrize(
        "existing_col,custom_col,expected_match",
        [
            ("id", "doc_id", "already has.*'id' column"),
            ("vector", "emb", "already has.*'vector' column"),
        ],
        ids=["id_conflict", "vector_conflict"],
    )
    def test_conflicting_column_names_raise(
        self, existing_col, custom_col, expected_match
    ):
        """Raise if table already has target column name."""
        if existing_col == "id":
            table = pa.table(
                {"id": [1, 2], "doc_id": [10, 20], "vector": [[0.1], [0.2]]}
            )
            sink = make_sink(id_column="doc_id")
        else:
            table = pa.table(
                {"id": [1, 2], "vector": [[0.1], [0.2]], "emb": [[0.3], [0.4]]}
            )
            sink = make_sink(vector_column="emb")

        with pytest.raises(ValueError, match=expected_match):
            sink._prepare_arrow_table(table)


# =============================================================================
# 4. Multi-namespace behavior
# =============================================================================


class TestMultiNamespaceBehavior:
    """Tests for _write_multi_namespace grouping and formatting."""

    def test_groups_by_column_and_formats_names(
        self, multi_namespace_sink, sample_table_with_namespace
    ):
        """Rows are grouped by namespace column and names formatted."""
        client = MagicMock()

        with patch.object(
            multi_namespace_sink, "_write_single_namespace"
        ) as mock_write:
            multi_namespace_sink._write_multi_namespace(
                client, sample_table_with_namespace
            )

        assert mock_write.call_count == 2

        seen_namespaces = {call.args[2] for call in mock_write.call_args_list}
        rows_per_ns = {
            call.args[2]: call.args[1].num_rows for call in mock_write.call_args_list
        }

        assert seen_namespaces == {"ns-a", "ns-b"}
        assert rows_per_ns["ns-a"] == 2  # "a" appears twice
        assert rows_per_ns["ns-b"] == 1

    @pytest.mark.parametrize(
        "byte_values,expected_format",
        [
            # 16-byte UUID bytes -> UUID string format
            (
                [uuid.uuid4().bytes, uuid.uuid4().bytes],
                lambda u: str(uuid.UUID(bytes=u)),
            ),
            # Non-16-byte bytes -> hex format
            (
                [b"\x01\x02\x03", b"\xde\xad\xbe\xef"],
                lambda b: b.hex(),
            ),
        ],
        ids=["uuid_bytes", "non_uuid_bytes"],
    )
    def test_bytes_to_namespace_conversion(self, byte_values, expected_format):
        """Bytes in namespace column are converted to valid namespace strings."""
        b1, b2 = byte_values[0], byte_values[1]
        table = pa.table(
            {
                "space_id": [b1, b2, b1],
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

        with patch.object(sink, "_write_single_namespace") as mock_write:
            sink._write_multi_namespace(client, table)

        ns_names = {call.args[2] for call in mock_write.call_args_list}
        expected = {f"ns-{expected_format(b1)}", f"ns-{expected_format(b2)}"}
        assert ns_names == expected

    def test_missing_namespace_column_raises(self, multi_namespace_sink):
        """Missing namespace column raises ValueError."""
        table = pa.table({"id": [1, 2], "vector": [[1.0], [2.0]]})
        client = MagicMock()

        with pytest.raises(ValueError):
            multi_namespace_sink._write_multi_namespace(client, table)

    def test_null_namespace_values_raises(self, multi_namespace_sink):
        """Null values in namespace column raise ValueError."""
        table = pa.table(
            {
                "space_id": ["a", None, "b"],
                "id": [1, 2, 3],
                "vector": [[1.0], [2.0], [3.0]],
            }
        )
        client = MagicMock()

        with pytest.raises(ValueError, match="contains null values"):
            multi_namespace_sink._write_multi_namespace(client, table)


# =============================================================================
# 5. Single-namespace batching
# =============================================================================


class TestSingleNamespaceBatching:
    """Tests for _write_single_namespace batching behavior."""

    def test_batches_by_batch_size(self, sink, mock_client):
        """Large tables are split into batches."""
        num_rows = 25
        table = pa.table(
            {
                "id": list(range(num_rows)),
                "vector": [[float(i)] for i in range(num_rows)],
            }
        )
        sink = make_sink(batch_size=10)
        batch_sizes: List[int] = []

        def track_batch(ns, batch_data, ns_name):
            batch_sizes.append(len(next(iter(batch_data.values()))))

        with patch.object(
            sink,
            "_transform_to_turbopuffer_format",
            side_effect=lambda t: t.to_pydict(),
        ):
            with patch.object(sink, "_write_batch_with_retry", side_effect=track_batch):
                sink._write_single_namespace(mock_client, table, "ns")

        assert batch_sizes == [10, 10, 5]

    def test_skips_empty_blocks(self, sink):
        """Empty blocks don't trigger namespace writes."""
        empty_table = pa.table({"id": [], "vector": []})

        with patch.object(sink, "_get_client") as mock_get_client:
            with patch.object(sink, "_write_single_namespace") as mock_single:
                with patch.object(sink, "_write_multi_namespace") as mock_multi:
                    mock_get_client.return_value = MagicMock()
                    sink.write([empty_table], ctx=None)

        mock_single.assert_not_called()
        mock_multi.assert_not_called()


# =============================================================================
# 6. Transform to Turbopuffer format
# =============================================================================


class TestTransformToTurbopufferFormat:
    """Tests for _transform_to_turbopuffer_format."""

    def test_requires_id_column(self, sink):
        """Table must have 'id' column."""
        table = pa.table({"col": [1, 2, 3]})

        with pytest.raises(ValueError):
            sink._transform_to_turbopuffer_format(table)

    def test_converts_uuid_bytes_to_native_uuid(self, sink):
        """16-byte binary IDs become native uuid.UUID objects.

        Per Turbopuffer performance docs, native UUIDs (16 bytes) are more
        efficient than string UUIDs (36 bytes).
        """
        u = uuid.uuid4()

        # ID column must be binary(16) for UUID conversion
        table = pa.table(
            {
                "id": pa.array([u.bytes], type=pa.binary(16)),
                "vector": [[0.1, 0.2]],
            }
        )

        columns = sink._transform_to_turbopuffer_format(table)

        expected = {
            "id": [u],  # Native uuid.UUID, not bytes
            "vector": [[0.1, 0.2]],
        }
        assert columns == expected
        assert isinstance(columns["id"][0], uuid.UUID)


# =============================================================================
# 7. Retry logic
# =============================================================================


class TestRetryLogic:
    """Tests for _write_batch_with_retry."""

    def test_success_first_try(self, sink):
        """Successful write on first attempt."""
        namespace = MagicMock()
        batch_data = {"id": [1], "vector": [[0.1]]}

        sink._write_batch_with_retry(namespace, batch_data, "ns")

        namespace.write.assert_called_once_with(
            upsert_columns=batch_data,
            schema=None,
            distance_metric="cosine_distance",
        )

    def test_retries_then_succeeds(self, sink, monkeypatch):
        """Transient failures are retried."""
        monkeypatch.setattr(time, "sleep", lambda _: None)
        namespace = MagicMock()
        batch_data = {"id": [1], "vector": [[0.1]]}
        attempts = {"count": 0}

        def flaky_write(*args, **kwargs):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise RuntimeError("temporary error")

        namespace.write.side_effect = flaky_write

        sink._write_batch_with_retry(namespace, batch_data, "ns")

        assert attempts["count"] == 3

    def test_exhausts_retries_and_raises(self, sink, monkeypatch):
        """Persistent failures exhaust retries and raise."""
        monkeypatch.setattr(time, "sleep", lambda _: None)
        namespace = MagicMock()
        batch_data = {"id": [1], "vector": [[0.1]]}
        namespace.write.side_effect = RuntimeError("persistent error")

        with pytest.raises(RuntimeError, match="persistent error"):
            sink._write_batch_with_retry(namespace, batch_data, "ns")

        assert namespace.write.call_count == 5  # max_attempts=5

    @pytest.mark.parametrize(
        "schema,distance_metric",
        [
            ({"field": "value"}, "cosine_distance"),
            (None, "euclidean_squared"),
            ({"type": "string"}, "euclidean_squared"),
        ],
        ids=["with_schema", "alt_metric", "both"],
    )
    def test_configurable_options(self, schema, distance_metric):
        """Schema and distance_metric are passed to write."""
        sink = make_sink(schema=schema, distance_metric=distance_metric)
        namespace = MagicMock()
        batch_data = {"id": [1], "vector": [[0.1]]}

        sink._write_batch_with_retry(namespace, batch_data, "ns")

        namespace.write.assert_called_once_with(
            upsert_columns=batch_data,
            schema=schema,
            distance_metric=distance_metric,
        )


# =============================================================================
# 8. End-to-end write orchestration
# =============================================================================


class TestWriteOrchestration:
    """Tests for top-level write() method."""

    @pytest.mark.parametrize(
        "mode,blocks,expected_single_calls,expected_multi_calls",
        [
            # Single-namespace mode: calls _write_single_namespace
            (
                "single",
                [
                    {"id": [1, 2], "vector": [[1.0], [2.0]]},
                    {"id": [3], "vector": [[3.0]]},
                ],
                2,
                0,
            ),
            # Multi-namespace mode: calls _write_multi_namespace
            (
                "multi",
                [
                    {"space_id": ["a"], "id": [1], "vector": [[1.0]]},
                    {"space_id": ["b"], "id": [2], "vector": [[2.0]]},
                ],
                0,
                2,
            ),
        ],
        ids=["single_namespace", "multi_namespace"],
    )
    def test_write_routes_to_correct_handler(
        self,
        sink,
        multi_namespace_sink,
        mode,
        blocks,
        expected_single_calls,
        expected_multi_calls,
    ):
        """Write routes to single or multi namespace handler based on config."""
        target_sink = sink if mode == "single" else multi_namespace_sink
        tables = [pa.table(b) for b in blocks]

        # Track calls to write handlers
        single_calls = []
        multi_calls = []

        def track_single(client, table, ns_name):
            single_calls.append(ns_name)

        def track_multi(client, table):
            multi_calls.append(table.num_rows)

        with patch.object(target_sink, "_get_client", return_value=MagicMock()):
            with patch.object(
                target_sink, "_write_single_namespace", side_effect=track_single
            ):
                with patch.object(
                    target_sink, "_write_multi_namespace", side_effect=track_multi
                ):
                    target_sink.write(tables, ctx=None)

        assert len(single_calls) == expected_single_calls
        assert len(multi_calls) == expected_multi_calls

        # Verify single-namespace writes go to correct namespace
        if mode == "single":
            assert all(ns == "default_ns" for ns in single_calls)


# =============================================================================
# 9. Streaming behavior (memory efficiency)
# =============================================================================


class TestStreamingBehavior:
    """Tests for memory-efficient streaming writes."""

    @staticmethod
    def _collect_writes(target_sink, blocks):
        """Execute write and return {namespace: [row_counts]} for each write call."""
        writes = {}

        def track(client, table, ns_name):
            writes.setdefault(ns_name, []).append(table.num_rows)

        with patch.object(target_sink, "_get_client", return_value=MagicMock()):
            with patch.object(
                target_sink, "_write_single_namespace", side_effect=track
            ):
                target_sink.write(blocks, ctx=None)
        return writes

    def test_processes_blocks_independently(self, sink):
        """Each block is processed and written separately."""
        blocks = [pa.table({"id": [i], "vector": [[float(i)]]}) for i in range(5)]

        writes = self._collect_writes(sink, blocks)

        # 5 blocks → 5 writes of 1 row each
        all_counts = [c for counts in writes.values() for c in counts]
        assert len(all_counts) == 5
        assert all(c == 1 for c in all_counts)

    def test_multi_namespace_streaming(self, multi_namespace_sink):
        """Same namespace can receive writes from multiple blocks."""
        blocks = [
            pa.table({"space_id": ["a", "a"], "id": [1, 2], "vector": [[1.0], [2.0]]}),
            pa.table({"space_id": ["a", "b"], "id": [3, 4], "vector": [[3.0], [4.0]]}),
        ]

        writes = self._collect_writes(multi_namespace_sink, blocks)

        # Namespace "a" receives 2 writes: 2 rows from block1, 1 row from block2
        assert writes["ns-a"] == [2, 1]
        assert writes["ns-b"] == [1]


# =============================================================================
# 10. Serialization behavior
# =============================================================================


class TestSerialization:
    """Tests for pickle serialization support."""

    def test_preserves_configuration(self, sink, mock_turbopuffer_module):
        """Configuration is preserved after pickle round-trip."""
        pickled = pickle.dumps(sink)
        unpickled = pickle.loads(pickled)

        assert unpickled.namespace == sink.namespace
        assert unpickled.api_key == sink.api_key
        assert unpickled.region == sink.region
        assert unpickled.batch_size == sink.batch_size
        assert unpickled._client is None

        # Lazy initialization works after unpickling
        client = unpickled._get_client()
        assert client is not None
        mock_turbopuffer_module.Turbopuffer.assert_called()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
