"""Tests for TurbopufferDatasink.

Organized by critical paths:
1. Constructor validation
2. Client initialization
3. Arrow table preparation
4. Single-namespace batching
5. Transform to Turbopuffer format
6. Retry logic
7. End-to-end write orchestration
8. Streaming behavior
9. Multi-namespace writes
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
        namespace="default_ns",
        region="gcp-us-central1",
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


def make_sink(**kwargs) -> TurbopufferDatasink:
    """Helper to construct a sink with minimal required arguments."""
    params = {
        "namespace": "default_ns",
        "region": "gcp-us-central1",
        "api_key": "test-api-key",
    }
    params.update(kwargs)
    return TurbopufferDatasink(**params)


# =============================================================================
# 1. Constructor validation
# =============================================================================


class TestConstructorValidation:
    """Tests for constructor argument validation."""

    def test_requires_namespace_or_namespace_column(self):
        """Must provide exactly one of namespace / namespace_column."""
        with pytest.raises(ValueError, match="Either.*must be provided"):
            TurbopufferDatasink(
                region="gcp-us-central1",
                api_key="k",
            )

    def test_rejects_both_namespace_and_namespace_column(self):
        """Cannot provide both namespace and namespace_column."""
        with pytest.raises(ValueError, match="exactly one"):
            TurbopufferDatasink(
                namespace="ns",
                namespace_column="ns_col",
                region="gcp-us-central1",
                api_key="k",
            )

    def test_namespace_column_cannot_be_id_or_vector(self):
        """namespace_column must not collide with id_column or vector_column."""
        with pytest.raises(ValueError, match="namespace_column.*must not be the same"):
            make_sink(namespace=None, namespace_column="id")

        with pytest.raises(ValueError, match="namespace_column.*must not be the same"):
            make_sink(namespace=None, namespace_column="vector")

    def test_api_key_from_env(self, monkeypatch):
        """API key can come from environment variable."""
        monkeypatch.delenv("TURBOPUFFER_API_KEY", raising=False)

        # No api_key and no env var -> error
        with pytest.raises(ValueError):
            TurbopufferDatasink(namespace="ns", region="gcp-us-central1")

        # With env var, init should succeed
        monkeypatch.setenv("TURBOPUFFER_API_KEY", "env-api-key")
        sink = TurbopufferDatasink(namespace="ns", region="gcp-us-central1")
        assert sink.api_key == "env-api-key"

    def test_rejects_same_id_and_vector_column(self):
        """id_column and vector_column must be distinct."""
        with pytest.raises(ValueError, match="id_column and vector_column"):
            make_sink(id_column="doc_id", vector_column="doc_id")

    def test_accepts_region_only(self):
        """Constructor succeeds with region and no base_url."""
        sink = make_sink(region="gcp-us-central1")
        assert sink.region == "gcp-us-central1"
        assert sink.base_url is None

    def test_accepts_base_url_only(self):
        """Constructor succeeds with base_url and no region."""
        sink = make_sink(
            region=None,
            base_url="https://gcp-us-central1.turbopuffer.com",
        )
        assert sink.base_url == "https://gcp-us-central1.turbopuffer.com"
        assert sink.region is None

    def test_rejects_both_region_and_base_url(self):
        """Cannot provide both region and base_url."""
        with pytest.raises(ValueError, match="exactly one of 'region' or 'base_url'"):
            make_sink(
                region="gcp-us-central1",
                base_url="https://gcp-us-central1.turbopuffer.com",
            )

    def test_rejects_neither_region_nor_base_url(self):
        """Must provide at least one of region or base_url."""
        with pytest.raises(ValueError, match="Either 'region' or 'base_url'"):
            TurbopufferDatasink(
                namespace="ns",
                api_key="k",
            )


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

    def test_uses_base_url(self, mock_turbopuffer_module):
        """Client uses base_url when region is not provided."""
        sink = make_sink(
            region=None,
            base_url="https://gcp-us-central1.turbopuffer.com",
        )
        sink._get_client()

        mock_turbopuffer_module.Turbopuffer.assert_called_once_with(
            api_key="test-api-key",
            base_url="https://gcp-us-central1.turbopuffer.com",
        )

    def test_base_url_does_not_pass_region(self, mock_turbopuffer_module):
        """When base_url is used, region is not passed to the client."""
        sink = make_sink(
            region=None,
            base_url="https://custom.turbopuffer.com",
        )
        sink._get_client()

        call_kwargs = mock_turbopuffer_module.Turbopuffer.call_args[1]
        assert "region" not in call_kwargs
        assert call_kwargs["base_url"] == "https://custom.turbopuffer.com"

    def test_region_does_not_pass_base_url(self, mock_turbopuffer_module):
        """When region is used, base_url is not passed to the client."""
        sink = make_sink(region="gcp-us-central1")
        sink._get_client()

        call_kwargs = mock_turbopuffer_module.Turbopuffer.call_args[1]
        assert "base_url" not in call_kwargs
        assert call_kwargs["region"] == "gcp-us-central1"


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

    def test_missing_vector_column_raises(self):
        """Missing vector column raises ValueError."""
        table = pa.table({"id": [1, 2, 3]})
        sink = make_sink(vector_column="embedding")

        with pytest.raises(ValueError, match="Vector column 'embedding' not found"):
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
# 4. Single-namespace batching
# =============================================================================


class TestSingleNamespaceBatching:
    """Tests for write batching behavior."""

    def test_batches_by_batch_size(self, mock_client):
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

        def track_batch(ns, batch, namespace_name=None):
            # batch is a RecordBatch, get its row count
            batch_sizes.append(batch.num_rows)

        with patch.object(sink, "_get_client", return_value=mock_client):
            with patch.object(sink, "_write_batch_with_retry", side_effect=track_batch):
                sink.write([table], ctx=None)

        assert batch_sizes == [10, 10, 5]

    def test_skips_empty_blocks(self, sink):
        """Empty blocks don't trigger namespace writes."""
        empty_table = pa.table({"id": [], "vector": []})

        with patch.object(sink, "_get_client") as mock_get_client:
            with patch.object(sink, "_write_batch_with_retry") as mock_write:
                mock_get_client.return_value = MagicMock()
                sink.write([empty_table], ctx=None)

        mock_write.assert_not_called()


# =============================================================================
# 5. Transform to Turbopuffer format
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
# 6. Retry logic
# =============================================================================


class TestRetryLogic:
    """Tests for _write_batch_with_retry."""

    @pytest.fixture
    def sample_batch(self):
        """A simple batch for retry tests."""
        return pa.table({"id": [1], "vector": [[0.1]]})

    def test_success_first_try(self, sink, sample_batch):
        """Successful write on first attempt."""
        namespace = MagicMock()

        sink._write_batch_with_retry(namespace, sample_batch)

        namespace.write.assert_called_once_with(
            upsert_columns={"id": [1], "vector": [[0.1]]},
            schema=None,
            distance_metric="cosine_distance",
        )

    def test_retries_then_succeeds(self, sink, sample_batch, monkeypatch):
        """Transient failures are retried."""
        monkeypatch.setattr(time, "sleep", lambda _: None)
        namespace = MagicMock()
        attempts = {"count": 0}

        def flaky_write(*args, **kwargs):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise RuntimeError("temporary error")

        namespace.write.side_effect = flaky_write

        sink._write_batch_with_retry(namespace, sample_batch)

        assert attempts["count"] == 3

    def test_exhausts_retries_and_raises(self, sink, sample_batch, monkeypatch):
        """Persistent failures exhaust retries and raise."""
        monkeypatch.setattr(time, "sleep", lambda _: None)
        namespace = MagicMock()
        namespace.write.side_effect = RuntimeError("persistent error")

        with pytest.raises(RuntimeError, match="persistent error"):
            sink._write_batch_with_retry(namespace, sample_batch)

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
        batch = pa.table({"id": [1], "vector": [[0.1]]})

        sink._write_batch_with_retry(namespace, batch)

        namespace.write.assert_called_once_with(
            upsert_columns={"id": [1], "vector": [[0.1]]},
            schema=schema,
            distance_metric=distance_metric,
        )


# =============================================================================
# 7. End-to-end write orchestration
# =============================================================================


class TestWriteOrchestration:
    """Tests for top-level write() method."""

    def test_write_multiple_blocks(self, sink):
        """Multiple blocks are processed and written."""
        blocks = [
            pa.table({"id": [1, 2], "vector": [[1.0], [2.0]]}),
            pa.table({"id": [3], "vector": [[3.0]]}),
        ]
        write_calls = []

        def track_write(ns, batch, namespace_name=None):
            write_calls.append(batch.num_rows)

        with patch.object(sink, "_get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            with patch.object(sink, "_write_batch_with_retry", side_effect=track_write):
                sink.write(blocks, ctx=None)

        # Two blocks written
        assert len(write_calls) == 2
        assert write_calls == [2, 1]

        # Namespace accessed with correct name
        mock_client.namespace.assert_called_with("default_ns")


# =============================================================================
# 8. Streaming behavior (memory efficiency)
# =============================================================================


class TestStreamingBehavior:
    """Tests for memory-efficient streaming writes."""

    def test_processes_blocks_independently(self, sink):
        """Each block is processed and written separately."""
        blocks = [pa.table({"id": [i], "vector": [[float(i)]]}) for i in range(5)]
        write_counts = []

        def track_write(ns, batch, namespace_name=None):
            write_counts.append(batch.num_rows)

        with patch.object(sink, "_get_client", return_value=MagicMock()):
            with patch.object(sink, "_write_batch_with_retry", side_effect=track_write):
                sink.write(blocks, ctx=None)

        # 5 blocks → 5 writes of 1 row each
        assert len(write_counts) == 5
        assert all(c == 1 for c in write_counts)


# =============================================================================
# 9. Multi-namespace writes
# =============================================================================


class TestMultiNamespaceWrites:
    """Tests for namespace_column-driven multi-namespace writes."""

    def test_routes_rows_to_correct_namespaces(self):
        """Rows are grouped by namespace_column and written to the right ns."""
        sink = make_sink(namespace=None, namespace_column="tenant")
        table = pa.table(
            {
                "tenant": ["ns_a", "ns_b", "ns_a", "ns_b"],
                "id": [1, 2, 3, 4],
                "vector": [[0.1], [0.2], [0.3], [0.4]],
            }
        )

        writes = {}  # namespace_name -> list of row counts

        def track_write(ns, batch, namespace_name=None):
            writes.setdefault(namespace_name, []).append(batch.num_rows)

        mock_client = MagicMock()
        mock_client.namespace.return_value = MagicMock()
        with patch.object(sink, "_get_client", return_value=mock_client):
            with patch.object(sink, "_write_batch_with_retry", side_effect=track_write):
                sink.write([table], ctx=None)

        assert "ns_a" in writes
        assert "ns_b" in writes
        assert sum(writes["ns_a"]) == 2
        assert sum(writes["ns_b"]) == 2

    def test_drops_namespace_column_before_writing(self):
        """The namespace column is not included in the written data."""
        sink = make_sink(namespace=None, namespace_column="tenant")
        table = pa.table(
            {
                "tenant": ["ns_a"],
                "id": [1],
                "vector": [[0.1]],
            }
        )

        written_batches = []

        def capture_batch(ns, batch, namespace_name=None):
            written_batches.append(batch)

        mock_client = MagicMock()
        mock_client.namespace.return_value = MagicMock()
        with patch.object(sink, "_get_client", return_value=mock_client):
            with patch.object(
                sink, "_write_batch_with_retry", side_effect=capture_batch
            ):
                sink.write([table], ctx=None)

        assert len(written_batches) == 1
        assert "tenant" not in written_batches[0].column_names
        assert "id" in written_batches[0].column_names

    def test_missing_namespace_column_raises(self):
        """Missing namespace column in data raises ValueError."""
        sink = make_sink(namespace=None, namespace_column="tenant")
        table = pa.table(
            {
                "id": [1],
                "vector": [[0.1]],
            }
        )

        mock_client = MagicMock()
        with patch.object(sink, "_get_client", return_value=mock_client):
            with pytest.raises(ValueError, match="Namespace column.*not found"):
                sink.write([table], ctx=None)

    def test_null_namespace_values_raise(self):
        """Null values in namespace column raise ValueError."""
        sink = make_sink(namespace=None, namespace_column="tenant")
        table = pa.table(
            {
                "tenant": ["ns_a", None],
                "id": [1, 2],
                "vector": [[0.1], [0.2]],
            }
        )

        mock_client = MagicMock()
        with patch.object(sink, "_get_client", return_value=mock_client):
            with pytest.raises(ValueError, match="contains null values"):
                sink.write([table], ctx=None)

    def test_skips_empty_blocks_in_multi_namespace(self):
        """Empty blocks are skipped in multi-namespace mode."""
        sink = make_sink(namespace=None, namespace_column="tenant")
        empty_table = pa.table(
            {
                "tenant": pa.array([], type=pa.string()),
                "id": pa.array([], type=pa.int64()),
                "vector": pa.array([], type=pa.list_(pa.float64())),
            }
        )

        mock_client = MagicMock()
        with patch.object(sink, "_get_client", return_value=mock_client):
            with patch.object(sink, "_write_batch_with_retry") as mock_write:
                sink.write([empty_table], ctx=None)

        mock_write.assert_not_called()


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
        assert unpickled.namespace_column == sink.namespace_column
        assert unpickled.api_key == sink.api_key
        assert unpickled.region == sink.region
        assert unpickled.base_url == sink.base_url
        assert unpickled.batch_size == sink.batch_size
        assert unpickled._client is None

        # Lazy initialization works after unpickling
        client = unpickled._get_client()
        assert client is not None
        mock_turbopuffer_module.Turbopuffer.assert_called()

    def test_preserves_namespace_column_configuration(self, mock_turbopuffer_module):
        """namespace_column configuration survives pickle round-trip."""
        sink = make_sink(namespace=None, namespace_column="tenant")
        pickled = pickle.dumps(sink)
        unpickled = pickle.loads(pickled)

        assert unpickled.namespace is None
        assert unpickled.namespace_column == "tenant"
        assert unpickled._client is None

    def test_preserves_base_url_configuration(self, mock_turbopuffer_module):
        """base_url configuration survives pickle round-trip."""
        sink = make_sink(
            region=None,
            base_url="https://gcp-us-central1.turbopuffer.com",
        )
        pickled = pickle.dumps(sink)
        unpickled = pickle.loads(pickled)

        assert unpickled.region is None
        assert unpickled.base_url == "https://gcp-us-central1.turbopuffer.com"
        assert unpickled._client is None

        # Lazy initialization works and uses base_url
        unpickled._get_client()
        mock_turbopuffer_module.Turbopuffer.assert_called_once_with(
            api_key="test-api-key",
            base_url="https://gcp-us-central1.turbopuffer.com",
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
