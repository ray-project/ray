import pickle
from typing import Iterator
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest
from google.api_core import exceptions, operation
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import job
from google.cloud.bigquery_storage_v1.types import stream as gcbqs_stream

import ray
from ray.data._internal.datasource.bigquery_client_provider import (
    BigQueryClientProvider,
    DefaultBigQueryClientProvider,
)
from ray.data._internal.datasource.bigquery_datasink import BigQueryDatasink
from ray.data._internal.datasource.bigquery_datasource import BigQueryDatasource
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.planner.plan_write_op import generate_collect_write_stats_fn
from ray.data.block import Block
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa

_TEST_GCP_PROJECT_ID = "mock-test-project-id"
_TEST_BQ_DATASET_ID = "mockdataset"
_TEST_BQ_TABLE_ID = "mocktable"
_TEST_BQ_DATASET = _TEST_BQ_DATASET_ID + "." + _TEST_BQ_TABLE_ID
_TEST_BQ_TEMP_DESTINATION = _TEST_GCP_PROJECT_ID + ".tempdataset.temptable"


@pytest.fixture(autouse=True)
def bq_client_full_mock(monkeypatch):
    client_mock = mock.create_autospec(bigquery.Client)
    client_mock.return_value = client_mock

    def bq_get_dataset_mock(dataset_id):
        if dataset_id != _TEST_BQ_DATASET_ID:
            raise exceptions.NotFound(
                "Dataset {} is not found. Please ensure that it exists.".format(
                    _TEST_BQ_DATASET
                )
            )

    def bq_get_table_mock(table_id):
        if table_id != _TEST_BQ_DATASET:
            raise exceptions.NotFound(
                "Table {} is not found. Please ensure that it exists.".format(
                    _TEST_BQ_DATASET
                )
            )

    def bq_create_dataset_mock(dataset_id, **kwargs):
        if dataset_id == "existingdataset":
            raise exceptions.Conflict("Dataset already exists")
        return mock.Mock(operation.Operation)

    def bq_delete_table_mock(table, **kwargs):
        return None

    def bq_query_mock(query):
        fake_job_ref = job._JobReference(
            "fake_job_id", _TEST_GCP_PROJECT_ID, "us-central1"
        )
        fake_query_job = job.QueryJob(fake_job_ref, query, None)
        fake_query_job.configuration.destination = _TEST_BQ_TEMP_DESTINATION
        return fake_query_job

    client_mock.get_dataset = bq_get_dataset_mock
    client_mock.get_table = bq_get_table_mock
    client_mock.create_dataset = bq_create_dataset_mock
    client_mock.delete_table = bq_delete_table_mock
    client_mock.query = bq_query_mock

    monkeypatch.setattr(bigquery, "Client", client_mock)
    return client_mock


@pytest.fixture(autouse=True)
def bqs_client_full_mock(monkeypatch):
    client_mock = mock.create_autospec(bigquery_storage.BigQueryReadClient)
    client_mock.return_value = client_mock

    def bqs_create_read_session(max_stream_count=0, **kwargs):
        read_session_proto = gcbqs_stream.ReadSession()
        read_session_proto.streams = [
            gcbqs_stream.ReadStream() for _ in range(max_stream_count)
        ]
        return read_session_proto

    client_mock.create_read_session = bqs_create_read_session

    monkeypatch.setattr(bigquery_storage, "BigQueryReadClient", client_mock)
    client_mock.reset_mock()
    return client_mock


@pytest.fixture
def bq_query_result_mock():
    with mock.patch.object(bigquery.job.QueryJob, "result") as query_result_mock:
        yield query_result_mock


@pytest.fixture
def bq_query_result_mock_fail():
    with mock.patch.object(bigquery.job.QueryJob, "result") as query_result_mock_fail:
        query_result_mock_fail.side_effect = exceptions.BadRequest("400 Syntax error")
        yield query_result_mock_fail


@pytest.fixture
def ray_get_mock():
    with mock.patch.object(ray, "get") as ray_get:
        ray_get.return_value = None
        yield ray_get


class TestReadBigQuery:
    """Tests for BigQuery Read."""

    @pytest.mark.parametrize(
        "parallelism",
        [1, 2, 3, 4, 10, 100],
    )
    def test_create_read_tasks(self, parallelism):
        bq_ds = BigQueryDatasource(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
        )
        read_tasks_list = bq_ds.get_read_tasks(parallelism)
        assert len(read_tasks_list) == parallelism

    @pytest.mark.parametrize(
        "parallelism",
        [1, 2, 3, 4, 10, 100],
    )
    def test_create_reader_query(self, parallelism, bq_query_result_mock):
        bq_ds = BigQueryDatasource(
            project_id=_TEST_GCP_PROJECT_ID,
            query="SELECT * FROM mockdataset.mocktable",
        )
        read_tasks_list = bq_ds.get_read_tasks(parallelism)
        bq_query_result_mock.assert_called_once()
        assert len(read_tasks_list) == parallelism

    @pytest.mark.parametrize(
        "parallelism",
        [1, 2, 3, 4, 10, 100],
    )
    def test_create_reader_query_bad_request(
        self,
        parallelism,
        bq_query_result_mock_fail,
    ):
        bq_ds = BigQueryDatasource(
            project_id=_TEST_GCP_PROJECT_ID,
            query="SELECT * FROM mockdataset.mocktable",
        )
        with pytest.raises(exceptions.BadRequest):
            bq_ds.get_read_tasks(parallelism)
        bq_query_result_mock_fail.assert_called()

    def test_dataset_query_kwargs_provided(self):
        with pytest.raises(ValueError) as exception:
            BigQueryDatasource(
                project_id=_TEST_GCP_PROJECT_ID,
                dataset=_TEST_BQ_DATASET,
                query="SELECT * FROM mockdataset.mocktable",
            )
        expected_message = (
            "Query and dataset kwargs cannot both be provided"
            + " (must be mutually exclusive)."
        )
        assert str(exception.value) == expected_message

    def test_create_reader_dataset_not_found(self):
        parallelism = 4
        bq_ds = BigQueryDatasource(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset="nonexistentdataset.mocktable",
        )
        with pytest.raises(ValueError) as exception:
            bq_ds.get_read_tasks(parallelism)
        expected_message = (
            "Dataset nonexistentdataset is not found. Please ensure that it exists."
        )
        assert str(exception.value) == expected_message

    def test_create_reader_table_not_found(self):
        parallelism = 4
        bq_ds = BigQueryDatasource(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset="mockdataset.nonexistenttable",
        )
        with pytest.raises(ValueError) as exception:
            bq_ds.get_read_tasks(parallelism)
        expected_message = (
            "Table mockdataset.nonexistenttable is not found."
            + " Please ensure that it exists."
        )
        assert str(exception.value) == expected_message


class TestWriteBigQuery:
    """Tests for BigQuery Write."""

    def _extract_write_result(self, stats: Iterator[Block]):
        return dict(next(stats).iloc[0])

    def test_write(self, ray_get_mock):
        bq_datasink = BigQueryDatasink(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
        )
        arr = pa.array([2, 4, 5, 100])
        block = pa.Table.from_arrays([arr], names=["data"])
        ctx = TaskContext(1, "")
        bq_datasink.write(
            blocks=[block],
            ctx=ctx,
        )

        collect_stats_fn = generate_collect_write_stats_fn()
        stats = collect_stats_fn([block], ctx)
        pd.testing.assert_frame_equal(
            next(stats),
            pd.DataFrame(
                {
                    "num_rows": [4],
                    "size_bytes": [32],
                    "write_return": [None],
                }
            ),
        )

    def test_write_dataset_exists(self, ray_get_mock):
        bq_datasink = BigQueryDatasink(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset="existingdataset" + "." + _TEST_BQ_TABLE_ID,
        )
        arr = pa.array([2, 4, 5, 100])
        block = pa.Table.from_arrays([arr], names=["data"])
        ctx = TaskContext(1, "")
        bq_datasink.write(
            blocks=[block],
            ctx=ctx,
        )
        collect_stats_fn = generate_collect_write_stats_fn()
        stats = collect_stats_fn([block], ctx)
        pd.testing.assert_frame_equal(
            next(stats),
            pd.DataFrame(
                {
                    "num_rows": [4],
                    "size_bytes": [32],
                    "write_return": [None],
                }
            ),
        )

    def test_write_empty_block(self, ray_get_mock):
        """Test that writing a zero-sized block doesn't crash.

        See https://github.com/ray-project/ray/issues/51892
        """
        bq_datasink = BigQueryDatasink(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
        )
        # Create an empty block with schema but no rows
        block = pa.Table.from_arrays([pa.array([], type=pa.int64())], names=["data"])
        ctx = TaskContext(1, "")
        # This should not raise an error - empty blocks should be skipped
        bq_datasink.write(
            blocks=[block],
            ctx=ctx,
        )

        # write() always calls ray.get(), but with an empty list since the
        # zero-row block is filtered out (no remote write tasks launched).
        ray_get_mock.assert_called_once_with([])


class _CountingClientProvider(BigQueryClientProvider):
    """A provider that records how many clients it built.

    Delegates to ``bigquery.Client`` / ``BigQueryReadClient``, which the autouse
    fixtures above replace with mocks.
    """

    def __init__(self):
        self.build_client_calls = 0
        self.build_read_client_calls = 0

    def _build_client(self):
        self.build_client_calls += 1
        return bigquery.Client(project=_TEST_GCP_PROJECT_ID)

    def _build_read_client(self):
        self.build_read_client_calls += 1
        return bigquery_storage.BigQueryReadClient()


class TestBigQueryClientProvider:
    """Tests for injecting BigQuery clients via a client provider."""

    def test_default_provider_forwards_credentials_and_options(
        self, bq_client_full_mock, bqs_client_full_mock
    ):
        credentials = mock.Mock(name="credentials")
        client_options = mock.Mock(name="client_options")
        provider = DefaultBigQueryClientProvider(
            project_id=_TEST_GCP_PROJECT_ID,
            credentials=credentials,
            client_options=client_options,
        )

        provider.get_client()
        _, kwargs = bq_client_full_mock.call_args
        assert kwargs["project"] == _TEST_GCP_PROJECT_ID
        assert kwargs["credentials"] is credentials
        assert kwargs["client_options"] is client_options
        assert kwargs["client_info"].user_agent.startswith("ray/")

        provider.get_read_client()
        _, kwargs = bqs_client_full_mock.call_args
        assert kwargs["credentials"] is credentials
        assert kwargs["client_options"] is client_options
        assert kwargs["client_info"].user_agent.startswith("ray/")

    def test_default_provider_kwargs_take_precedence(self, bq_client_full_mock):
        override = mock.Mock(name="override_credentials")
        provider = DefaultBigQueryClientProvider(
            project_id=_TEST_GCP_PROJECT_ID,
            credentials=mock.Mock(name="credentials"),
            client_kwargs={"credentials": override, "location": "us-central1"},
        )

        provider.get_client()
        _, kwargs = bq_client_full_mock.call_args
        assert kwargs["credentials"] is override
        assert kwargs["location"] == "us-central1"

    def test_clients_are_built_once_and_cached(self):
        provider = _CountingClientProvider()

        assert provider.get_client() is provider.get_client()
        assert provider.get_read_client() is provider.get_read_client()
        assert provider.build_client_calls == 1
        assert provider.build_read_client_calls == 1

    def test_cached_clients_are_dropped_when_serialized(self):
        """Google's clients aren't picklable, so they must not be pickled.

        See https://github.com/ray-project/ray/issues/65614.
        """
        provider = _CountingClientProvider()
        provider.get_client()
        provider.get_read_client()

        unpickled = pickle.loads(pickle.dumps(provider))

        assert "_cached_client" not in unpickled.__dict__
        assert "_cached_read_client" not in unpickled.__dict__
        # The clients are rebuilt on demand in the receiving process.
        assert unpickled.build_client_calls == 1
        unpickled.get_client()
        assert unpickled.build_client_calls == 2

    def test_read_uses_injected_provider(self):
        provider = _CountingClientProvider()
        bq_ds = BigQueryDatasource(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
            client_provider=provider,
        )

        read_tasks_list = bq_ds.get_read_tasks(2)

        assert len(read_tasks_list) == 2
        assert provider.build_client_calls == 1
        assert provider.build_read_client_calls == 1

    def test_read_tasks_do_not_capture_a_client(self):
        """Read tasks must carry the provider, not a live client."""
        provider = _CountingClientProvider()
        bq_ds = BigQueryDatasource(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
            client_provider=provider,
        )
        bq_ds.get_read_tasks(1)

        # The datasource is shipped to workers, so it must survive a round-trip
        # even after the driver has already built its clients.
        restored = pickle.loads(pickle.dumps(bq_ds))
        assert "_cached_read_client" not in restored._client_provider.__dict__

    def test_write_uses_injected_provider(self):
        provider = _CountingClientProvider()
        bq_datasink = BigQueryDatasink(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
            client_provider=provider,
        )

        bq_datasink.on_write_start()
        assert provider.build_client_calls == 1

        # The datasink is shipped to write tasks, so it must survive a
        # round-trip after the driver has already built its client.
        restored = pickle.loads(pickle.dumps(bq_datasink))
        assert "_cached_client" not in restored.client_provider.__dict__

    def test_defaults_to_default_provider(self):
        bq_ds = BigQueryDatasource(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
        )
        assert isinstance(bq_ds._client_provider, DefaultBigQueryClientProvider)

        bq_datasink = BigQueryDatasink(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
        )
        assert isinstance(bq_datasink.client_provider, DefaultBigQueryClientProvider)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
