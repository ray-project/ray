from unittest import mock

import pytest
from google.api_core import exceptions, operation
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import job
from google.cloud.bigquery_storage_v1.types import stream as gcbqs_stream

import ray
from ray.data.datasource import BigQueryDatasource
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa

_TEST_GCP_PROJECT_ID = "mock-test-project-id"
_TEST_BQ_DATASET_ID = "mockdataset"
_TEST_BQ_TABLE_ID = "mocktable"
_TEST_BQ_DATASET = _TEST_BQ_DATASET_ID + "." + _TEST_BQ_TABLE_ID
_TEST_BQ_TEMP_DESTINATION = _TEST_GCP_PROJECT_ID + ".tempdataset.temptable"
_TEST_DISPLAY_NAME = "display_name"


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
def ray_remote_function_mock():
    with mock.patch.object(ray.remote_function.RemoteFunction, "_remote") as remote_fn:
        remote_fn.return_value = 1
        yield remote_fn


class TestReadBigQuery:
    """Tests for BigQuery Read."""

    @pytest.mark.parametrize(
        "parallelism",
        [1, 2, 3, 4, 10, 100],
    )
    def test_create_reader(self, parallelism):
        bq_ds = BigQueryDatasource()
        reader = bq_ds.create_reader(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
            parallelism=parallelism,
        )
        read_tasks_list = reader.get_read_tasks(parallelism)
        assert len(read_tasks_list) == parallelism

    @pytest.mark.parametrize(
        "parallelism",
        [1, 2, 3, 4, 10, 100],
    )
    def test_create_reader_query(self, parallelism, bq_query_result_mock):
        bq_ds = BigQueryDatasource()
        reader = bq_ds.create_reader(
            project_id=_TEST_GCP_PROJECT_ID,
            parallelism=parallelism,
            query="SELECT * FROM mockdataset.mocktable",
        )
        read_tasks_list = reader.get_read_tasks(parallelism)
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
        bq_ds = BigQueryDatasource()
        reader = bq_ds.create_reader(
            project_id=_TEST_GCP_PROJECT_ID,
            parallelism=parallelism,
            query="SELECT * FROM mockdataset.mocktable",
        )
        with pytest.raises(exceptions.BadRequest):
            reader.get_read_tasks(parallelism)
        bq_query_result_mock_fail.assert_called()

    def test_dataset_query_kwargs_provided(self):
        parallelism = 4
        bq_ds = BigQueryDatasource()
        with pytest.raises(ValueError) as exception:
            bq_ds.create_reader(
                project_id=_TEST_GCP_PROJECT_ID,
                dataset=_TEST_BQ_DATASET,
                query="SELECT * FROM mockdataset.mocktable",
                parallelism=parallelism,
            )
        expected_message = (
            "Query and dataset kwargs cannot both be provided"
            + " (must be mutually exclusive)."
        )
        assert str(exception.value) == expected_message

    def test_create_reader_dataset_not_found(self):
        parallelism = 4
        bq_ds = BigQueryDatasource()
        reader = bq_ds.create_reader(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset="nonexistentdataset.mocktable",
            parallelism=parallelism,
        )
        with pytest.raises(ValueError) as exception:
            reader.get_read_tasks(parallelism)
        expected_message = (
            "Dataset nonexistentdataset is not found. Please ensure that it exists."
        )
        assert str(exception.value) == expected_message

    def test_create_reader_table_not_found(self):
        parallelism = 4
        bq_ds = BigQueryDatasource()
        reader = bq_ds.create_reader(
            project_id=_TEST_GCP_PROJECT_ID,
            dataset="mockdataset.nonexistenttable",
            parallelism=parallelism,
        )
        with pytest.raises(ValueError) as exception:
            reader.get_read_tasks(parallelism)
        expected_message = (
            "Table mockdataset.nonexistenttable is not found."
            + " Please ensure that it exists."
        )
        assert str(exception.value) == expected_message


class TestWriteBigQuery:
    """Tests for BigQuery Write."""

    def test_do_write(self):
        bq_ds = BigQueryDatasource()
        arr = pa.array([2, 4, 5, 100])
        block = pa.Table.from_arrays([arr], names=["data"])
        status = bq_ds.write(
            blocks=[block],
            ctx=None,
            project_id=_TEST_GCP_PROJECT_ID,
            dataset=_TEST_BQ_DATASET,
        )
        assert status == "ok"

    def test_do_write_dataset_exists(self, ray_remote_function_mock):
        bq_ds = BigQueryDatasource()
        arr = pa.array([2, 4, 5, 100])
        block = pa.Table.from_arrays([arr], names=["data"])
        status = bq_ds.write(
            blocks=[block],
            ctx=None,
            project_id=_TEST_GCP_PROJECT_ID,
            dataset="existingdataset" + "." + _TEST_BQ_TABLE_ID,
        )
        assert status == "ok"
