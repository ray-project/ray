import json
import unittest
from typing import TYPE_CHECKING, Optional
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from delta_sharing.protocol import Table
from delta_sharing.rest_client import DataSharingRestClient

from ray.data._internal.datasource.delta_sharing_datasource import (
    DeltaSharingDatasource,
    _parse_delta_sharing_url,
)
from ray.data.block import BlockMetadata
from ray.data.dataset import Dataset
from ray.data.datasource.datasource import ReadTask
from ray.data.read_api import read_delta_sharing_tables

if TYPE_CHECKING:
    from ray.data.context import DataContext


class TestDeltaSharingDatasource(unittest.TestCase):
    def setUp(self):
        self.url = "path/to/profile#share.schema.table"
        self.limit = 1000
        self.version = 1
        self.json_predicate_hints = '{"column":"value"}'
        self.table = Table(name="table", share="share", schema="schema")

        self.mock_rest_client = mock.create_autospec(DataSharingRestClient)
        self.mock_response = mock.Mock()
        self.mock_rest_client.list_files_in_table.return_value = self.mock_response

        self.mock_response.add_files = [
            {"url": "file1", "id": "1"},
            {"url": "file2", "id": "2"},
        ]
        self.mock_response.metadata.schema_string = json.dumps(
            {
                "type": "struct",
                "fields": [
                    {
                        "name": "column1",
                        "type": "string",
                        "nullable": True,
                        "metadata": {},
                    }
                ],
            }
        )

    @patch(
        "ray.data._internal.datasource.delta_sharing_datasource.DeltaSharingDatasource."
        "setup_delta_sharing_connections"
    )
    def test_init(self, mock_setup_delta_sharing_connections):
        mock_setup_delta_sharing_connections.return_value = (
            self.table,
            self.mock_rest_client,
        )
        datasource = DeltaSharingDatasource(
            url=self.url,
            json_predicate_hints=self.json_predicate_hints,
            limit=self.limit,
            version=self.version,
            timestamp=None,
        )

        self.assertEqual(datasource._url, self.url)
        self.assertEqual(datasource._json_predicate_hints, self.json_predicate_hints)
        self.assertEqual(datasource._limit, self.limit)
        self.assertEqual(datasource._version, self.version)
        self.assertEqual(datasource._timestamp, None)

    @patch(
        "ray.data._internal.datasource.delta_sharing_datasource.DeltaSharingDatasource."
        "setup_delta_sharing_connections"
    )
    def test_get_read_tasks(self, mock_setup_delta_sharing_connections):
        mock_setup_delta_sharing_connections.return_value = (
            self.table,
            self.mock_rest_client,
        )
        datasource = DeltaSharingDatasource(
            url=self.url,
            json_predicate_hints=self.json_predicate_hints,
            limit=self.limit,
            version=self.version,
            timestamp=None,
        )

        read_tasks = datasource.get_read_tasks(parallelism=2)
        self.assertEqual(len(read_tasks), 2)
        self.assertTrue(all(isinstance(task, ReadTask) for task in read_tasks))

        for task in read_tasks:
            metadata = task.metadata
            self.assertIsInstance(metadata, BlockMetadata)
            self.assertEqual(len(metadata.input_files), 1)
            self.assertTrue(metadata.input_files[0]["url"] in ["file1", "file2"])
            self.assertEqual(metadata.num_rows, None)
            self.assertEqual(metadata.size_bytes, None)
            self.assertEqual(task.schema, None)
            self.assertEqual(metadata.exec_stats, None)


class TestParseDeltaSharingUrl(unittest.TestCase):
    def test_valid_url(self):
        url = "profile#share.schema.table"
        expected_result = ("profile", "share", "schema", "table")
        self.assertEqual(_parse_delta_sharing_url(url), expected_result)

    def test_missing_hash(self):
        url = "profile-share.schema.table"
        with self.assertRaises(ValueError) as context:
            _parse_delta_sharing_url(url)
        self.assertEqual(str(context.exception), f"Invalid 'url': {url}")

    def test_missing_fragments(self):
        url = "profile#share.schema"
        with self.assertRaises(ValueError) as context:
            _parse_delta_sharing_url(url)
        self.assertEqual(str(context.exception), f"Invalid 'url': {url}")

    def test_empty_profile(self):
        url = "#share.schema.table"
        with self.assertRaises(ValueError) as context:
            _parse_delta_sharing_url(url)
        self.assertEqual(str(context.exception), f"Invalid 'url': {url}")

    def test_empty_share(self):
        url = "profile#.schema.table"
        with self.assertRaises(ValueError) as context:
            _parse_delta_sharing_url(url)
        self.assertEqual(str(context.exception), f"Invalid 'url': {url}")

    def test_empty_schema(self):
        url = "profile#share..table"
        with self.assertRaises(ValueError) as context:
            _parse_delta_sharing_url(url)
        self.assertEqual(str(context.exception), f"Invalid 'url': {url}")

    def test_empty_table(self):
        url = "profile#share.schema."
        with self.assertRaises(ValueError) as context:
            _parse_delta_sharing_url(url)
        self.assertEqual(str(context.exception), f"Invalid 'url': {url}")


class MockDeltaSharingDatasource:
    def __init__(
        self, url, json_predicate_hints=None, limit=None, version=None, timestamp=None
    ):
        self._url = url
        self._json_predicate_hints = json_predicate_hints
        self._limit = limit
        self._version = version
        self._timestamp = timestamp

    def setup_delta_sharing_connections(self, url):
        # Return mock objects for table and rest_client
        table = MagicMock()
        rest_client = MagicMock()

        # Mock the rest_client's list_files_in_table method
        rest_client.list_files_in_table.return_value = MagicMock(
            add_files=[
                {
                    "url": "https://s3-bucket-name.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet",  # noqa E501
                    "id": "591723a8-6a27-4240-a90e-57426f4736d2",
                    "size": 573,
                    "partitionValues": {"date": "2021-04-28"},
                    "stats": '{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"nullCount":{"eventTime":0}}',  # noqa E501
                    "expirationTimestamp": 1652140800000,
                }
            ],
            metadata=MagicMock(
                schema_string='{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}'  # noqa E501
            ),
        )
        return table, rest_client

    def get_read_tasks(
        self,
        parallelism: int,
        per_task_row_limit: Optional[int] = None,
        data_context: Optional["DataContext"] = None,
    ):
        self._table, self._rest_client = self.setup_delta_sharing_connections(self._url)
        response = self._rest_client.list_files_in_table(
            self._table,
            jsonPredicateHints=self._json_predicate_hints,
            limitHint=self._limit,
            version=self._version,
            timestamp=self._timestamp,
        )

        read_tasks = []
        for _ in range(parallelism):
            read_task = MagicMock()
            read_task.metadata = MagicMock(
                num_rows=1,
                schema=None,
                input_files=[file["url"] for file in response.add_files],
                size_bytes=573,
                exec_stats=None,
            )
            read_task.data = MagicMock(
                return_value=[
                    {
                        "eventTime": "2021-04-28T23:33:48.719Z",
                        "date": "2021-04-28",
                    }
                ]
            )
            read_task.per_task_row_limit = per_task_row_limit
            read_tasks.append(read_task)
        return read_tasks


@pytest.fixture
def mock_delta_sharing_datasource(mocker):
    mock_datasource = mocker.patch(
        "ray.data._internal.datasource.delta_sharing_datasource.DeltaSharingDatasource",
        new=MockDeltaSharingDatasource,
    )
    return mock_datasource


@pytest.fixture
def mock_ray_data_read_datasource(mocker):
    mock_read_datasource = mocker.patch("ray.data.read_datasource")
    mock_read_datasource.return_value = MagicMock(spec=Dataset)
    return mock_read_datasource


@pytest.fixture
def setup_profile_file(tmpdir):
    profile_content = {
        "shareCredentialsVersion": 1,
        "endpoint": "https://sharing.delta.io/delta-sharing/",
        "bearerToken": "<token>",
        "expirationTime": "2021-11-12T00:12:29.0Z",
    }
    profile_file = tmpdir.join("profile.json")
    profile_file.write(json.dumps(profile_content))
    return str(profile_file)


def test_read_delta_sharing_tables(
    mock_delta_sharing_datasource, mock_ray_data_read_datasource, setup_profile_file
):
    url = f"{setup_profile_file}#share.schema.table"
    limit = 100
    version = 1
    timestamp = "2021-01-01T00:00:00Z"
    json_predicate_hints = '{"eventTime": "2021-04-28T23:33:48.719Z"}'
    ray_remote_args = {"num_cpus": 2}
    concurrency = 4
    override_num_blocks = 2

    # Call the function under test
    result = read_delta_sharing_tables(
        url=url,
        limit=limit,
        version=version,
        timestamp=timestamp,
        json_predicate_hints=json_predicate_hints,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )

    # Assert the result and interactions
    assert isinstance(result, Dataset)
    mock_ray_data_read_datasource.assert_called_once()
    args, kwargs = mock_ray_data_read_datasource.call_args
    datasource = kwargs["datasource"]
    assert datasource._url == url
    assert datasource._json_predicate_hints == json_predicate_hints
    assert datasource._limit == limit
    assert datasource._version == version
    assert datasource._timestamp == timestamp
    assert kwargs["ray_remote_args"] == ray_remote_args
    assert kwargs["concurrency"] == concurrency
    assert kwargs["override_num_blocks"] == override_num_blocks


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
