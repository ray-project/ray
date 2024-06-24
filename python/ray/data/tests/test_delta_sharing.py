import json
import unittest
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from delta_sharing.protocol import Table
from delta_sharing.rest_client import DataSharingRestClient

from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.delta_sharing_datasource import (
    DeltaSharingDatasource,
    _parse_delta_sharing_url,
)
from ray.data.read_api import read_delta_sharing_tables


class TestDeltaSharingDatasource(unittest.TestCase):
    def setUp(self):
        self.url = "path/to/profile#share.schema.table"
        self.limit = 1000
        self.version = 1
        self.jsonPredicateHints = '{"column":"value"}'
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
        "ray.data.datasource.delta_sharing_datasource.DeltaSharingDatasource."
        "setup_delta_sharing_connections"
    )
    def test_init(self, mock_setup_delta_sharing_connections):
        mock_setup_delta_sharing_connections.return_value = (
            self.table,
            self.mock_rest_client,
        )
        datasource = DeltaSharingDatasource(
            url=self.url,
            jsonPredicateHints=self.jsonPredicateHints,
            limit=self.limit,
            version=self.version,
            timestamp=None,
        )

        self.assertEqual(datasource._table, self.table)
        self.assertEqual(datasource._rest_client, self.mock_rest_client)
        self.assertEqual(datasource._jsonPredicateHints, self.jsonPredicateHints)
        self.assertEqual(datasource._limit, self.limit)
        self.assertEqual(datasource._version, self.version)
        self.assertEqual(datasource._timestamp, None)

    @patch(
        "ray.data.datasource.delta_sharing_datasource.DeltaSharingDatasource."
        "setup_delta_sharing_connections"
    )
    def test_get_read_tasks(self, mock_setup_delta_sharing_connections):
        mock_setup_delta_sharing_connections.return_value = (
            self.table,
            self.mock_rest_client,
        )
        datasource = DeltaSharingDatasource(
            url=self.url,
            jsonPredicateHints=self.jsonPredicateHints,
            limit=self.limit,
            version=self.version,
            timestamp=None,
        )

        read_tasks = datasource.get_read_tasks(parallelism=2)
        self.assertEqual(len(read_tasks), 2)
        self.assertTrue(all(isinstance(task, ReadTask) for task in read_tasks))

    @patch(
        "ray.data.datasource.delta_sharing_datasource.DeltaSharingDatasource."
        "setup_delta_sharing_connections"
    )
    def test_empty_files_warning(self, mock_setup_delta_sharing_connections):
        self.mock_response.add_files = []
        mock_setup_delta_sharing_connections.return_value = (
            self.table,
            self.mock_rest_client,
        )
        with self.assertLogs(
            "ray.data.datasource.delta_sharing_datasource", level="WARNING"
        ) as cm:
            DeltaSharingDatasource(
                url=self.url,
                jsonPredicateHints=self.jsonPredicateHints,
                limit=self.limit,
                version=self.version,
                timestamp=None,
            )
            self.assertIn(
                "No files found from the delta sharing table or limit is 0",
                cm.output[0],
            )


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


@pytest.fixture
def mock_delta_sharing_datasource(mocker):
    mock_datasource = mocker.patch(
        "ray.data.datasource.delta_sharing_datasource.DeltaSharingDatasource"
    )
    mock_datasource_instance = mock_datasource.return_value
    mock_datasource_instance.setup_delta_sharing_connections.return_value = (
        MagicMock(),
        MagicMock(),
    )
    mock_datasource_instance._response = MagicMock()
    mock_datasource_instance._response.add_files = [
        {
            "file": {
                "url": "https://s3-bucket-name.s3.us-west-2.amazonaws.com/delta-exchange-test/table2/date%3D2021-04-28/part-00000-591723a8-6a27-4240-a90e-57426f4736d2.c000.snappy.parquet",  # noqa E501
                "id": "591723a8-6a27-4240-a90e-57426f4736d2",
                "size": 573,
                "partitionValues": {"date": "2021-04-28"},
                "stats": '{"numRecords":1,"minValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"maxValues":{"eventTime":"2021-04-28T23:33:48.719Z"},"nullCount":{"eventTime":0}}',  # noqa E501
                "expirationTimestamp": 1652140800000,
            }
        }
    ]
    mock_datasource_instance._response.metadata = MagicMock()
    mock_datasource_instance._response.metadata.schema_string = '{"type":"struct","fields":[{"name":"eventTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"date","type":"date","nullable":true,"metadata":{}}]}'  # noqa E501
    return mock_datasource


@pytest.fixture
def mock_to_pandas(mocker):
    return mocker.patch("delta_sharing.reader.DeltaSharingReader._to_pandas")


@pytest.fixture
def mock_rest_client(mocker):
    mock_rest_client = mocker.patch("delta_sharing.rest_client.DataSharingRestClient")
    mock_rest_client_instance = mock_rest_client.return_value
    mock_rest_client_instance.list_files_in_table.return_value = MagicMock(
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
    return mock_rest_client


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
    mock_delta_sharing_datasource, mock_to_pandas, mock_rest_client, setup_profile_file
):
    # Mock the pandas DataFrame returned by _to_pandas
    mock_to_pandas.return_value = MagicMock()

    # Call the function with test parameters
    dataset = read_delta_sharing_tables(
        url=f"{setup_profile_file}#share.schema.table",
        limit=10,
        version=1,
        timestamp="2021-11-12T00:12:29.0Z",
    )

    # Validate the results
    mock_delta_sharing_datasource.assert_called_once_with(
        url=f"{setup_profile_file}#share.schema.table",
        jsonPredicateHints="{}",
        limit=10,
        version=1,
        timestamp="2021-11-12T00:12:29.0Z",
    )
    assert mock_to_pandas.called
    assert isinstance(dataset, MagicMock)


if __name__ == "__main__":
    unittest.main()
