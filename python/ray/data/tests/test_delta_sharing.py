import json
import unittest
from unittest import mock
from unittest.mock import patch

from delta_sharing.protocol import Table
from delta_sharing.rest_client import DataSharingRestClient

import ray
from ray.data import Dataset
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.delta_sharing_datasource import DeltaSharingDatasource


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
        "ray.data.datasource.delta_sharing_datasource.DeltaSharingDatasource.\
            setup_delta_sharing_connections"
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
        "ray.data.datasource.delta_sharing_datasource.DeltaSharingDatasource.\
            setup_delta_sharing_connections"
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
        "ray.data.datasource.delta_sharing_datasource.DeltaSharingDatasource.\
            setup_delta_sharing_connections"
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

    @patch("delta_sharing.protocol.DeltaSharingProfile.read_from_file")
    def test_setup_delta_sharing_connections(self, mock_read_from_file):
        # Create a mock profile object with necessary attributes
        mock_profile = mock.Mock()
        mock_profile.share_credentials_version = "1.0"
        mock_profile.endpoint = "https://example.com"

        mock_read_from_file.return_value = mock_profile

        profile_path = "path/to/profile"
        url = f"{profile_path}#share.schema.table"

        # Call the setup_delta_sharing_connections method
        table, rest_client = DeltaSharingDatasource.setup_delta_sharing_connections(url)

        # Assertions
        self.assertEqual(table.name, "table")
        self.assertEqual(table.share, "share")
        self.assertEqual(table.schema, "schema")
        self.assertIsInstance(rest_client, DataSharingRestClient)


class TestReadDeltaSharingTables(unittest.TestCase):
    def setUp(self):
        self.url = "path/to/profile#share.schema.table"
        self.limit = 1000
        self.version = 1
        self.jsonPredicateHints = '{"column":"value"}'
        self.ray_remote_args = {"num_cpus": 1}
        self.concurrency = 2
        self.override_num_blocks = 4

    @patch("ray.data.datasource.delta_sharing_datasource.DeltaSharingDatasource")
    @patch("ray.data.read_datasource")
    def test_read_delta_sharing_tables(self, mock_read_datasource, mock_datasource):
        # Mock the DeltaSharingDatasource constructor to return a mock object
        mock_datasource_instance = mock_datasource.return_value

        # Mock the response from read_datasource
        mock_read_datasource.return_value = mock.create_autospec(Dataset)

        # Call the function with the mocked inputs
        ds = ray.data.read_delta_sharing_tables(
            url=self.url,
            limit=self.limit,
            version=self.version,
            jsonPredicateHints=self.jsonPredicateHints,
            ray_remote_args=self.ray_remote_args,
            concurrency=self.concurrency,
            override_num_blocks=self.override_num_blocks,
        )

        # Assert the correct calls were made
        mock_datasource.assert_called_once_with(
            url=self.url,
            jsonPredicateHints=self.jsonPredicateHints,
            limit=self.limit,
            version=self.version,
            timestamp=None,
        )

        mock_read_datasource.assert_called_once_with(
            datasource=mock_datasource_instance,
            ray_remote_args=self.ray_remote_args,
            concurrency=self.concurrency,
            override_num_blocks=self.override_num_blocks,
        )

        # Assert the returned object is a Dataset
        self.assertIsInstance(ds, Dataset)


if __name__ == "__main__":
    unittest.main()
