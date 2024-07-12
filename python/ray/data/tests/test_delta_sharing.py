import json
import unittest
from unittest import mock
from unittest.mock import patch

import pytest
from delta_sharing.protocol import Table
from delta_sharing.rest_client import DataSharingRestClient

from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.delta_sharing_datasource import (
    DeltaSharingDatasource,
    _parse_delta_sharing_url,
)


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
            json_predicate_hints=self.json_predicate_hints,
            limit=self.limit,
            version=self.version,
            timestamp=None,
        )

        self.assertEqual(datasource._table, self.table)
        self.assertEqual(datasource._rest_client, self.mock_rest_client)
        self.assertEqual(datasource._json_predicate_hints, self.json_predicate_hints)
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
            json_predicate_hints=self.json_predicate_hints,
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
                json_predicate_hints=self.json_predicate_hints,
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
