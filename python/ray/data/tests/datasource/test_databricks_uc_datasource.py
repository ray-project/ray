"""Tests for Databricks Unity Catalog datasource."""

import os
import re
import tempfile
import uuid
from collections import namedtuple
from contextlib import contextmanager
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

import ray
import ray.cloudpickle as pickle
from ray.tests.conftest import *  # noqa


def test_databricks_uc_datasource():
    MockResponse = namedtuple("Response", "raise_for_status json content")

    MockChunk = namedtuple("Chunk", "index, row_count byte_count data")

    token = "test_token"
    warehouse_id = "test_warehouse_id"
    catalog = "catalog1"
    schema = "db1"
    query = "select * from table1"
    expected_result_df = pd.DataFrame(
        {
            "c1": range(10000),
            "c2": map(lambda x: "str" + str(x), range(10000)),
        }
    )
    rows_per_chunk = 700

    @contextmanager
    def setup_mock():
        mock_chunks = []

        num_rows = len(expected_result_df)
        cur_pos = 0
        index = 0

        while cur_pos < num_rows:
            if cur_pos + rows_per_chunk <= num_rows:
                chunk_rows = rows_per_chunk
            else:
                chunk_rows = num_rows - cur_pos

            chunk_df = expected_result_df[cur_pos : (cur_pos + chunk_rows)]
            chunk_pa_table = pa.Table.from_pandas(chunk_df)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, chunk_pa_table.schema) as writer:
                writer.write_table(chunk_pa_table)

            chunk_data = sink.getvalue()

            mock_chunks.append(
                MockChunk(
                    index=index,
                    row_count=chunk_rows,
                    byte_count=len(chunk_data),
                    data=chunk_data,
                )
            )
            index += 1
            cur_pos += rows_per_chunk

        chunk_meta_json = [
            {
                "chunk_index": index,
                "row_count": mock_chunk.row_count,
                "byte_count": mock_chunk.byte_count,
            }
            for index, mock_chunk in enumerate(mock_chunks)
        ]
        chunk_meta_json.reverse()
        valid_statement_ids = set()

        def request_post_mock(url, data=None, json=None, **kwargs):
            import json as jsonlib

            headers = kwargs["headers"]

            if url == "https://test_shard/api/2.0/sql/statements/":
                assert headers == {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + token,
                }
                assert jsonlib.loads(data) == {
                    "statement": query,
                    "warehouse_id": warehouse_id,
                    "wait_timeout": "0s",
                    "disposition": "EXTERNAL_LINKS",
                    "format": "ARROW_STREAM",
                    "catalog": catalog,
                    "schema": schema,
                }

                statement_id = uuid.uuid4().hex
                valid_statement_ids.add(statement_id)

                return MockResponse(
                    raise_for_status=lambda: None,
                    json=lambda: {
                        "statement_id": statement_id,
                        "status": {"state": "PENDING"},
                    },
                    content=b"",
                )

            assert False, "Invalid request."

        def request_get_mock(url, params=None, **kwargs):
            headers = kwargs["headers"]

            if match := re.match(
                r"^https://test_shard/api/2\.0/sql/statements/([^/]*)/$", url
            ):
                statement_id = match.group(1)
                assert headers == {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + token,
                }

                assert statement_id in valid_statement_ids

                return MockResponse(
                    raise_for_status=lambda: None,
                    json=lambda: {
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {
                            "truncated": False,
                            "chunks": chunk_meta_json,
                        },
                    },
                    content=None,
                )

            if match := re.match(
                r"^https://test_shard/api/2\.0/sql/"
                r"statements/([^/]*)/result/chunks/([^/]*)$",
                url,
            ):
                assert headers == {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + token,
                }

                chunk_index = match.group(2)

                external_link = f"https://test_external_link/{chunk_index}"
                return MockResponse(
                    raise_for_status=lambda: None,
                    json=lambda: {
                        "external_links": [
                            {
                                "external_link": external_link,
                            }
                        ]
                    },
                    content=None,
                )

            if match := re.match(r"^https://test_external_link/([^/]*)$", url):
                assert headers is None

                chunk_index = int(match.group(1))

                return MockResponse(
                    raise_for_status=lambda: None,
                    json=lambda: None,
                    content=mock_chunks[chunk_index].data,
                )

            assert False, "Invalid request."

        with (
            mock.patch("requests.get", request_get_mock),
            mock.patch("requests.post", request_post_mock),
            mock.patch.dict(
                os.environ,
                {
                    "DATABRICKS_HOST": "test_shard",
                    "DATABRICKS_TOKEN": token,
                },
            ),
        ):
            yield

    setup_mock_fn_path = os.path.join(tempfile.mkdtemp(), "setup_mock_fn.pkl")
    with open(setup_mock_fn_path, "wb") as fp:
        pickle.dump(setup_mock, fp)

    MOCK_ENV = "RAY_DATABRICKS_UC_DATASOURCE_READ_FN_MOCK_TEST_SETUP_FN_PATH"
    with setup_mock(), mock.patch.dict(os.environ, {MOCK_ENV: setup_mock_fn_path}):
        # shut down existing Ray local cluster and
        # recreate Ray local cluster so that Ray head node could get the
        # "MOCK_ENV"
        ray.shutdown()
        ray.init()

        # test query with a table name
        result = (
            ray.data.read_databricks_tables(
                warehouse_id=warehouse_id,
                table="table1",
                catalog="catalog1",
                schema="db1",
                override_num_blocks=5,
            )
            .to_pandas()
            .sort_values("c1")
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(result, expected_result_df)

        # test query with SQL
        result = (
            ray.data.read_databricks_tables(
                warehouse_id=warehouse_id,
                query="select * from table1",
                catalog="catalog1",
                schema="db1",
                override_num_blocks=5,
            )
            .to_pandas()
            .sort_values("c1")
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(result, expected_result_df)

        # test larger parallelism
        result = (
            ray.data.read_databricks_tables(
                warehouse_id=warehouse_id,
                query="select * from table1",
                catalog="catalog1",
                schema="db1",
                override_num_blocks=100,
            )
            .to_pandas()
            .sort_values("c1")
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(result, expected_result_df)


def test_databricks_uc_datasource_with_credential_provider():
    """Test DatabricksUCDatasource with credential_provider parameter."""
    from ray.data._internal.datasource.databricks_credentials import (
        StaticCredentialProvider,
    )
    from ray.data._internal.datasource.databricks_uc_datasource import (
        DatabricksUCDatasource,
    )

    with (
        mock.patch("requests.get") as mock_get,
        mock.patch("requests.post") as mock_post,
    ):
        mock_post.return_value = mock.Mock(
            raise_for_status=lambda: None,
            json=lambda: {"statement_id": "test_stmt", "status": {"state": "PENDING"}},
        )
        mock_get.return_value = mock.Mock(
            raise_for_status=lambda: None,
            json=lambda: {
                "status": {"state": "SUCCEEDED"},
                "manifest": {"truncated": False},
            },
        )

        provider = StaticCredentialProvider(token="my_provider_token", host="test_host")
        datasource = DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=provider,
        )

        # Verify the token from provider was used in requests
        call_kwargs = mock_post.call_args[1]
        assert "Authorization" in call_kwargs["headers"]
        assert "Bearer my_provider_token" in call_kwargs["headers"]["Authorization"]


def test_databricks_uc_datasource_401_retry():
    """Test that 401 response triggers credential invalidation and retry."""
    from ray.data._internal.datasource.databricks_credentials import (
        DatabricksCredentialProvider,
    )
    from ray.data._internal.datasource.databricks_uc_datasource import (
        DatabricksUCDatasource,
    )

    # Create a mock credential provider that tracks invalidate() calls
    class MockCredentialProvider(DatabricksCredentialProvider):
        def __init__(self):
            self.invalidate_count = 0
            self.tokens = ["old_token", "new_token"]
            self.token_index = 0

        def get_token(self) -> str:
            return self.tokens[min(self.token_index, len(self.tokens) - 1)]

        def get_host(self):
            return "test_host"

        def invalidate(self) -> None:
            self.invalidate_count += 1
            self.token_index += 1

    mock_provider = MockCredentialProvider()

    with (
        mock.patch("requests.get") as mock_get,
        mock.patch("requests.post") as mock_post,
    ):
        # Mock POST for statement creation
        mock_post.return_value = mock.Mock(
            raise_for_status=lambda: None,
            json=lambda: {"statement_id": "test_stmt", "status": {"state": "PENDING"}},
        )

        # Mock GET for status check - returns SUCCEEDED with one chunk
        def get_side_effect(url, *args, **kwargs):
            if "/result/chunks/" in url:
                # Chunk resolution request - return 401 first time, then succeed
                headers = kwargs.get("headers", {})
                auth_header = headers.get("Authorization", "")

                if "old_token" in auth_header:
                    # First attempt with old token - return 401
                    resp = mock.Mock()
                    resp.status_code = 401
                    resp.raise_for_status = mock.Mock(
                        side_effect=Exception("401 Unauthorized")
                    )
                    return resp
                else:
                    # Retry with new token - return success
                    resp = mock.Mock()
                    resp.status_code = 200
                    resp.raise_for_status = lambda: None
                    resp.json = lambda: {
                        "external_links": [{"external_link": "https://external/data"}]
                    }
                    return resp
            else:
                # Status check request
                return mock.Mock(
                    status_code=200,
                    raise_for_status=lambda: None,
                    json=lambda: {
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {
                            "truncated": False,
                            "chunks": [
                                {"chunk_index": 0, "row_count": 10, "byte_count": 100}
                            ],
                        },
                    },
                )

        mock_get.side_effect = get_side_effect

        datasource = DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=mock_provider,
        )

        # Verify datasource was created (this doesn't trigger chunk reads yet)
        assert datasource.num_chunks == 1

        # Verify invalidate was not called during datasource creation
        assert mock_provider.invalidate_count == 0


def test_databricks_uc_datasource_fresh_token_per_request():
    """Test that fresh tokens are fetched for each request during polling."""
    from ray.data._internal.datasource.databricks_credentials import (
        DatabricksCredentialProvider,
    )
    from ray.data._internal.datasource.databricks_uc_datasource import (
        DatabricksUCDatasource,
    )

    # Create a credential provider that returns incrementing tokens
    class TokenTrackingProvider(DatabricksCredentialProvider):
        def __init__(self):
            self.token_fetch_count = 0

        def get_token(self) -> str:
            self.token_fetch_count += 1
            return f"token_{self.token_fetch_count}"

        def get_host(self) -> str:
            return "test_host"

        def invalidate(self) -> None:
            pass

    provider = TokenTrackingProvider()

    with (
        mock.patch("requests.get") as mock_get,
        mock.patch("requests.post") as mock_post,
    ):
        # Track tokens used in requests
        tokens_used = []

        def capture_post(url, *args, **kwargs):
            tokens_used.append(kwargs["headers"]["Authorization"])
            return mock.Mock(
                raise_for_status=lambda: None,
                json=lambda: {
                    "statement_id": "test_stmt",
                    "status": {"state": "PENDING"},
                },
            )

        poll_count = [0]

        def capture_get(url, *args, **kwargs):
            tokens_used.append(kwargs["headers"]["Authorization"])
            poll_count[0] += 1
            # Return PENDING twice, then SUCCEEDED
            if poll_count[0] < 3:
                state = "PENDING"
            else:
                state = "SUCCEEDED"
            return mock.Mock(
                status_code=200,
                raise_for_status=lambda: None,
                json=lambda: {
                    "status": {"state": state},
                    "manifest": {"truncated": False, "chunks": []},
                },
            )

        mock_post.side_effect = capture_post
        mock_get.side_effect = capture_get

        DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=provider,
        )

        # Verify fresh token was fetched for each request:
        # 1 POST (statement creation) + 3 GETs (polling)
        assert provider.token_fetch_count == 4
        assert tokens_used == [
            "Bearer token_1",  # POST
            "Bearer token_2",  # GET poll 1
            "Bearer token_3",  # GET poll 2
            "Bearer token_4",  # GET poll 3
        ]


def test_databricks_uc_datasource_401_retry_during_polling():
    """Test that 401 during polling triggers credential invalidation and retry."""
    from ray.data._internal.datasource.databricks_credentials import (
        DatabricksCredentialProvider,
    )
    from ray.data._internal.datasource.databricks_uc_datasource import (
        DatabricksUCDatasource,
    )

    class RefreshableProvider(DatabricksCredentialProvider):
        def __init__(self):
            self.invalidate_count = 0
            self.current_token = "expired_token"

        def get_token(self) -> str:
            return self.current_token

        def get_host(self) -> str:
            return "test_host"

        def invalidate(self) -> None:
            self.invalidate_count += 1
            self.current_token = "refreshed_token"

    provider = RefreshableProvider()

    with (
        mock.patch("requests.get") as mock_get,
        mock.patch("requests.post") as mock_post,
    ):
        mock_post.return_value = mock.Mock(
            raise_for_status=lambda: None,
            json=lambda: {
                "statement_id": "test_stmt",
                "status": {"state": "PENDING"},
            },
        )

        poll_count = [0]

        def get_side_effect(url, *args, **kwargs):
            poll_count[0] += 1
            headers = kwargs.get("headers", {})
            auth_header = headers.get("Authorization", "")

            # First poll returns 401 with expired token
            if poll_count[0] == 1 and "expired_token" in auth_header:
                return mock.Mock(
                    status_code=401,
                    raise_for_status=mock.Mock(side_effect=Exception("401")),
                )
            # Retry or subsequent polls succeed
            return mock.Mock(
                status_code=200,
                raise_for_status=lambda: None,
                json=lambda: {
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"truncated": False, "chunks": []},
                },
            )

        mock_get.side_effect = get_side_effect

        DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=provider,
        )

        # Verify invalidate was called once during polling
        assert provider.invalidate_count == 1
        assert provider.current_token == "refreshed_token"


def test_databricks_uc_datasource_empty_result():
    with (
        mock.patch("requests.get") as mock_get,
        mock.patch("requests.post") as mock_post,
    ):
        #  Mock the POST request starting the query
        def post_mock(url, *args, **kwargs):
            class Resp:
                def raise_for_status(self):
                    pass

                def json(self):
                    return {"statement_id": "test_stmt", "status": {"state": "PENDING"}}

            return Resp()

        # Mock the GET request returning no chunks key to simulate empty result
        def get_mock(url, *args, **kwargs):
            class Resp:
                def raise_for_status(self):
                    pass

                def json(self):
                    return {
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {"truncated": False},
                    }

            return Resp()

        mock_post.side_effect = post_mock
        mock_get.side_effect = get_mock

        with mock.patch.dict(
            os.environ,
            {"DATABRICKS_HOST": "test_host", "DATABRICKS_TOKEN": "test_token"},
        ):

            # Call with dummy query to hit mocked flow
            ds = ray.data.read_databricks_tables(
                warehouse_id="dummy_warehouse",
                query="select * from dummy_table",
                catalog="dummy_catalog",
                schema="dummy_schema",
                override_num_blocks=1,
            )

            assert ds.count() == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
