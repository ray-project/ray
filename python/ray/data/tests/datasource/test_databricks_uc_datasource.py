import json
import os
import re
import socket
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest
import requests

import ray
import ray.cloudpickle as pickle
from ray.data._internal.datasource.databricks_credentials import (
    DatabricksCredentialProvider,
    StaticCredentialProvider,
)
from ray.data._internal.datasource.databricks_uc_datasource import (
    DatabricksUCDatasource,
    _validate_external_url,
)
from ray.data._internal.util import rows_same
from ray.data.tests.datasource.databricks_test_utils import (
    MockResponse,
    RefreshableCredentialProvider,
)
from ray.tests.conftest import *  # noqa: F403

# =============================================================================
# Dataclasses for mock objects
# =============================================================================


@dataclass
class MockChunk:
    """Mock chunk data for testing."""

    index: int
    row_count: int
    byte_count: int
    data: bytes


# =============================================================================
# Mock credential providers for testing
# =============================================================================


class TokenTrackingProvider(DatabricksCredentialProvider):
    """A credential provider that returns incrementing tokens to track fetches."""

    def __init__(self):
        self.token_fetch_count = 0

    def get_token(self) -> str:
        self.token_fetch_count += 1
        return f"token_{self.token_fetch_count}"

    def get_host(self) -> str:
        return "test_host"

    def invalidate(self) -> None:
        pass


# =============================================================================
# Pytest fixtures
# =============================================================================


@pytest.fixture
def databricks_env():
    """Fixture that sets up Databricks environment variables."""
    with mock.patch.dict(
        os.environ,
        {"DATABRICKS_HOST": "test_host", "DATABRICKS_TOKEN": "test_token"},
    ):
        yield


@pytest.fixture
def refreshable_credential_provider():
    """Fixture that provides a refreshable credential provider."""
    return RefreshableCredentialProvider(host="test_host")


@pytest.fixture
def token_tracking_provider():
    """Fixture that provides a token tracking credential provider."""
    return TokenTrackingProvider()


@pytest.fixture
def requests_mocker():
    """Fixture that mocks requests.get and requests.post."""
    with (
        mock.patch("requests.get") as mock_get,
        mock.patch("requests.post") as mock_post,
        mock.patch("socket.getaddrinfo") as mock_dns,
    ):

        # Return a decoy public IP so fake test hostnames pass SSRF validation
        # and existing integration tests can execute normally.
        mock_dns.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))
        ]

        yield {"get": mock_get, "post": mock_post, "dns": mock_dns}


@pytest.fixture
def test_data():
    """Fixture that provides test DataFrame and configuration."""
    return {
        "expected_df": pd.DataFrame(
            {
                "c1": range(10000),
                "c2": [f"str{i}" for i in range(10000)],
            }
        ),
        "token": "test_token",
        "warehouse_id": "test_warehouse_id",
        "catalog": "catalog1",
        "schema": "db1",
        "query": "select * from table1",
        "rows_per_chunk": 700,
    }


# =============================================================================
# Helper functions
# =============================================================================


def create_mock_chunks(df: pd.DataFrame, rows_per_chunk: int) -> list[MockChunk]:
    """Create mock chunks from a DataFrame."""
    chunks = []
    num_rows = len(df)
    cur_pos = 0
    index = 0

    while cur_pos < num_rows:
        chunk_rows = min(rows_per_chunk, num_rows - cur_pos)
        chunk_df = df[cur_pos : cur_pos + chunk_rows]
        chunk_pa_table = pa.Table.from_pandas(chunk_df)

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, chunk_pa_table.schema) as writer:
            writer.write_table(chunk_pa_table)

        chunks.append(
            MockChunk(
                index=index,
                row_count=chunk_rows,
                byte_count=len(sink.getvalue()),
                data=sink.getvalue(),
            )
        )
        index += 1
        cur_pos += rows_per_chunk

    return chunks


# =============================================================================
# Test classes
# =============================================================================


class TestDatabricksUCDatasourceIntegration:
    """Integration tests for DatabricksUCDatasource."""

    _MOCK_ENV_VAR = "RAY_DATABRICKS_UC_DATASOURCE_READ_FN_MOCK_TEST_SETUP_FN_PATH"

    @contextmanager
    def _setup_mock(self, test_data: dict, mock_chunks: list[MockChunk]):
        """Set up mocks for integration tests."""
        chunk_meta_json = [
            {
                "chunk_index": chunk.index,
                "row_count": chunk.row_count,
                "byte_count": chunk.byte_count,
            }
            for chunk in mock_chunks
        ]
        chunk_meta_json.reverse()
        valid_statement_ids = set()

        def request_post_mock(url, data=None, json=None, **kwargs):
            import json as jsonlib

            headers = kwargs["headers"]

            if url == "https://test_shard/api/2.0/sql/statements/":
                assert headers == {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {test_data['token']}",
                }
                assert jsonlib.loads(data) == {
                    "statement": test_data["query"],
                    "warehouse_id": test_data["warehouse_id"],
                    "wait_timeout": "0s",
                    "disposition": "EXTERNAL_LINKS",
                    "format": "ARROW_STREAM",
                    "catalog": test_data["catalog"],
                    "schema": test_data["schema"],
                }

                statement_id = uuid.uuid4().hex
                valid_statement_ids.add(statement_id)

                return MockResponse(
                    status_code=200,
                    content=b"",
                    _json_data={
                        "statement_id": statement_id,
                        "status": {"state": "PENDING"},
                    },
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
                    "Authorization": f"Bearer {test_data['token']}",
                }
                assert statement_id in valid_statement_ids

                return MockResponse(
                    status_code=200,
                    _json_data={
                        "status": {"state": "SUCCEEDED"},
                        "manifest": {
                            "truncated": False,
                            "chunks": chunk_meta_json,
                        },
                    },
                )

            if match := re.match(
                r"^https://test_shard/api/2\.0/sql/"
                r"statements/([^/]*)/result/chunks/([^/]*)$",
                url,
            ):
                assert headers == {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {test_data['token']}",
                }

                chunk_index = match.group(2)
                external_link = f"https://test_external_link/{chunk_index}"
                return MockResponse(
                    status_code=200,
                    _json_data={"external_links": [{"external_link": external_link}]},
                )

            if match := re.match(r"^https://test_external_link/([^/]*)$", url):
                assert headers is None
                chunk_index = int(match.group(1))
                return MockResponse(
                    status_code=200,
                    content=mock_chunks[chunk_index].data,
                )

            assert False, "Invalid request."

        with (
            mock.patch("requests.get", request_get_mock),
            mock.patch("requests.post", request_post_mock),
            mock.patch(
                "socket.getaddrinfo",
                return_value=[
                    (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))
                ],
            ),
            mock.patch.dict(
                os.environ,
                {
                    "DATABRICKS_HOST": "test_shard",
                    "DATABRICKS_TOKEN": test_data["token"],
                },
            ),
        ):
            yield

    @contextmanager
    def _setup_integration_test(self, test_data: dict):
        """Set up complete integration test environment with mocks and Ray."""
        mock_chunks = create_mock_chunks(
            test_data["expected_df"], test_data["rows_per_chunk"]
        )

        setup_mock_fn_path = os.path.join(tempfile.mkdtemp(), "setup_mock_fn.pkl")
        with open(setup_mock_fn_path, "wb") as fp:
            pickle.dump(lambda: self._setup_mock(test_data, mock_chunks), fp)

        with (
            self._setup_mock(test_data, mock_chunks),
            mock.patch.dict(os.environ, {self._MOCK_ENV_VAR: setup_mock_fn_path}),
        ):
            ray.shutdown()
            ray.init()
            yield

    def test_read_with_table_name(self, test_data):
        """Test reading data using table name."""
        with self._setup_integration_test(test_data):
            result = ray.data.read_databricks_tables(
                warehouse_id=test_data["warehouse_id"],
                table="table1",
                catalog=test_data["catalog"],
                schema=test_data["schema"],
                override_num_blocks=5,
            ).to_pandas()

            assert rows_same(result, test_data["expected_df"])

    def test_read_with_sql_query(self, test_data):
        """Test reading data using SQL query."""
        with self._setup_integration_test(test_data):
            result = ray.data.read_databricks_tables(
                warehouse_id=test_data["warehouse_id"],
                query=test_data["query"],
                catalog=test_data["catalog"],
                schema=test_data["schema"],
                override_num_blocks=5,
            ).to_pandas()

            assert rows_same(result, test_data["expected_df"])

    @pytest.mark.parametrize("num_blocks", [5, 100])
    def test_read_with_different_parallelism(self, test_data, num_blocks):
        """Test reading data with different parallelism settings."""
        with self._setup_integration_test(test_data):
            result = ray.data.read_databricks_tables(
                warehouse_id=test_data["warehouse_id"],
                query=test_data["query"],
                catalog=test_data["catalog"],
                schema=test_data["schema"],
                override_num_blocks=num_blocks,
            ).to_pandas()

            assert rows_same(result, test_data["expected_df"])


class TestDatabricksUCDatasourceCredentials:
    """Tests for credential provider handling."""

    def test_schema_name_does_not_shadow_datasource_fields(self, requests_mocker):
        """Test that schema name is stored without using the `schema` attribute.
        This is a regression test for https://github.com/ray-project/ray/issues/46481.
        """
        requests_mocker["post"].return_value = mock.Mock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {"statement_id": "test_stmt", "status": {"state": "PENDING"}},
        )
        requests_mocker["get"].return_value = mock.Mock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {
                "status": {"state": "SUCCEEDED"},
                "manifest": {"truncated": False, "chunks": []},
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

        assert datasource.schema_name == "test_schema"
        assert "schema" not in datasource.__dict__

        call_kwargs = requests_mocker["post"].call_args[1]
        payload = json.loads(call_kwargs["data"])
        assert payload["schema"] == "test_schema"

    def test_with_credential_provider(self, requests_mocker):
        """Test DatabricksUCDatasource with credential_provider parameter."""
        requests_mocker["post"].return_value = mock.Mock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {"statement_id": "test_stmt", "status": {"state": "PENDING"}},
        )
        requests_mocker["get"].return_value = mock.Mock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {
                "status": {"state": "SUCCEEDED"},
                "manifest": {"truncated": False},
            },
        )

        provider = StaticCredentialProvider(token="my_provider_token", host="test_host")
        _datasource = DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=provider,
        )

        # Verify the token from provider was used in requests
        call_kwargs = requests_mocker["post"].call_args[1]
        assert "Authorization" in call_kwargs["headers"]
        assert "Bearer my_provider_token" in call_kwargs["headers"]["Authorization"]

    def test_fresh_token_per_request(self, requests_mocker, token_tracking_provider):
        """Test that fresh tokens are fetched for each request during polling."""
        tokens_used = []

        def capture_post(url, *args, **kwargs):
            tokens_used.append(kwargs["headers"]["Authorization"])
            return mock.Mock(
                status_code=200,
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
            state = "PENDING" if poll_count[0] < 3 else "SUCCEEDED"
            return mock.Mock(
                status_code=200,
                raise_for_status=lambda: None,
                json=lambda: {
                    "status": {"state": state},
                    "manifest": {"truncated": False, "chunks": []},
                },
            )

        requests_mocker["post"].side_effect = capture_post
        requests_mocker["get"].side_effect = capture_get

        DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=token_tracking_provider,
        )

        # Verify fresh token was fetched for each request:
        # 1 POST (statement creation) + 3 GETs (polling)
        assert token_tracking_provider.token_fetch_count == 4
        assert tokens_used == [
            "Bearer token_1",  # POST
            "Bearer token_2",  # GET poll 1
            "Bearer token_3",  # GET poll 2
            "Bearer token_4",  # GET poll 3
        ]


class TestDatabricksUCDatasource401Retry:
    """Tests for 401 retry behavior."""

    def test_401_during_initial_post(
        self, requests_mocker, refreshable_credential_provider
    ):
        """Test that 401 during initial POST triggers credential invalidation and retry."""
        post_call_count = [0]
        post_headers_captured = []

        def post_side_effect(url, *args, **kwargs):
            post_call_count[0] += 1
            headers = kwargs.get("headers", {})
            post_headers_captured.append(headers.get("Authorization", ""))

            # First POST returns 401
            if post_call_count[0] == 1:
                return mock.Mock(status_code=401)
            # Retry succeeds
            return mock.Mock(
                status_code=200,
                json=lambda: {
                    "statement_id": "test_stmt",
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"truncated": False, "chunks": []},
                },
            )

        requests_mocker["post"].side_effect = post_side_effect

        DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=refreshable_credential_provider,
        )

        # Verify retry occurred
        assert (
            post_call_count[0] == 2
        ), "Expected POST to be called twice (initial + retry)"

        # Verify invalidate was called
        assert refreshable_credential_provider.invalidate_count == 1

        # Verify first request used expired token, retry used refreshed token
        assert "expired_token" in post_headers_captured[0]
        assert "refreshed_token" in post_headers_captured[1]

    def test_401_during_polling(self, requests_mocker, refreshable_credential_provider):
        """Test that 401 during polling triggers credential invalidation and retry."""
        poll_call_count = [0]
        poll_headers_captured = []

        requests_mocker["post"].return_value = mock.Mock(
            status_code=200,
            json=lambda: {
                "statement_id": "test_stmt",
                "status": {"state": "PENDING"},
            },
        )

        def get_side_effect(url, *args, **kwargs):
            poll_call_count[0] += 1
            headers = kwargs.get("headers", {})
            poll_headers_captured.append(headers.get("Authorization", ""))

            # First poll returns 401 with expired token
            if poll_call_count[0] == 1:
                return mock.Mock(status_code=401)
            # Retry succeeds
            return mock.Mock(
                status_code=200,
                json=lambda: {
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"truncated": False, "chunks": []},
                },
            )

        requests_mocker["get"].side_effect = get_side_effect

        DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=refreshable_credential_provider,
        )

        # Verify retry occurred
        assert (
            poll_call_count[0] == 2
        ), "Expected GET to be called twice (initial + retry)"

        # Verify invalidate was called once
        assert refreshable_credential_provider.invalidate_count == 1

        # Verify first request used expired token, retry used refreshed token
        assert "expired_token" in poll_headers_captured[0]
        assert "refreshed_token" in poll_headers_captured[1]

    def test_401_during_chunk_fetch(
        self, requests_mocker, refreshable_credential_provider
    ):
        """Test that 401 during chunk fetch triggers credential invalidation and retry."""
        chunk_fetch_count = [0]
        chunk_fetch_headers = []

        # Create Arrow data for external URL response
        table = pa.Table.from_pydict({"col1": [1, 2, 3]})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        arrow_data = sink.getvalue().to_pybytes()

        # POST for statement creation succeeds
        requests_mocker["post"].return_value = mock.Mock(
            status_code=200,
            json=lambda: {
                "statement_id": "test_stmt",
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "truncated": False,
                    "chunks": [{"chunk_index": 0, "row_count": 10, "byte_count": 100}],
                },
            },
        )

        def get_side_effect(url, *args, **kwargs):
            headers = kwargs.get("headers", {})

            # External URL fetch (no auth headers)
            if url.startswith("https://external/"):
                return mock.Mock(status_code=200, content=arrow_data, is_redirect=False)

            if "/result/chunks/" in url:
                chunk_fetch_count[0] += 1
                chunk_fetch_headers.append(headers.get("Authorization", ""))

                # First chunk fetch returns 401
                if chunk_fetch_count[0] == 1:
                    return mock.Mock(status_code=401)
                # Retry succeeds
                return mock.Mock(
                    status_code=200,
                    json=lambda: {
                        "external_links": [{"external_link": "https://external/data"}]
                    },
                )
            else:
                # Polling response (already succeeded in POST)
                return mock.Mock(
                    status_code=200,
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

        requests_mocker["get"].side_effect = get_side_effect

        # Create datasource
        datasource = DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=refreshable_credential_provider,
        )

        # Get read tasks and execute the read function to trigger chunk fetch
        read_tasks = datasource.get_read_tasks(parallelism=1)
        assert len(read_tasks) == 1

        # Execute the read function - this triggers chunk fetch
        read_fn = read_tasks[0].read_fn
        results = list(read_fn())

        # Verify chunk fetch retry occurred
        assert (
            chunk_fetch_count[0] == 2
        ), "Expected chunk fetch to be called twice (initial + retry)"

        # Verify invalidate was called during chunk fetch
        assert refreshable_credential_provider.invalidate_count == 1

        # Verify first chunk fetch used expired token, retry used refreshed token
        assert "expired_token" in chunk_fetch_headers[0]
        assert "refreshed_token" in chunk_fetch_headers[1]

        # Verify we got results
        assert len(results) == 1


class TestDatabricksUCDatasourceEmptyResult:
    """Tests for empty result handling."""

    def test_empty_result_returns_zero_count(self, requests_mocker, databricks_env):
        """Test that empty result returns zero count."""

        def post_mock(url, *args, **kwargs):
            return MockResponse(
                status_code=200,
                _json_data={
                    "statement_id": "test_stmt",
                    "status": {"state": "PENDING"},
                },
            )

        def get_mock(url, *args, **kwargs):
            return MockResponse(
                status_code=200,
                _json_data={
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"truncated": False},
                },
            )

        requests_mocker["post"].side_effect = post_mock
        requests_mocker["get"].side_effect = get_mock

        ds = ray.data.read_databricks_tables(
            warehouse_id="dummy_warehouse",
            query="select * from dummy_table",
            catalog="dummy_catalog",
            schema="dummy_schema",
            override_num_blocks=1,
        )

        assert ds.count() == 0


class TestSSRFValidation:
    """Tests for SSRF URL validation (GitHub issue #65669)."""

    # Helper to create a mock getaddrinfo return value for a given IP.
    @staticmethod
    def _make_addrinfo(ip_str):
        """Return a getaddrinfo-style result list for the given IP."""
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip_str, 0))]

    def test_valid_https_url(self):
        """Test that a valid HTTPS URL with a public IP passes."""
        public_ip = "52.94.228.100"
        with mock.patch(
            "socket.getaddrinfo",
            return_value=self._make_addrinfo(public_ip),
        ):
            # Should not raise.
            _validate_external_url("https://valid.example.com/data")

    def test_http_scheme_rejected(self):
        """Test that HTTP URLs are rejected."""
        with pytest.raises(ValueError, match="Only HTTPS"):
            _validate_external_url("http://example.com/data")

    @pytest.mark.parametrize(
        "url",
        [
            "ftp://example.com/data",
            "file:///etc/passwd",
            "gopher://example.com/",
        ],
    )
    def test_non_http_scheme_rejected(self, url):
        """Test that non-HTTP(S) schemes are rejected."""
        with pytest.raises(ValueError, match="Only HTTPS"):
            _validate_external_url(url)

    def test_loopback_ip_rejected(self):
        """Test that loopback IPs (127.0.0.1) are blocked."""
        with (
            mock.patch(
                "socket.getaddrinfo",
                return_value=self._make_addrinfo("127.0.0.1"),
            ),
            pytest.raises(ValueError, match="prevent SSRF attacks"),
        ):
            _validate_external_url("https://localhost/data")

    def test_multicast_ip_rejected(self):
        """Test that multicast IPs are blocked."""
        with (
            mock.patch(
                "socket.getaddrinfo",
                return_value=self._make_addrinfo("224.0.0.1"),
            ),
            pytest.raises(ValueError, match="prevent SSRF attacks"),
        ):
            _validate_external_url("https://multicast.example.com/data")

    def test_private_ip_rejected(self):
        """Test that private IPs (RFC 1918) are blocked by default."""
        with (
            mock.patch(
                "socket.getaddrinfo",
                return_value=self._make_addrinfo("10.0.0.1"),
            ),
            pytest.raises(ValueError, match="private"),
        ):
            _validate_external_url("https://vpc.example.com/data")

    def test_private_ip_allowed_with_env_var(self):
        """Test that private IPs pass when RAY_DATABRICKS_ALLOW_PRIVATE_IPS=1."""
        with (
            mock.patch(
                "socket.getaddrinfo",
                return_value=self._make_addrinfo("10.0.0.1"),
            ),
            mock.patch.dict(os.environ, {"RAY_DATABRICKS_ALLOW_PRIVATE_IPS": "1"}),
        ):
            # Should NOT raise
            _validate_external_url("https://vpc.example.com/data")

    def test_parser_mismatch_rejected(self):
        """Test that URLs with basic authentication (@) are blocked."""
        # This prevents urllib vs requests parser mismatch vulnerabilities
        with mock.patch("socket.getaddrinfo"):
            with pytest.raises(ValueError, match="basic authentication"):
                _validate_external_url(
                    "https://169.254.169.254\\@valid.example.com/data"
                )

            with pytest.raises(ValueError, match="basic authentication"):
                _validate_external_url("https://user:pass@127.0.0.1/data")

    def test_metadata_ip_rejected(self):
        """Test that cloud metadata IP (169.254.169.254) is blocked."""
        with (
            mock.patch(
                "socket.getaddrinfo",
                return_value=self._make_addrinfo("169.254.169.254"),
            ),
            pytest.raises(ValueError, match="prevent SSRF attacks"),
        ):
            _validate_external_url("https://metadata.example.com/data")

    def test_no_hostname_rejected(self):
        """Test that URLs with no hostname are rejected."""
        with pytest.raises(ValueError, match="no hostname"):
            _validate_external_url("https:///path/only")

    def test_dns_failure_rejected(self):
        """Test that unresolvable hostnames raise ValueError."""
        with (
            mock.patch(
                "socket.getaddrinfo",
                side_effect=socket.gaierror("Name resolution failed"),
            ),
            pytest.raises(ValueError, match="Cannot resolve hostname"),
        ):
            _validate_external_url("https://nonexistent.invalid/data")

    def test_ssrf_blocked_during_read(self, requests_mocker):
        """Test that SSRF is blocked end-to-end during data reading."""
        # POST for statement creation succeeds.
        requests_mocker["post"].return_value = mock.Mock(
            status_code=200,
            json=lambda: {
                "statement_id": "test_stmt",
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "truncated": False,
                    "chunks": [
                        {
                            "chunk_index": 0,
                            "row_count": 10,
                            "byte_count": 100,
                        }
                    ],
                },
            },
        )

        # GET for chunk resolution returns a malicious internal URL.
        def get_side_effect(url, *args, **kwargs):
            if "/result/chunks/" in url:
                return mock.Mock(
                    status_code=200,
                    json=lambda: {
                        "external_links": [
                            {
                                "external_link": (
                                    "http://169.254.169.254" "/latest/meta-data/"
                                ),
                            }
                        ]
                    },
                )
            return mock.Mock(
                status_code=200,
                json=lambda: {
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {
                        "truncated": False,
                        "chunks": [
                            {
                                "chunk_index": 0,
                                "row_count": 10,
                                "byte_count": 100,
                            }
                        ],
                    },
                },
            )

        requests_mocker["get"].side_effect = get_side_effect

        provider = StaticCredentialProvider(token="test_token", host="test_host")
        datasource = DatabricksUCDatasource(
            warehouse_id="test_warehouse",
            catalog="test_catalog",
            schema="test_schema",
            query="SELECT 1",
            credential_provider=provider,
        )

        read_tasks = datasource.get_read_tasks(parallelism=1)
        assert len(read_tasks) == 1

        # The read function should raise ValueError due to SSRF
        # validation rejecting the HTTP metadata URL.
        read_fn = read_tasks[0].read_fn
        with pytest.raises(ValueError, match="Only HTTPS"):
            list(read_fn())

    def test_ssrf_redirect_blocked_during_read(self, requests_mocker):
        """Test that SSRF via HTTP redirect is blocked end-to-end."""
        # POST for statement creation succeeds.
        requests_mocker["post"].return_value = mock.Mock(
            status_code=200,
            json=lambda: {
                "statement_id": "test_stmt",
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "truncated": False,
                    "chunks": [
                        {
                            "chunk_index": 0,
                            "row_count": 10,
                            "byte_count": 100,
                        }
                    ],
                },
            },
        )

        def get_side_effect(url, *args, **kwargs):
            if "/result/chunks/" in url:
                return mock.Mock(
                    status_code=200,
                    json=lambda: {
                        "external_links": [
                            {
                                "external_link": "https://valid.example.com/data",
                            }
                        ]
                    },
                )
            if "valid.example.com" in url:
                # The actual read request. Return a redirect response.
                m = mock.Mock(status_code=302, is_redirect=True)
                m.raise_for_status = mock.Mock()
                return m

            return mock.Mock(
                status_code=200,
                json=lambda: {
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {
                        "truncated": False,
                        "chunks": [
                            {
                                "chunk_index": 0,
                                "row_count": 10,
                                "byte_count": 100,
                            }
                        ],
                    },
                },
            )

        requests_mocker["get"].side_effect = get_side_effect

        with mock.patch(
            "socket.getaddrinfo",
            return_value=self._make_addrinfo("52.94.228.100"),
        ):
            provider = StaticCredentialProvider(token="test_token", host="test_host")
            datasource = DatabricksUCDatasource(
                warehouse_id="test_warehouse",
                catalog="test_catalog",
                schema="test_schema",
                query="SELECT 1",
                credential_provider=provider,
            )

            read_tasks = datasource.get_read_tasks(parallelism=1)
            assert len(read_tasks) == 1

            # The read function should raise ValueError because of the 302 redirect.
            read_fn = read_tasks[0].read_fn
            with pytest.raises(ValueError, match="HTTP redirects are not allowed"):
                list(read_fn())

    def test_ssrf_non_redirect_errors_raise_http_error(self, requests_mocker):
        """Test that 4xx/5xx HTTP errors are not blocked as redirects."""
        requests_mocker["post"].return_value = mock.Mock(
            status_code=200,
            json=lambda: {
                "statement_id": "test_stmt",
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "truncated": False,
                    "chunks": [{"chunk_index": 0, "row_count": 10, "byte_count": 100}],
                },
            },
        )

        def get_side_effect(url, *args, **kwargs):
            if "/result/chunks/" in url:
                return mock.Mock(
                    status_code=200,
                    json=lambda: {
                        "external_links": [
                            {"external_link": "https://valid.example.com/data"}
                        ]
                    },
                )
            if "valid.example.com" in url:
                # Return a 403 Forbidden
                m = mock.Mock(status_code=403, is_redirect=False)
                m.raise_for_status.side_effect = requests.exceptions.HTTPError(
                    "403 Forbidden"
                )
                return m

            return mock.Mock(status_code=200, is_redirect=False)

        requests_mocker["get"].side_effect = get_side_effect

        with mock.patch(
            "socket.getaddrinfo",
            return_value=self._make_addrinfo("52.94.228.100"),
        ):
            provider = StaticCredentialProvider(token="test", host="test")
            datasource = DatabricksUCDatasource(
                warehouse_id="w",
                catalog="c",
                schema="s",
                query="q",
                credential_provider=provider,
            )
            read_tasks = datasource.get_read_tasks(parallelism=1)

            # The read function should raise HTTPError, not ValueError.
            read_fn = read_tasks[0].read_fn
            with pytest.raises(requests.exceptions.HTTPError, match="403 Forbidden"):
                list(read_fn())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
