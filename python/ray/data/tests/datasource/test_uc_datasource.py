"""Tests for Unity Catalog datasource (uc_datasource.py)."""

from unittest import mock

import pytest

from ray.data._internal.datasource.databricks_credentials import (
    StaticCredentialProvider,
    build_headers,
    request_with_401_retry,
)
from ray.data._internal.datasource.uc_datasource import (
    UnityCatalogConnector,
)
from ray.data.tests.datasource.databricks_test_utils import (
    MockResponse,
    RefreshableCredentialProvider,
)

# =============================================================================
# Pytest fixtures
# =============================================================================


@pytest.fixture
def static_credential_provider():
    """Fixture that provides a static credential provider."""
    return StaticCredentialProvider(
        token="test_token", host="https://test-host.databricks.com"
    )


@pytest.fixture
def refreshable_credential_provider():
    """Fixture that provides a refreshable credential provider."""
    return RefreshableCredentialProvider()


@pytest.fixture
def requests_mocker():
    """Fixture that mocks requests.get and requests.post."""
    with mock.patch("requests.get") as mock_get:
        with mock.patch("requests.post") as mock_post:
            yield {"get": mock_get, "post": mock_post}


# =============================================================================
# Test classes
# =============================================================================


class TestBuildHeaders:
    """Tests for build_headers function."""

    def test_builds_correct_headers(self, static_credential_provider):
        """Test that headers contain correct token and content type."""
        headers = build_headers(static_credential_provider)

        assert headers["Content-Type"] == "application/json"
        assert headers["Authorization"] == "Bearer test_token"

    def test_fetches_fresh_token(self, refreshable_credential_provider):
        """Test that token is fetched fresh each time."""
        headers1 = build_headers(refreshable_credential_provider)
        assert "expired_token" in headers1["Authorization"]

        refreshable_credential_provider.invalidate()

        headers2 = build_headers(refreshable_credential_provider)
        assert "refreshed_token" in headers2["Authorization"]


class TestRequestWith401Retry:
    """Tests for request_with_401_retry function."""

    def test_successful_request_no_retry(self, static_credential_provider):
        """Test that successful request doesn't trigger retry."""
        mock_request = mock.Mock(return_value=MockResponse(status_code=200))

        response = request_with_401_retry(
            mock_request,
            "https://test-url.com",
            static_credential_provider,
        )

        assert response.status_code == 200
        assert mock_request.call_count == 1

    def test_401_triggers_invalidate_and_retry(self, refreshable_credential_provider):
        """Test that 401 response triggers credential invalidation and retry."""
        call_count = [0]
        headers_captured = []

        def mock_request(url, headers=None, **kwargs):
            call_count[0] += 1
            headers_captured.append(headers.get("Authorization", ""))
            if call_count[0] == 1:
                return MockResponse(status_code=401)
            return MockResponse(status_code=200)

        response = request_with_401_retry(
            mock_request,
            "https://test-url.com",
            refreshable_credential_provider,
        )

        assert response.status_code == 200
        assert call_count[0] == 2
        assert refreshable_credential_provider.invalidate_count == 1
        assert "expired_token" in headers_captured[0]
        assert "refreshed_token" in headers_captured[1]

    def test_non_401_error_raises(self, static_credential_provider):
        """Test that non-401 errors are raised without retry."""
        mock_request = mock.Mock(return_value=MockResponse(status_code=500))

        with pytest.raises(Exception, match="HTTP Error 500"):
            request_with_401_retry(
                mock_request,
                "https://test-url.com",
                static_credential_provider,
            )

        assert mock_request.call_count == 1


class TestUnityCatalogConnectorInit:
    """Tests for UnityCatalogConnector initialization."""

    def test_init_with_credential_provider(self, static_credential_provider):
        """Test initialization with credential provider."""
        connector = UnityCatalogConnector(
            table_full_name="catalog.schema.table",
            credential_provider=static_credential_provider,
        )

        assert connector.base_url == "https://test-host.databricks.com"
        assert connector.table_full_name == "catalog.schema.table"

    @pytest.mark.parametrize(
        "input_host,expected_url",
        [
            ("test-host.databricks.com", "https://test-host.databricks.com"),
            ("https://test-host.databricks.com/", "https://test-host.databricks.com"),
            ("http://test-host.databricks.com", "http://test-host.databricks.com"),
        ],
        ids=["adds_https", "strips_trailing_slash", "preserves_http"],
    )
    def test_init_normalizes_host_url(self, input_host, expected_url):
        """Test that host URL is normalized correctly."""
        provider = StaticCredentialProvider(token="token", host=input_host)

        connector = UnityCatalogConnector(
            table_full_name="catalog.schema.table",
            credential_provider=provider,
        )

        assert connector.base_url == expected_url


class TestUnityCatalogConnector401Retry:
    """Tests for 401 retry behavior in UnityCatalogConnector."""

    def test_401_during_get_table_info(
        self, requests_mocker, refreshable_credential_provider
    ):
        """Test that 401 during _get_table_info triggers retry."""
        call_count = [0]
        headers_captured = []

        def get_side_effect(url, headers=None, **kwargs):
            call_count[0] += 1
            headers_captured.append(headers.get("Authorization", ""))
            if call_count[0] == 1:
                return MockResponse(status_code=401)
            return MockResponse(
                status_code=200,
                _json_data={"table_id": "test_table_id", "name": "table"},
            )

        requests_mocker["get"].side_effect = get_side_effect

        connector = UnityCatalogConnector(
            table_full_name="catalog.schema.table",
            credential_provider=refreshable_credential_provider,
        )
        result = connector._get_table_info()

        assert result["table_id"] == "test_table_id"
        assert call_count[0] == 2
        assert refreshable_credential_provider.invalidate_count == 1
        assert "expired_token" in headers_captured[0]
        assert "refreshed_token" in headers_captured[1]

    def test_401_during_get_creds(
        self, requests_mocker, refreshable_credential_provider
    ):
        """Test that 401 during _get_creds triggers retry."""
        # First set up table info
        requests_mocker["get"].return_value = MockResponse(
            status_code=200,
            _json_data={"table_id": "test_table_id", "name": "table"},
        )

        connector = UnityCatalogConnector(
            table_full_name="catalog.schema.table",
            credential_provider=refreshable_credential_provider,
        )
        connector._get_table_info()

        # Reset for _get_creds test
        refreshable_credential_provider.invalidate_count = 0
        refreshable_credential_provider.current_token = "expired_token"

        post_call_count = [0]
        post_headers_captured = []

        def post_side_effect(url, headers=None, **kwargs):
            post_call_count[0] += 1
            post_headers_captured.append(headers.get("Authorization", ""))
            if post_call_count[0] == 1:
                return MockResponse(status_code=401)
            return MockResponse(
                status_code=200,
                _json_data={"url": "s3://bucket/path"},
            )

        requests_mocker["post"].side_effect = post_side_effect

        connector._get_creds()

        assert connector._table_url == "s3://bucket/path"
        assert post_call_count[0] == 2
        assert refreshable_credential_provider.invalidate_count == 1
        assert "expired_token" in post_headers_captured[0]
        assert "refreshed_token" in post_headers_captured[1]


class TestReadUnityCatalogAPI:
    """Tests for read_unity_catalog API function."""

    @pytest.mark.parametrize(
        "credential_provider, url, token",
        [
            (
                StaticCredentialProvider(
                    token="my_token", host="https://my-host.databricks.com"
                ),
                None,
                None,
            ),
            (None, "https://my-host.databricks.com", "my_token"),
        ],
        ids=["with_credential_provider", "with_url_and_token"],
    )
    def test_successful_read_with_valid_credentials(
        self, requests_mocker, credential_provider, url, token
    ):
        """Test read_unity_catalog succeeds with valid credentials."""
        import ray.data

        with mock.patch.object(
            UnityCatalogConnector, "read", return_value=mock.Mock()
        ) as mock_read:
            ray.data.read_unity_catalog(
                table="catalog.schema.table",
                credential_provider=credential_provider,
                url=url,
                token=token,
            )
            mock_read.assert_called_once()

    @pytest.mark.parametrize(
        "url,token",
        [
            (None, None),
            ("https://my-host.databricks.com", None),
            (None, "my_token"),
        ],
        ids=["no_credentials", "only_url", "only_token"],
    )
    def test_raises_with_incomplete_credentials(self, url, token):
        """Test that read_unity_catalog raises when credentials are incomplete."""
        import ray.data

        with pytest.raises(ValueError, match="Either 'credential_provider' or both"):
            ray.data.read_unity_catalog(
                table="catalog.schema.table",
                url=url,
                token=token,
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
