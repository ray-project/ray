import os
import sys
from unittest.mock import patch

import pytest

from ray.anyscale.safetensors._private.uri import URIInfo, parse_uri_info


class FakeAWSCredentials:
    """Fake for botocore.credentials.Credentials."""

    access_key: str = "FAKE_AWS_ACCESS_KEY"
    secret_key: str = "FAKE_AWS_SECRET_KEY"
    token: str = "FAKE_AWS_TOKEN"


class FakeGCPCredentials:
    """Fake for google.oauth2.credentials.Credentials."""

    token: str = "FAKE_GCP_TOKEN"


@pytest.fixture
def mock_aws_credentials():
    with patch(
        "ray.anyscale.safetensors._private.uri._get_aws_credentials",
        new=lambda: FakeAWSCredentials,
    ):
        yield


@pytest.fixture
def mock_gcp_credentials():
    with patch(
        "ray.anyscale.safetensors._private.uri._get_gcp_credentials",
        new=lambda: FakeGCPCredentials,
    ):
        yield


class TestValidation:
    def test_unrecognized_scheme(self):
        with pytest.raises(ValueError, match="Unrecognized URI scheme 'foo'"):
            parse_uri_info("foo://bar")

    def test_reject_trailing_slash(self):
        with pytest.raises(ValueError, match="URI must not end with '/'"):
            parse_uri_info("http://bar/baz/")


class TestHTTPURI:
    @pytest.mark.parametrize("scheme", ["http", "https"])
    def test_basic(self, scheme: str):
        uri = f"{scheme}://some-host.com/file"
        assert parse_uri_info(uri) == URIInfo(
            uri=uri,
            download_url=f"{scheme}://some-host.com/file",
            download_headers={},
            cache_prefix=f"{scheme}/some-host.com",
        )


class TestAWSURI:
    def test_basic(self, mock_aws_credentials):
        uri_info = parse_uri_info("s3://some_bucket/file", region="us-west-2")
        assert (
            uri_info.download_url
            == "https://some_bucket.s3.us-west-2.amazonaws.com/file"
        )
        assert (
            uri_info.download_headers["Host"]
            == "some_bucket.s3.us-west-2.amazonaws.com"
        )
        assert uri_info.download_headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD"
        assert (
            uri_info.download_headers["X-Amz-Security-Token"]
            == FakeAWSCredentials.token
        )
        assert len(uri_info.download_headers["X-Amz-Date"]) > 0
        assert len(uri_info.download_headers["Authorization"]) > 0

    def test_missing_region(self, mock_aws_credentials):
        with pytest.raises(ValueError, match="Region must be provided for s3:// URIs"):
            parse_uri_info("s3://some_bucket/file")

    def test_subdir(self, mock_aws_credentials):
        uri_info = parse_uri_info("s3://some_bucket/subdir/file", region="us-west-2")
        assert (
            uri_info.download_url
            == "https://some_bucket.s3.us-west-2.amazonaws.com/subdir/file"
        )
        assert (
            uri_info.download_headers["Host"]
            == "some_bucket.s3.us-west-2.amazonaws.com"
        )
        assert uri_info.download_headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD"
        assert (
            uri_info.download_headers["X-Amz-Security-Token"]
            == FakeAWSCredentials.token
        )
        assert len(uri_info.download_headers["X-Amz-Date"]) > 0
        assert len(uri_info.download_headers["Authorization"]) > 0


class TestGCPURI:
    def test_basic(self, mock_gcp_credentials):
        uri_info = parse_uri_info("gs://some_bucket/file")
        assert uri_info == URIInfo(
            uri="gs://some_bucket/file",
            download_url="https://storage.googleapis.com/storage/v1/b/some_bucket/o/file?alt=media",  # noqa: E501
            download_headers={"Authorization": f"Bearer {FakeGCPCredentials.token}"},
            cache_prefix="gs/some_bucket",
        )

    def test_subdir(self, mock_gcp_credentials):
        uri_info = parse_uri_info("gs://some_bucket/subdir/file")
        assert uri_info == URIInfo(
            uri="gs://some_bucket/subdir/file",
            download_url="https://storage.googleapis.com/storage/v1/b/some_bucket/o/subdir%2Ffile?alt=media",  # noqa: E501
            download_headers={"Authorization": f"Bearer {FakeGCPCredentials.token}"},
            cache_prefix="gs/some_bucket/subdir",
        )


class TestAnyscaleArtifactStorage:
    @patch.dict(os.environ, {}, clear=True)
    def test_missing_storage_env_var(self):
        with pytest.raises(
            ValueError,
            match=("ANYSCALE_ARTIFACT_STORAGE env var not detected"),
        ):
            parse_uri_info("anyscale://file")

    @patch.dict(
        os.environ, {"ANYSCALE_ARTIFACT_STORAGE": "s3://artifact_storage"}, clear=True
    )
    def test_missing_region_env_var(self, mock_aws_credentials):
        with pytest.raises(
            ValueError,
            match=(
                "ANYSCALE_CLOUD_STORAGE_BUCKET_REGION env var not detected, "
                "so region must be manually specified"
            ),
        ):
            parse_uri_info("anyscale://file")

        # Should work if region is specified.
        uri_info = parse_uri_info("anyscale://file", region="us-west-2")
        assert (
            uri_info.download_url
            == "https://artifact_storage.s3.us-west-2.amazonaws.com/file"
        )

    @patch.dict(
        os.environ,
        {
            "ANYSCALE_ARTIFACT_STORAGE": "s3://artifact_storage",
            "ANYSCALE_CLOUD_STORAGE_BUCKET_REGION": "us-west-2",
        },
        clear=True,
    )
    def test_mismatched_region_env_var(self, mock_aws_credentials):
        with pytest.raises(
            ValueError,
            match=(
                "Region was manually specified but does not match "
                "ANYSCALE_CLOUD_STORAGE_BUCKET_REGION. When using an "
                "'anyscale://' URI, region does not need to be specified."
            ),
        ):
            parse_uri_info("anyscale://file", region="something-else")

    @patch.dict(
        os.environ,
        {
            "ANYSCALE_ARTIFACT_STORAGE": "s3://artifact_storage",
            "ANYSCALE_CLOUD_STORAGE_BUCKET_REGION": "us-west-2",
        },
        clear=True,
    )
    def test_subdir(self, mock_aws_credentials):
        uri_info = parse_uri_info("anyscale://subdir/file")
        assert (
            uri_info.download_url
            == "https://artifact_storage.s3.us-west-2.amazonaws.com/subdir/file"
        )


@patch.dict(
    os.environ,
    {
        "ANYSCALE_ARTIFACT_STORAGE": "s3://artifact_storage",
        "ANYSCALE_CLOUD_STORAGE_BUCKET_REGION": "us-west-2",
    },
    clear=True,
)
@pytest.mark.parametrize(
    "uri,cache_prefix",
    [
        ("http://something.com/foo", "http/something.com"),
        ("http://something.com/foo/bar", "http/something.com/foo"),
        ("https://something.com/foo", "https/something.com"),
        ("https://something.com/foo/bar", "https/something.com/foo"),
        ("s3://bucket/foo", "s3/bucket"),
        ("s3://bucket/foo/bar", "s3/bucket/foo"),
        ("gs://bucket/foo", "gs/bucket"),
        ("gs://bucket/foo/bar", "gs/bucket/foo"),
        ("anyscale://foo", "s3/artifact_storage"),
        ("anyscale://foo/bar", "s3/artifact_storage/foo"),
        (
            "anyscale://bucket/foo/bar/baz.tensors",
            "s3/artifact_storage/bucket/foo/bar",
        ),
    ],
)
def test_get_cache_prefix_for_path(
    mock_aws_credentials, mock_gcp_credentials, uri: str, cache_prefix: str
):
    uri_info = parse_uri_info(
        uri, region="us-west-2" if uri.startswith("s3://") else None
    )
    assert uri_info.cache_prefix == cache_prefix


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
