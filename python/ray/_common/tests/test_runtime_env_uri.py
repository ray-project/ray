import sys

import pytest

from ray._common.runtime_env_uri import Protocol, parse_uri


class TestParseUri:
    @pytest.mark.parametrize(
        "uri,protocol,package_name",
        [
            ("gcs://file.zip", Protocol.GCS, "file.zip"),
            ("s3://bucket/file.zip", Protocol.S3, "s3_bucket_file.zip"),
            ("http://test.com/file.zip", Protocol.HTTP, "http_test_com_file.zip"),
            ("https://test.com/file.zip", Protocol.HTTPS, "https_test_com_file.zip"),
            ("gs://bucket/file.zip", Protocol.GS, "gs_bucket_file.zip"),
            ("azure://container/file.zip", Protocol.AZURE, "azure_container_file.zip"),
            (
                "abfss://container@account.dfs.core.windows.net/file.zip",
                Protocol.ABFSS,
                "abfss_container_account_dfs_core_windows_net_file.zip",
            ),
            (
                "https://test.com/package-0.0.1-py2.py3-none-any.whl?param=value",
                Protocol.HTTPS,
                "package-0.0.1-py2.py3-none-any.whl",
            ),
            (
                "http://test.com/package-0.0.1-py2.py3-none-any.whl?param=value",
                Protocol.HTTP,
                "package-0.0.1-py2.py3-none-any.whl",
            ),
        ],
    )
    def test_parsing_remote_basic(self, uri, protocol, package_name):
        assert parse_uri(uri) == (protocol, package_name)

    @pytest.mark.parametrize(
        "uri,package_name",
        [
            (
                "https://username:PAT@github.com/repo/archive/commit_hash.zip",
                "https_username_PAT_github_com_repo_archive_commit_hash.zip",
            ),
            (
                (
                    "https://un:pwd@gitlab.com/user/repo/-/"
                    "archive/commit_hash/repo-commit_hash.zip"
                ),
                (
                    "https_un_pwd_gitlab_com_user_repo_-_"
                    "archive_commit_hash_repo-commit_hash.zip"
                ),
            ),
        ],
    )
    def test_parse_private_git_https_uris(self, uri, package_name):
        assert parse_uri(uri) == (Protocol.HTTPS, package_name)

    @pytest.mark.parametrize(
        "uri,protocol,package_name",
        [
            (
                "https://username:PAT@github.com/repo/archive:2/commit_hash.zip",
                Protocol.HTTPS,
                "https_username_PAT_github_com_repo_archive_2_commit_hash.zip",
            ),
            (
                "gs://fake/2022-10-21T13:11:35+00:00/package.zip",
                Protocol.GS,
                "gs_fake_2022-10-21T13_11_35_00_00_package.zip",
            ),
            (
                "s3://fake/2022-10-21T13:11:35+00:00/package.zip",
                Protocol.S3,
                "s3_fake_2022-10-21T13_11_35_00_00_package.zip",
            ),
            (
                "azure://fake/2022-10-21T13:11:35+00:00/package.zip",
                Protocol.AZURE,
                "azure_fake_2022-10-21T13_11_35_00_00_package.zip",
            ),
            (
                (
                    "abfss://container@account.dfs.core.windows.net/"
                    "2022-10-21T13:11:35+00:00/package.zip"
                ),
                Protocol.ABFSS,
                (
                    "abfss_container_account_dfs_core_windows_net_"
                    "2022-10-21T13_11_35_00_00_package.zip"
                ),
            ),
            (
                "file:///fake/2022-10-21T13:11:35+00:00/package.zip",
                Protocol.FILE,
                "file__fake_2022-10-21T13_11_35_00_00_package.zip",
            ),
            (
                "file:///fake/2022-10-21T13:11:35+00:00/(package).zip",
                Protocol.FILE,
                "file__fake_2022-10-21T13_11_35_00_00__package_.zip",
            ),
        ],
    )
    def test_parse_uris_with_disallowed_chars(self, uri, protocol, package_name):
        assert parse_uri(uri) == (protocol, package_name)

    @pytest.mark.parametrize(
        "uri,protocol,package_name",
        [
            (
                "https://username:PAT@github.com/repo/archive:2/commit_hash.whl",
                Protocol.HTTPS,
                "commit_hash.whl",
            ),
            (
                "gs://fake/2022-10-21T13:11:35+00:00/package.whl",
                Protocol.GS,
                "package.whl",
            ),
            (
                "s3://fake/2022-10-21T13:11:35+00:00/package.whl",
                Protocol.S3,
                "package.whl",
            ),
            (
                "azure://fake/2022-10-21T13:11:35+00:00/package.whl",
                Protocol.AZURE,
                "package.whl",
            ),
            (
                (
                    "abfss://container@account.dfs.core.windows.net/"
                    "2022-10-21T13:11:35+00:00/package.whl"
                ),
                Protocol.ABFSS,
                "package.whl",
            ),
            (
                "file:///fake/2022-10-21T13:11:35+00:00/package.whl",
                Protocol.FILE,
                "package.whl",
            ),
        ],
    )
    def test_parse_remote_whl_uris(self, uri, protocol, package_name):
        assert parse_uri(uri) == (protocol, package_name)

    @pytest.mark.parametrize(
        "gcs_uri",
        ["gcs://pip_install_test-0.5-py3-none-any.whl", "gcs://storing@here.zip"],
    )
    def test_parse_gcs_uri(self, gcs_uri):
        """GCS URIs should not be modified in this function."""
        protocol, package_name = parse_uri(gcs_uri)
        assert protocol == Protocol.GCS
        assert package_name == gcs_uri.split("/")[-1]


def test_parse_uri_tar_gz():
    protocol, package_name = parse_uri("s3://bucket/archive.tar.gz")
    assert package_name.endswith(".tar.gz")
    assert protocol == Protocol.S3

    protocol, package_name = parse_uri("https://example.com/path/my.pkg.tar.gz")
    assert package_name.endswith(".tar.gz")
    assert "_" in package_name


def test_parse_uri_rejects_local_path():
    with pytest.raises(ValueError, match="Expected URI but received path"):
        parse_uri("/tmp/file.zip")


def test_parse_uri_rejects_invalid_protocol():
    with pytest.raises(ValueError, match="Invalid protocol for runtime_env URI"):
        parse_uri("unknown://file.zip")


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
