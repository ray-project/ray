import hashlib
import sys

import pytest

from ray._common.runtime_env_uri import Protocol, parse_uri


def _sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


class TestParseUri:
    @pytest.mark.parametrize(
        "uri,protocol,package_name",
        [
            # GCS stays in the netloc-only branch (unchanged behavior).
            ("gcs://file.zip", Protocol.GCS, "file.zip"),
            # Remote (non-.whl) URIs are always hashed.
            (
                "s3://bucket/file.zip",
                Protocol.S3,
                f"s3_{_sha1_hex('s3://bucket/file.zip')}.zip",
            ),
            (
                "http://test.com/file.zip",
                Protocol.HTTP,
                f"http_{_sha1_hex('http://test.com/file.zip')}.zip",
            ),
            (
                "https://test.com/file.zip",
                Protocol.HTTPS,
                f"https_{_sha1_hex('https://test.com/file.zip')}.zip",
            ),
            (
                "gs://bucket/file.zip",
                Protocol.GS,
                f"gs_{_sha1_hex('gs://bucket/file.zip')}.zip",
            ),
            (
                "azure://container/file.zip",
                Protocol.AZURE,
                f"azure_{_sha1_hex('azure://container/file.zip')}.zip",
            ),
            (
                "abfss://container@account.dfs.core.windows.net/file.zip",
                Protocol.ABFSS,
                f"abfss_{_sha1_hex('abfss://container@account.dfs.core.windows.net/file.zip')}.zip",
            ),
            # .whl URIs bypass the hash path (PEP 427 — names must round-trip).
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
                f"https_{_sha1_hex('https://username:PAT@github.com/repo/archive/commit_hash.zip')}.zip",
            ),
            (
                (
                    "https://un:pwd@gitlab.com/user/repo/-/"
                    "archive/commit_hash/repo-commit_hash.zip"
                ),
                f"https_{_sha1_hex('https://un:pwd@gitlab.com/user/repo/-/archive/commit_hash/repo-commit_hash.zip')}.zip",
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
                f"https_{_sha1_hex('https://username:PAT@github.com/repo/archive:2/commit_hash.zip')}.zip",
            ),
            (
                "gs://fake/2022-10-21T13:11:35+00:00/package.zip",
                Protocol.GS,
                f"gs_{_sha1_hex('gs://fake/2022-10-21T13:11:35+00:00/package.zip')}.zip",
            ),
            (
                "s3://fake/2022-10-21T13:11:35+00:00/package.zip",
                Protocol.S3,
                f"s3_{_sha1_hex('s3://fake/2022-10-21T13:11:35+00:00/package.zip')}.zip",
            ),
            (
                "azure://fake/2022-10-21T13:11:35+00:00/package.zip",
                Protocol.AZURE,
                f"azure_{_sha1_hex('azure://fake/2022-10-21T13:11:35+00:00/package.zip')}.zip",
            ),
            (
                (
                    "abfss://container@account.dfs.core.windows.net/"
                    "2022-10-21T13:11:35+00:00/package.zip"
                ),
                Protocol.ABFSS,
                f"abfss_{_sha1_hex('abfss://container@account.dfs.core.windows.net/2022-10-21T13:11:35+00:00/package.zip')}.zip",
            ),
            (
                "file:///fake/2022-10-21T13:11:35+00:00/package.zip",
                Protocol.FILE,
                f"file_{_sha1_hex('file:///fake/2022-10-21T13:11:35+00:00/package.zip')}.zip",
            ),
            (
                "file:///fake/2022-10-21T13:11:35+00:00/(package).zip",
                Protocol.FILE,
                f"file_{_sha1_hex('file:///fake/2022-10-21T13:11:35+00:00/(package).zip')}.zip",
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

    def test_parse_uri_remote_is_stable(self):
        """Remote URIs must map deterministically to the same package_name;
        otherwise concurrent workers would acquire different file locks for
        the same package."""
        uri = "s3://bucket/some/path/pkg.zip"
        _, name_first = parse_uri(uri)
        _, name_second = parse_uri(uri)
        assert name_first == name_second

    def test_parse_uri_remote_paths_are_unique(self):
        """Distinct remote URIs must not collide on the same package_name."""
        _, name_a = parse_uri("s3://bucket/a.zip")
        _, name_b = parse_uri("s3://bucket/b.zip")
        assert name_a != name_b

    @pytest.mark.parametrize(
        "uri_template",
        [
            "s3://bucket/package{ext}",  # extension in path
            "s3://package{ext}",  # extension in netloc (no path component)
        ],
    )
    @pytest.mark.parametrize("ext", [".zip", ".tar.gz", ".tar.bz2"])
    def test_parse_uri_remote_preserves_extension(self, uri_template, ext):
        """Extensions are kept intact after hashing, whether the filename
        is in the path or the netloc."""
        _, name = parse_uri(uri_template.format(ext=ext))
        assert name.endswith(ext)


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
