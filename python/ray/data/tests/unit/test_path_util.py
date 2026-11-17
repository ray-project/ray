from unittest import mock

import pyarrow
import pyarrow.fs
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.memory import MemoryFileSystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

from ray.data._internal.util import RetryingPyFileSystem
from ray.data.datasource.path_util import (
    _has_file_extension,
    _is_filesystem_compatible_with_scheme,
    _is_local_windows_path,
    _resolve_paths_and_filesystem,
    _resolve_single_path_with_fallback,
    _rewrite_azure_blob_https_url,
    _unwrap_protocol,
)
from ray.util.annotations import RayDeprecationWarning


@pytest.mark.parametrize("override_region", [None, "us-east-2"])
def test_storage_options_preserves_uri_options_and_path(override_region):
    from ray.data.context import DataContext

    options = {} if override_region is None else {"region": override_region}
    native = pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2")
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=native) as constructor:
        paths, fs = _resolve_paths_and_filesystem(
            "s3://bucket/a.parquet?region=us-west-2&allow_bucket_creation=true",
            storage_options=options,
        )
    assert paths == ["bucket/a.parquet"]
    assert fs is native
    constructor.assert_called_once_with(
        **{
            **DataContext.get_current().s3_filesystem_defaults,
            "region": override_region or "us-west-2",
            "allow_bucket_creation": True,
        }
    )


@pytest.mark.parametrize(
    "key, expected",
    [("a%20b", "a b"), ("a%3Fb", "a?b"), ("a%23b", "a#b"), ("a#b", "a#b")],
)
def test_storage_options_decodes_uri_object_keys(key, expected):
    native = pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2")
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=native):
        paths, _ = _resolve_paths_and_filesystem(
            f"s3://bucket/{key}", storage_options={}
        )
    assert paths == [f"bucket/{expected}"]


def test_storage_options_preserves_uri_connection_options():
    native = pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2")
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=native) as constructor:
        paths, _ = _resolve_paths_and_filesystem(
            "s3://bucket/a?scheme=http&endpoint_override=localhost%3A9000"
            "&allow_bucket_deletion=false",
            storage_options={"endpoint_override": "localhost:9001"},
        )
    assert paths == ["bucket/a"]
    assert constructor.call_args.kwargs["scheme"] == "http"
    assert constructor.call_args.kwargs["endpoint_override"] == "localhost:9001"
    assert constructor.call_args.kwargs["allow_bucket_deletion"] is False


def test_storage_options_none_preserves_uri_query_inference():
    paths, fs = _resolve_paths_and_filesystem(
        "s3://test-access:test-secret@bucket/a.parquet?region=us-west-2",
        storage_options=None,
    )
    assert paths == ["bucket/a.parquet"]
    assert fs.region == "us-west-2"


def test_storage_options_preserves_uri_credentials():
    from ray.data.context import DataContext

    native = pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2")
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=native) as constructor:
        paths, _ = _resolve_paths_and_filesystem(
            "s3://test-access:test-secret@bucket/a?region=us-west-2",
            storage_options={"secret_key": "test-override"},
        )
    assert paths == ["bucket/a"]
    constructor.assert_called_once_with(
        **DataContext.get_current().s3_filesystem_defaults,
        region="us-west-2",
        access_key="test-access",
        secret_key="test-override",
    )


@pytest.mark.parametrize("override_region", [None, "us-east-2"])
def test_storage_options_requires_one_effective_uri_configuration(override_region):
    paths = [
        "s3://bucket/a?region=us-west-2",
        "s3://bucket/b?region=us-east-1",
    ]
    native = pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2")
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=native) as constructor:
        if override_region is None:
            with pytest.raises(ValueError, match="same S3 filesystem configuration"):
                _resolve_paths_and_filesystem(paths, storage_options={})
            constructor.assert_not_called()
        else:
            resolved, fs = _resolve_paths_and_filesystem(
                paths, storage_options={"region": override_region}
            )
            assert resolved == ["bucket/a", "bucket/b"]
            assert fs is native
            assert constructor.call_args.kwargs["region"] == override_region


@pytest.mark.parametrize(
    "query", ["allow_bucket_creation=test-marker", "unsupported_option=test-marker"]
)
def test_storage_options_invalid_uri_is_sanitized(query):
    with pytest.raises(ValueError, match="Invalid `storage_options`") as exc:
        _resolve_paths_and_filesystem(
            f"s3://test-access:test-marker@bucket/a?{query}", storage_options={}
        )
    assert "test-marker" not in str(exc.value)
    assert exc.value.__suppress_context__


@pytest.mark.parametrize(
    "options",
    [
        {"scheme": "test-marker"},
        {"proxy_options": "http://test-user:test-marker@localhost:bad"},
    ],
)
def test_storage_options_real_constructor_errors_are_sanitized(options):
    with pytest.raises(ValueError, match="Invalid `storage_options`") as exc:
        _resolve_paths_and_filesystem(
            "s3://bucket/a",
            storage_options={"anonymous": True, "region": "us-west-2", **options},
        )
    assert "test-marker" not in str(exc.value)
    assert exc.value.__suppress_context__


@pytest.mark.parametrize(
    "format, class_name",
    [
        ("parquet", "ParquetDatasourceV2"),
        ("text", "TextDatasourceV2"),
        ("orc", "OrcDatasourceV2"),
        ("mcap", "MCAPDatasourceV2"),
    ],
)
def test_storage_options_v2_datasources_forward_to_resolver(
    monkeypatch, format, class_name
):
    import importlib

    module = importlib.import_module(
        f"ray.data._internal.datasource_v2.formats.{format}.{format}_datasource_v2"
    )

    class Resolved(Exception):
        pass

    resolver = mock.Mock(side_effect=Resolved)
    monkeypatch.setattr(module, "_resolve_paths_and_filesystem", resolver)
    if format == "mcap":
        monkeypatch.setattr(module, "_check_import", lambda *args, **kwargs: None)
    options = {"region": "us-west-2", "endpoint_override": "localhost:9000"}
    with pytest.raises(Resolved):
        getattr(module, class_name)(["s3://bucket/a"], storage_options=options)
    resolver.assert_called_once_with(["s3://bucket/a"], None, storage_options=options)


def test_storage_options_defaults_and_overrides(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(
        "ray.data.context.DataContext.get_current",
        lambda: SimpleNamespace(
            s3_filesystem_defaults={"connect_timeout": 10.0, "request_timeout": 30.0}
        ),
    )
    options = {
        "region": "us-west-2",
        "connect_timeout": 5,
        "access_key": "test-access",
        "secret_key": "test-secret",
        "endpoint_override": "localhost:9000",
    }
    original = options.copy()
    native = pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2")
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=native) as constructor:
        paths, fs = _resolve_paths_and_filesystem("s3://bucket/a", storage_options={})
        assert paths == ["bucket/a"]
        assert fs is native
        constructor.assert_called_once_with(connect_timeout=10.0, request_timeout=30.0)
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=native) as constructor:
        _resolve_paths_and_filesystem("s3://bucket/a", storage_options=options)
        constructor.assert_called_once_with(request_timeout=30.0, **options)
    assert options == original
    fs = pyarrow.fs.LocalFileSystem()
    assert _resolve_paths_and_filesystem("/tmp/a", fs, storage_options=None)[1] is fs
    assert (
        _resolve_paths_and_filesystem("/tmp/a", storage_options=None)[1].type_name
        == "local"
    )


def test_storage_options_override_context_defaults(monkeypatch):
    from types import SimpleNamespace

    defaults = {
        "connect_timeout": 20.0,
        "request_timeout": 40.0,
        "background_writes": False,
    }
    original_defaults = defaults.copy()
    options = {"connect_timeout": 5.0, "request_timeout": 15.0, "region": "us-west-2"}
    original_options = options.copy()
    monkeypatch.setattr(
        "ray.data.context.DataContext.get_current",
        lambda: SimpleNamespace(s3_filesystem_defaults=defaults),
    )
    native = pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2")
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=native) as constructor:
        _resolve_paths_and_filesystem("s3://bucket/a", storage_options=options)
    constructor.assert_called_once_with(
        connect_timeout=5.0,
        request_timeout=15.0,
        background_writes=False,
        region="us-west-2",
    )
    assert options == original_options
    assert defaults == original_defaults


def test_storage_options_none_keeps_uri_inference():
    with mock.patch("pyarrow.fs.S3FileSystem") as constructor, mock.patch(
        "ray.data.datasource.path_util._resolve_single_path_with_fallback",
        return_value=(pyarrow.fs.LocalFileSystem(), "bucket/a"),
    ) as resolve:
        _resolve_paths_and_filesystem("s3://bucket/a", storage_options=None)
    constructor.assert_not_called()
    resolve.assert_called_once_with("s3://bucket/a", None)


@pytest.mark.parametrize("options", [[], "bad", True, 1])
def test_storage_options_rejects_invalid_types(options):
    with pytest.raises(TypeError, match="must be a dictionary"):
        _resolve_paths_and_filesystem("s3://bucket/a", None, storage_options=options)


@pytest.mark.parametrize(
    "paths",
    [
        "/tmp/a",
        "gs://bucket/a",
        "s3a://bucket/a",
        ["s3://bucket/a", "/tmp/b"],
    ],
)
def test_storage_options_rejects_non_s3_paths(paths):
    with mock.patch("pyarrow.fs.S3FileSystem") as constructor:
        with pytest.raises(ValueError, match="requires s3://"):
            _resolve_paths_and_filesystem(paths, None, storage_options={})
        constructor.assert_not_called()


def test_storage_options_conflicts():
    with pytest.raises(ValueError, match="cannot be combined"):
        _resolve_paths_and_filesystem(
            "s3://bucket/a", pyarrow.fs.LocalFileSystem(), storage_options={}
        )


@pytest.mark.parametrize("error_type", [ValueError, TypeError, OSError])
def test_storage_options_constructor_errors_are_sanitized(error_type):
    with mock.patch("pyarrow.fs.S3FileSystem", side_effect=error_type("test-secret")):
        with pytest.raises(ValueError, match="Invalid `storage_options`") as exc:
            _resolve_paths_and_filesystem("s3://bucket/a", None, storage_options={})
    assert "test-secret" not in str(exc.value)
    assert exc.value.__suppress_context__


@pytest.mark.parametrize("wrapped", [False, True])
def test_storage_options_preserves_filesystem_without_reinference(wrapped):
    import pickle

    _, fs = _resolve_paths_and_filesystem(
        "s3://bucket/data#v2/a",
        None,
        storage_options={"anonymous": True, "region": "us-west-2"},
    )
    assert pickle.loads(pickle.dumps(fs)).equals(fs)
    if wrapped:
        fs = RetryingPyFileSystem.wrap(fs, retryable_errors=[], max_attempts=1)
    with mock.patch(
        "pyarrow.fs._resolve_filesystem_and_path",
        wraps=pyarrow.fs._resolve_filesystem_and_path,
    ) as infer:
        paths, result = _resolve_paths_and_filesystem("s3://bucket/data#v2/a", fs)
    assert paths == ["bucket/data#v2/a"]
    assert result is fs
    assert infer.call_count == 1
    assert infer.call_args.args[1] is fs


def test_storage_options_creates_one_filesystem_for_multiple_paths():
    from ray.data.context import DataContext

    native = pyarrow.fs.S3FileSystem(anonymous=True, region="us-west-2")
    with mock.patch("pyarrow.fs.S3FileSystem", return_value=native) as constructor:
        paths, fs = _resolve_paths_and_filesystem(
            ["s3://bucket/a", "s3://bucket/b"], storage_options={}
        )
    constructor.assert_called_once_with(
        **DataContext.get_current().s3_filesystem_defaults
    )
    assert paths == ["bucket/a", "bucket/b"]
    assert fs is native


def test_storage_options_validation_is_delegated_to_pyarrow():
    with pytest.raises(ValueError, match="Invalid `storage_options`"):
        _resolve_paths_and_filesystem(
            "s3://bucket/a", storage_options={"unknown_s3_option": True}
        )


@pytest.mark.parametrize(
    "path, extensions, has_extension",
    [
        ("foo.csv", ["csv"], True),
        ("foo.csv", ["json", "csv"], True),
        ("foo.csv", ["json", "jsonl"], False),
        ("foo.csv", [".csv"], True),
        ("foo.parquet.crc", ["parquet"], False),
        ("foo.parquet.crc", ["crc"], True),
        ("s3://bucket/foo.parquet?versionId=abc123", ["parquet"], True),
        ("bucket/foo.parquet?versionId=abc123", ["parquet"], True),
        ("s3://bucket/foo.parquet?versionId=abc123", ["csv"], False),
        ("s3://bucket/data#v2/file.parquet", ["parquet"], True),
        ("C:\\data\\test.parquet", ["parquet"], True),
        ("C:\\data\\parquet", ["parquet"], False),
        ("foo.csv", None, True),
    ],
)
def test_has_file_extension(path, extensions, has_extension):
    assert _has_file_extension(path, extensions) == has_extension


@pytest.mark.parametrize(
    "filesystem", [None, PyFileSystem(FSSpecHandler(HTTPFileSystem()))]
)
def test_resolve_http_paths(filesystem):
    resolved_paths, resolve_filesystem = _resolve_paths_and_filesystem(
        "https://google.com", filesystem
    )
    # `_resolve_paths_and_filesystem` shouldn't remove the protocol/scheme from the
    # path for HTTP paths.
    assert resolved_paths == ["https://google.com"]
    assert isinstance(resolve_filesystem, pyarrow.fs.PyFileSystem)
    assert isinstance(resolve_filesystem.handler, pyarrow.fs.FSSpecHandler)
    assert isinstance(resolve_filesystem.handler.fs, HTTPFileSystem)


@pytest.mark.parametrize(
    "path, expected",
    [
        ("hdfs://namenode.example/path/file", "/path/file"),
        ("hdfs://namenode.example/path/file;param?query=value", "/path/file;param"),
        ("viewfs://cluster/path/file", "/path/file"),
        ("viewfs://cluster/path/file;param?query=value", "/path/file;param"),
        ("s3://bucket/path/file", "bucket/path/file"),
        ("gs://bucket/path/file", "bucket/path/file"),
        ("abfs://container/path/file", "container/path/file"),
        ("file:///path/file", "/path/file"),
        ("https://example.com/path/file", "example.com/path/file"),
    ],
)
def test_unwrap_protocol(path: str, expected: str) -> None:
    assert _unwrap_protocol(path) == expected


def test_resolve_hdfs_uri_with_explicit_filesystem() -> None:
    class HdfsMemoryFileSystem(MemoryFileSystem):
        protocol = "hdfs"

    resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
        paths="hdfs://namenode.example/path/file",
        filesystem=HdfsMemoryFileSystem(),
    )

    assert resolved_paths == ["/path/file"]
    assert isinstance(resolved_filesystem, PyFileSystem)
    assert isinstance(resolved_filesystem.handler.fs, HdfsMemoryFileSystem)


@pytest.mark.parametrize(
    "path, expected",
    [
        # Blob endpoint: rewritten to PyArrow-compatible bare-container form.
        (
            "https://acct.blob.core.windows.net/cont/dir/file.jpg",
            ("abfs://cont/dir/file.jpg", "acct"),
        ),
        # DFS / Gen2 endpoint: same target form, account still extracted.
        (
            "https://acct.dfs.core.windows.net/cont/dir/file.jpg",
            ("abfs://cont/dir/file.jpg", "acct"),
        ),
        # Mixed-case host is still recognised; account is lower-cased.
        (
            "https://Acct.Blob.Core.Windows.Net/cont/file.bin",
            ("abfs://cont/file.bin", "acct"),
        ),
        # Explicit :443 port — host suffix match still works via parsed.hostname.
        (
            "https://acct.blob.core.windows.net:443/cont/dir/file.jpg",
            ("abfs://cont/dir/file.jpg", "acct"),
        ),
        # Userinfo prefix (`user:pass@host`) — also stripped by parsed.hostname.
        (
            "https://user:pass@acct.blob.core.windows.net/cont/file.bin",
            ("abfs://cont/file.bin", "acct"),
        ),
        # SAS token present — leave URL untouched so HTTP path serves it.
        (
            "https://acct.blob.core.windows.net/cont/file?sv=2024&sig=abc",
            ("https://acct.blob.core.windows.net/cont/file?sv=2024&sig=abc", None),
        ),
        # Non-Azure HTTPS host — passthrough.
        ("https://example.com/cont/file", ("https://example.com/cont/file", None)),
        # Container with no blob path — not enough to rewrite.
        (
            "https://acct.blob.core.windows.net/cont",
            ("https://acct.blob.core.windows.net/cont", None),
        ),
        # Bare account URL with no container.
        (
            "https://acct.blob.core.windows.net/",
            ("https://acct.blob.core.windows.net/", None),
        ),
        # Already-abfs URI is unchanged.
        (
            "abfs://cont/dir/file",
            ("abfs://cont/dir/file", None),
        ),
        # Non-HTTPS scheme is unchanged.
        ("s3://bucket/key", ("s3://bucket/key", None)),
    ],
)
def test_rewrite_azure_blob_https_url(path, expected):
    assert _rewrite_azure_blob_https_url(path) == expected


def test_resolve_single_path_without_azure_filesystem_keeps_https_passthrough():
    """When PyArrow has no AzureFileSystem (older builds), an Azure Blob HTTPS
    URL must NOT be rewritten to abfs:// — otherwise the resolver would have
    no filesystem to construct, the HTTP fallback below cannot match the new
    scheme, and resolution fails outright. The URL should pass through to the
    fsspec HTTP filesystem unchanged.
    """
    import pyarrow.fs

    url = "https://acct.blob.core.windows.net/cont/dir/file.bin"

    # The production code reads pyarrow.fs.AzureFileSystem via
    # ``getattr(pyarrow.fs, "AzureFileSystem", None)`` and treats ``None`` as
    # "not available", so patching the attribute to None simulates the
    # missing-Azure-support build without needing to delete it.
    with mock.patch.object(pyarrow.fs, "AzureFileSystem", None):
        fs, resolved = _resolve_single_path_with_fallback(url)

    assert resolved == url
    assert isinstance(fs, pyarrow.fs.PyFileSystem)
    assert isinstance(fs.handler, pyarrow.fs.FSSpecHandler)
    assert isinstance(fs.handler.fs, HTTPFileSystem)


@pytest.mark.parametrize(
    "path",
    [
        "c:/some/where",
        "c:\\some\\where",
        "c:\\some\\where/mixed",
    ],
)
def test_windows_path(path):
    with mock.patch("sys.platform", "win32"):
        assert _is_local_windows_path(path)


@pytest.mark.parametrize(
    "path",
    [
        "some/file",
        "some/file;semicolon",
        "some/file?questionmark",
        "some/file#hash",
        "some/file;all?of the#above",
    ],
)
def test_weird_local_paths(path):
    resolved_paths, _ = _resolve_paths_and_filesystem(path)
    assert resolved_paths[0] == path


@pytest.mark.parametrize("prefix", ["local://", "local:"])
def test_resolve_local_scheme_emits_deprecation_warning(tmp_path, prefix):
    path = tmp_path / "data.txt"
    path.write_text("")

    with pytest.warns(RayDeprecationWarning, match="local://"):
        _resolve_paths_and_filesystem(f"{prefix}{path}")


class TestIsFilesystemCompatibleWithScheme:
    """Tests for _is_filesystem_compatible_with_scheme with real filesystem implementations."""

    def test_native_local_filesystem(self):
        """Native PyArrow LocalFileSystem should be compatible with empty scheme."""
        fs = pyarrow.fs.LocalFileSystem()
        assert _is_filesystem_compatible_with_scheme(fs, "") is True
        assert _is_filesystem_compatible_with_scheme(fs, "s3") is False

    def test_native_s3_filesystem(self):
        """Native PyArrow S3FileSystem should be compatible with s3 scheme."""
        fs = pyarrow.fs.S3FileSystem(anonymous=True, region="us-east-1")
        assert _is_filesystem_compatible_with_scheme(fs, "s3") is True
        assert _is_filesystem_compatible_with_scheme(fs, "") is False

    def test_fsspec_s3_filesystem_with_s3_scheme(self):
        """fsspec S3FileSystem wrapped in PyFileSystem should be compatible with s3 scheme."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "s3") is True

    def test_fsspec_s3_filesystem_with_bare_paths(self):
        """fsspec S3FileSystem wrapped in PyFileSystem should be compatible with bare paths."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "") is True

    def test_fsspec_s3_filesystem_with_s3a_scheme(self):
        """Real s3fs has protocol=('s3', 's3a'), so it should match s3a scheme."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "s3a") is True

    def test_fsspec_gcs_filesystem_with_gs_scheme(self):
        """fsspec GCS filesystem should be compatible with gs scheme."""
        gcsfs = pytest.importorskip("gcsfs")
        wrapped_fs = PyFileSystem(FSSpecHandler(gcsfs.GCSFileSystem(token="anon")))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "gs") is True

    def test_fsspec_gcs_filesystem_with_gcs_scheme(self):
        """Real gcsfs has protocol=('gcs', 'gs'), so it should match gcs scheme."""
        gcsfs = pytest.importorskip("gcsfs")
        wrapped_fs = PyFileSystem(FSSpecHandler(gcsfs.GCSFileSystem(token="anon")))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "gcs") is True

    def test_fsspec_http_filesystem(self):
        """fsspec HTTPFileSystem wrapped in PyFileSystem should be compatible with http."""
        wrapped_fs = PyFileSystem(FSSpecHandler(HTTPFileSystem()))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "http") is True
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "https") is True

    def test_fsspec_s3_not_compatible_with_gs(self):
        """fsspec S3FileSystem should NOT be compatible with gs scheme."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "gs") is False

    def test_fsspec_s3_not_compatible_with_http(self):
        """fsspec S3FileSystem should NOT be compatible with http/https schemes."""
        s3fs = pytest.importorskip("s3fs")
        wrapped_fs = PyFileSystem(FSSpecHandler(s3fs.S3FileSystem(anon=True)))
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "http") is False
        assert _is_filesystem_compatible_with_scheme(wrapped_fs, "https") is False

    def test_retrying_wrapper_around_native_s3(self):
        """RetryingPyFileSystem wrapping a native S3FileSystem should be compatible with s3."""
        s3_fs = pyarrow.fs.S3FileSystem(anonymous=True)
        retrying_fs = RetryingPyFileSystem.wrap(s3_fs, retryable_errors=["AWS Error"])
        assert _is_filesystem_compatible_with_scheme(retrying_fs, "s3") is True
        assert _is_filesystem_compatible_with_scheme(retrying_fs, "gs") is False

    def test_unknown_scheme_trusts_filesystem(self):
        """Unknown schemes should always return True (trust user's filesystem)."""
        fs = pyarrow.fs.LocalFileSystem()
        assert _is_filesystem_compatible_with_scheme(fs, "custom") is True


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
