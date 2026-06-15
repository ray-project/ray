"""Tests for the Catalog abstraction (ray.data.catalog).

These unit-test ``UnityCatalog.resolve`` against mocked Databricks Unity
Catalog REST responses (no network, no Ray cluster), plus the additive
``catalog=`` wiring on ``read_delta`` and the ``read_unity_catalog``
deprecation warning.
"""

from unittest import mock

import pytest

from ray.data.catalog import Catalog, ResolvedTable, UnityCatalog

# ----------------------------------------------------------------------
# Mock REST plumbing.
# ----------------------------------------------------------------------


class _MockResp:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _patch_uc(monkeypatch, table_info, creds):
    """Patch request_with_401_retry so GET returns table_info, POST returns creds."""
    import ray.data._internal.datasource.databricks_credentials as dc

    def fake_request(request_fn, url, provider, **kwargs):
        if url.endswith("temporary-table-credentials"):
            return _MockResp(creds)
        return _MockResp(table_info)

    monkeypatch.setattr(dc, "request_with_401_retry", fake_request)


# ----------------------------------------------------------------------
# UnityCatalog.resolve — credential translation.
# ----------------------------------------------------------------------


def test_unity_catalog_is_a_catalog():
    uc = UnityCatalog(url="https://x.databricks.com", token="t")
    assert isinstance(uc, Catalog)


def test_resolve_aws_credentials(monkeypatch):
    _patch_uc(
        monkeypatch,
        table_info={"table_id": "tid", "data_source_format": "DELTA"},
        creds={
            "url": "s3://bucket/path/",
            "aws_temp_credentials": {
                "access_key_id": "AKIA",
                "secret_access_key": "secret",
                "session_token": "tok",
            },
        },
    )
    uc = UnityCatalog(url="https://x.databricks.com", token="t", region="us-west-2")
    resolved = uc.resolve("cat.schema.tbl", operation="READ")

    assert isinstance(resolved, ResolvedTable)
    assert resolved.url == "s3://bucket/path/"
    assert resolved.data_format == "delta"
    assert resolved.storage_options == {
        "AWS_ACCESS_KEY_ID": "AKIA",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_SESSION_TOKEN": "tok",
        "AWS_REGION": "us-west-2",
    }


def test_resolve_aws_without_region_raises(monkeypatch):
    _patch_uc(
        monkeypatch,
        table_info={"table_id": "tid", "data_source_format": "DELTA"},
        creds={
            "url": "s3://bucket/path/",
            "aws_temp_credentials": {
                "access_key_id": "AKIA",
                "secret_access_key": "secret",
                "session_token": "tok",
            },
        },
    )
    uc = UnityCatalog(url="https://x.databricks.com", token="t")  # no region
    with pytest.raises(ValueError, match="region"):
        uc.resolve("cat.schema.tbl")


def test_resolve_azure_user_delegation_sas(monkeypatch):
    _patch_uc(
        monkeypatch,
        table_info={"table_id": "tid", "data_source_format": "DELTA"},
        creds={
            "url": "abfss://container@myacct.dfs.core.windows.net/path/",
            "azure_user_delegation_sas": {"sas_token": "?sig=abc"},
        },
    )
    uc = UnityCatalog(url="https://x.databricks.com", token="t")
    resolved = uc.resolve("cat.schema.tbl")

    assert resolved.url.startswith("abfss://")
    # Leading '?' stripped; account derived from the URL host.
    assert resolved.storage_options["AZURE_STORAGE_SAS_TOKEN"] == "sig=abc"
    assert resolved.storage_options["AZURE_STORAGE_ACCOUNT_NAME"] == "myacct"


def test_resolve_azure_sas_uri(monkeypatch):
    _patch_uc(
        monkeypatch,
        table_info={"table_id": "tid", "data_source_format": "DELTA"},
        creds={
            "url": "abfss://container@acct2.dfs.core.windows.net/path/",
            "azuresasuri": "sig=def",
        },
    )
    uc = UnityCatalog(url="https://x.databricks.com", token="t")
    resolved = uc.resolve("cat.schema.tbl")
    assert resolved.storage_options["AZURE_STORAGE_SAS_TOKEN"] == "sig=def"
    assert resolved.storage_options["AZURE_STORAGE_ACCOUNT_NAME"] == "acct2"


def test_resolve_format_inferred_from_extension(monkeypatch):
    _patch_uc(
        monkeypatch,
        table_info={"table_id": "tid"},  # no data_source_format
        creds={
            "url": "s3://bucket/file.parquet",
            "aws_temp_credentials": {
                "access_key_id": "a",
                "secret_access_key": "b",
                "session_token": "c",
            },
        },
    )
    uc = UnityCatalog(url="https://x.databricks.com", token="t", region="us-east-1")
    resolved = uc.resolve("cat.schema.tbl")
    assert resolved.data_format == "parquet"


def test_resolve_unknown_credential_type_raises(monkeypatch):
    _patch_uc(
        monkeypatch,
        table_info={"table_id": "tid", "data_source_format": "DELTA"},
        creds={"url": "s3://bucket/path/", "mystery_creds": {}},
    )
    uc = UnityCatalog(url="https://x.databricks.com", token="t", region="us-east-1")
    with pytest.raises(ValueError, match="No known credential type"):
        uc.resolve("cat.schema.tbl")


# ----------------------------------------------------------------------
# read_delta(catalog=) wiring.
# ----------------------------------------------------------------------


def test_read_delta_with_catalog_resolves_and_delegates(monkeypatch):
    """read_delta(catalog=) resolves the identifier, then calls DeltaTable with
    the physical url + merged storage_options."""
    import ray.data.read_api as read_api

    resolved = ResolvedTable(
        url="s3://bucket/tbl/",
        data_format="delta",
        storage_options={"AWS_ACCESS_KEY_ID": "k", "AWS_SECRET_ACCESS_KEY": "s"},
    )

    class _StubCatalog(Catalog):
        def resolve(self, identifier, *, operation="READ"):
            assert identifier == "cat.schema.tbl"
            assert operation == "READ"
            return resolved

    captured = {}

    class _FakeDeltaTable:
        def __init__(self, path, version=None, storage_options=None):
            captured["path"] = path
            captured["storage_options"] = storage_options

        def to_pyarrow_dataset(self, filesystem=None):
            captured["filesystem"] = filesystem
            return mock.MagicMock()

    # Stub deltalake.DeltaTable and the downstream read so we don't touch a cluster.
    import sys

    fake_deltalake = mock.MagicMock()
    fake_deltalake.DeltaTable = _FakeDeltaTable
    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)
    monkeypatch.setattr(
        read_api.ParquetDatasource,
        "from_pyarrow_dataset",
        classmethod(lambda cls, *a, **k: mock.MagicMock()),
    )
    monkeypatch.setattr(read_api, "read_datasource", lambda *a, **k: "DATASET")
    # Avoid actually building a cloud filesystem.
    import ray.data._internal.datasource.delta.utils as delta_utils

    monkeypatch.setattr(
        delta_utils, "create_filesystem_from_storage_options", lambda *a, **k: None
    )

    result = read_api.read_delta("cat.schema.tbl", catalog=_StubCatalog())
    assert result == "DATASET"
    assert captured["path"] == "s3://bucket/tbl/"
    assert captured["storage_options"]["AWS_ACCESS_KEY_ID"] == "k"


def test_read_delta_with_catalog_format_mismatch_raises(monkeypatch):
    import ray.data.read_api as read_api

    class _ParquetCatalog(Catalog):
        def resolve(self, identifier, *, operation="READ"):
            return ResolvedTable(url="s3://b/x.parquet", data_format="parquet")

    import sys

    monkeypatch.setitem(sys.modules, "deltalake", mock.MagicMock())
    with pytest.raises(ValueError, match="not 'delta'"):
        read_api.read_delta("cat.schema.tbl", catalog=_ParquetCatalog())


def test_read_delta_user_storage_options_win(monkeypatch):
    """User-supplied storage_options override vended values."""
    import sys

    import ray.data.read_api as read_api

    class _StubCatalog(Catalog):
        def resolve(self, identifier, *, operation="READ"):
            return ResolvedTable(
                url="s3://bucket/tbl/",
                data_format="delta",
                storage_options={"AWS_ACCESS_KEY_ID": "vended"},
            )

    captured = {}

    class _FakeDeltaTable:
        def __init__(self, path, version=None, storage_options=None):
            captured["storage_options"] = storage_options

        def to_pyarrow_dataset(self, filesystem=None):
            return mock.MagicMock()

    fake_deltalake = mock.MagicMock()
    fake_deltalake.DeltaTable = _FakeDeltaTable
    monkeypatch.setitem(sys.modules, "deltalake", fake_deltalake)
    monkeypatch.setattr(
        read_api.ParquetDatasource,
        "from_pyarrow_dataset",
        classmethod(lambda cls, *a, **k: mock.MagicMock()),
    )
    monkeypatch.setattr(read_api, "read_datasource", lambda *a, **k: "DATASET")
    import ray.data._internal.datasource.delta.utils as delta_utils

    monkeypatch.setattr(
        delta_utils, "create_filesystem_from_storage_options", lambda *a, **k: None
    )

    read_api.read_delta(
        "cat.schema.tbl",
        catalog=_StubCatalog(),
        storage_options={"AWS_ACCESS_KEY_ID": "user"},
    )
    assert captured["storage_options"]["AWS_ACCESS_KEY_ID"] == "user"


# ----------------------------------------------------------------------
# read_unity_catalog deprecation.
# ----------------------------------------------------------------------


def test_read_unity_catalog_emits_deprecation_warning(monkeypatch):
    import ray.data.read_api as read_api

    # Stop before any network: make the connector's read() a no-op.
    monkeypatch.setattr(read_api.UnityCatalogConnector, "read", lambda self: "DATASET")

    with pytest.warns(DeprecationWarning, match="catalog="):
        result = read_api.read_unity_catalog(
            "cat.schema.tbl", url="https://x.databricks.com", token="t"
        )
    assert result == "DATASET"


# ----------------------------------------------------------------------
# worker_filesystem honours storage_options (the linchpin for write creds).
# ----------------------------------------------------------------------


def test_worker_filesystem_builds_from_storage_options():
    """With S3 credentials in the config, worker_filesystem must build a
    credentialed S3FileSystem, not a bare from_uri filesystem."""
    import pyarrow.fs as pa_fs

    from ray.data._internal.datasource.delta.fs import _FsConfig, worker_filesystem

    config = _FsConfig(
        table_uri="s3://bucket/tbl/",
        storage_options={
            "AWS_ACCESS_KEY_ID": "AKIA",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_SESSION_TOKEN": "tok",
            "AWS_REGION": "us-west-2",
        },
    )
    fs = worker_filesystem(config)
    assert isinstance(fs, pa_fs.S3FileSystem)


def test_worker_filesystem_falls_back_to_from_uri_for_local(tmp_path):
    """No usable cloud creds -> fall back to the path-only filesystem
    (unchanged behavior for local / ambient-credential writes)."""
    import pyarrow.fs as pa_fs

    from ray.data._internal.datasource.delta.fs import _FsConfig, worker_filesystem

    config = _FsConfig(table_uri=str(tmp_path), storage_options={})
    fs = worker_filesystem(config)
    assert isinstance(fs, pa_fs.LocalFileSystem)


# ----------------------------------------------------------------------
# write_delta(catalog=) wiring.
# ----------------------------------------------------------------------


def test_write_delta_with_catalog_resolves_read_write_and_injects(monkeypatch):
    """write_delta(catalog=) resolves with operation=READ_WRITE and injects the
    vended storage_options into the DeltaDatasink."""
    import ray.data.dataset as dataset_mod

    seen = {}

    class _StubCatalog(Catalog):
        def resolve(self, identifier, *, operation="READ"):
            seen["identifier"] = identifier
            seen["operation"] = operation
            return ResolvedTable(
                url="s3://bucket/tbl/",
                data_format="delta",
                storage_options={
                    "AWS_ACCESS_KEY_ID": "vended",
                    "AWS_REGION": "us-west-2",
                },
            )

    captured = {}

    def _fake_datasink(path, **kwargs):
        captured["path"] = path
        captured["storage_options"] = kwargs.get("storage_options")
        return object()

    # Patch the DeltaDatasink import target + write_datasink so no cluster runs.
    import ray.data._internal.datasource.delta as delta_pkg

    monkeypatch.setattr(delta_pkg, "DeltaDatasink", _fake_datasink)
    monkeypatch.setattr(
        dataset_mod.Dataset, "write_datasink", lambda self, *a, **k: None
    )

    ds = dataset_mod.Dataset.__new__(dataset_mod.Dataset)  # bare instance; no data
    dataset_mod.Dataset.write_delta(ds, "cat.schema.tbl", catalog=_StubCatalog())

    assert seen["operation"] == "READ_WRITE"
    assert seen["identifier"] == "cat.schema.tbl"
    assert captured["path"] == "s3://bucket/tbl/"
    assert captured["storage_options"]["AWS_ACCESS_KEY_ID"] == "vended"


def test_write_delta_with_catalog_format_mismatch_raises(monkeypatch):
    import ray.data.dataset as dataset_mod

    class _ParquetCatalog(Catalog):
        def resolve(self, identifier, *, operation="READ"):
            return ResolvedTable(url="s3://b/x.parquet", data_format="parquet")

    ds = dataset_mod.Dataset.__new__(dataset_mod.Dataset)
    with pytest.raises(ValueError, match="not 'delta'"):
        dataset_mod.Dataset.write_delta(ds, "cat.schema.tbl", catalog=_ParquetCatalog())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
