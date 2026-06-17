"""Tests for the Catalog connector API (ray.data.catalog)."""

import contextlib
import os
import pickle
from unittest import mock

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data.catalog import Catalog, ReaderFormat, ResolvedSource, UnityCatalog

# conftest provides ray_start_regular_shared
from ray.data.tests.conftest import *  # noqa: F401,F403
from ray.data.tests.datasource.databricks_test_utils import MockResponse

AWS_CREDS = {
    "url": "s3://bucket/path",
    "aws_temp_credentials": {
        "access_key_id": "AKIA",
        "secret_access_key": "secret",
        "session_token": "token",
    },
}


def _mock_uc_rest(data_source_format="DELTA", creds=None):
    """Patch ray.data.catalog.requests so UC REST calls return canned data."""
    creds = creds if creds is not None else AWS_CREDS
    table_info = {
        "table_id": "tid-123",
        "data_source_format": data_source_format,
        "storage_location": creds["url"],
    }
    patcher = mock.patch("ray.data.catalog.requests")
    m = patcher.start()
    m.get.return_value = MockResponse(_json_data=table_info)
    m.post.return_value = MockResponse(_json_data=creds)
    return patcher


@pytest.fixture
def uc_catalog():
    return UnityCatalog(
        url="https://dbc-test.cloud.databricks.com",
        token="dapi-test",
        region="us-west-2",
    )


# ---------------------------------------------------------------------------
# UnityCatalog.resolve
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("reader", [ReaderFormat.DELTA, ReaderFormat.PARQUET])
def test_resolve_storage_aws(uc_catalog, reader):
    patcher = _mock_uc_rest()
    try:
        resolved = uc_catalog.resolve("main.sales.txns", reader=reader)
    finally:
        patcher.stop()

    assert resolved.path == "s3://bucket/path"
    assert isinstance(resolved.filesystem, pafs.S3FileSystem)
    assert resolved.storage_options is None
    assert resolved.data_format is ReaderFormat.DELTA


def test_resolve_aws_requires_region():
    catalog = UnityCatalog(url="https://h.databricks.com", token="t")  # no region
    patcher = _mock_uc_rest()
    try:
        with pytest.raises(ValueError, match="region"):
            catalog.resolve("main.sales.txns", reader=ReaderFormat.DELTA)
    finally:
        patcher.stop()


def test_resolve_iceberg(uc_catalog):
    # Iceberg resolution does not hit the credential-vending REST endpoints.
    resolved = uc_catalog.resolve("main.sales.txns", reader=ReaderFormat.ICEBERG)

    assert resolved.path is None
    assert resolved.filesystem is None
    assert resolved.data_format is ReaderFormat.ICEBERG
    ckw = resolved.catalog_kwargs
    assert ckw["type"] == "rest"
    assert ckw["uri"] == (
        "https://dbc-test.cloud.databricks.com/api/2.1/unity-catalog/iceberg"
    )
    assert ckw["token"] == "dapi-test"
    assert ckw["header.X-Iceberg-Access-Delegation"] == "vended-credentials"


def test_resolve_unsupported_reader(uc_catalog):
    with pytest.raises(ValueError, match="does not support"):
        uc_catalog.resolve("main.sales.txns", reader="bogus")


def test_resolve_azure_returns_storage_options():
    catalog = UnityCatalog(url="https://h.databricks.com", token="t")
    azure_creds = {
        "url": "abfss://c@acct.dfs.core.windows.net/path",
        "azuresasuri": "sv=2021&sig=abc",
    }
    patcher = _mock_uc_rest(data_source_format="DELTA", creds=azure_creds)
    try:
        resolved = catalog.resolve("main.sales.txns", reader=ReaderFormat.DELTA)
    finally:
        patcher.stop()

    assert resolved.filesystem is None
    assert resolved.storage_options == {"AZURE_STORAGE_SAS_TOKEN": "sv=2021&sig=abc"}


def test_gcp_creds_written_and_env_set(monkeypatch):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    catalog = UnityCatalog(url="https://h.databricks.com", token="t")

    catalog._write_gcp_creds('{"sa": 1}')

    path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    assert os.path.exists(path)
    with open(path) as f:
        assert f.read() == '{"sa": 1}'
    os.unlink(path)  # test housekeeping; production intentionally leaves it


# ---------------------------------------------------------------------------
# Reader integration via a fake catalog (no network)
# ---------------------------------------------------------------------------


class _FakeCatalog(Catalog):
    """Returns a pre-baked ResolvedSource; records the reader it was asked for."""

    def __init__(self, resolved):
        self._resolved = resolved
        self.calls = []

    def resolve(self, table, *, reader):
        self.calls.append((table, reader))
        return self._resolved


def test_read_parquet_with_catalog(ray_start_regular_shared, tmp_path):
    path = str(tmp_path / "data.parquet")
    pq.write_table(pa.table({"id": [1, 2, 3]}), path)

    catalog = _FakeCatalog(ResolvedSource(path=path))
    ds = ray.data.read_parquet("main.db.tbl", catalog=catalog)

    assert sorted(r["id"] for r in ds.take_all()) == [1, 2, 3]
    assert catalog.calls == [("main.db.tbl", ReaderFormat.PARQUET)]


@pytest.mark.parametrize("reader", ["parquet", "delta"])
def test_catalog_filesystem_overrides_with_warning(reader):
    # The catalog-resolved filesystem overrides a user-supplied one, but warns.
    # The warning fires at the top of the reader body; suppress any downstream
    # failure from the (intentionally unreachable) s3 path.
    if reader == "delta":
        pytest.importorskip("deltalake")
    fs = pafs.S3FileSystem(
        access_key="AKIA", secret_key="secret", session_token="t", region="us-west-2"
    )
    catalog = _FakeCatalog(ResolvedSource(path="s3://b/p", filesystem=fs))
    read_fn = ray.data.read_parquet if reader == "parquet" else ray.data.read_delta

    with mock.patch.object(ray.data.read_api.logger, "warning") as warn:
        with contextlib.suppress(Exception):
            read_fn("main.db.tbl", catalog=catalog, filesystem=pafs.LocalFileSystem())

    assert any(
        "Overriding the provided `filesystem`" in str(c) for c in warn.call_args_list
    )


def test_read_delta_with_catalog(ray_start_regular_shared, tmp_path):
    deltalake = pytest.importorskip("deltalake")
    path = str(tmp_path / "delta-table")
    deltalake.write_deltalake(path, pa.table({"id": [1, 2, 3]}))

    catalog = _FakeCatalog(ResolvedSource(path=path))
    ds = ray.data.read_delta("main.db.tbl", catalog=catalog)

    assert sorted(r["id"] for r in ds.take_all()) == [1, 2, 3]
    assert catalog.calls == [("main.db.tbl", ReaderFormat.DELTA)]


def test_read_iceberg_uses_catalog_resolved_kwargs():
    catalog = _FakeCatalog(
        ResolvedSource(catalog_kwargs={"type": "rest", "uri": "u", "token": "tk"})
    )
    with mock.patch(
        "ray.data._internal.datasource.iceberg_datasource.IcebergDatasource"
    ) as ds_cls, mock.patch("ray.data.read_api.read_datasource"):
        ray.data.read_iceberg(table_identifier="main.db.tbl", catalog=catalog)

    _, kwargs = ds_cls.call_args
    assert kwargs["catalog_kwargs"] == {"type": "rest", "uri": "u", "token": "tk"}
    assert catalog.calls == [("main.db.tbl", ReaderFormat.ICEBERG)]


def test_read_iceberg_explicit_catalog_kwargs_take_precedence():
    # When both catalog and catalog_kwargs are given, catalog is ignored.
    catalog = _FakeCatalog(ResolvedSource(catalog_kwargs={"type": "rest", "uri": "u"}))
    with mock.patch(
        "ray.data._internal.datasource.iceberg_datasource.IcebergDatasource"
    ) as ds_cls, mock.patch("ray.data.read_api.read_datasource"):
        ray.data.read_iceberg(
            table_identifier="main.db.tbl",
            catalog=catalog,
            catalog_kwargs={"type": "sql", "uri": "explicit"},
        )

    _, kwargs = ds_cls.call_args
    assert kwargs["catalog_kwargs"] == {"type": "sql", "uri": "explicit"}
    assert catalog.calls == []  # catalog was not consulted


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def test_unity_catalog_is_picklable(uc_catalog):
    restored = pickle.loads(pickle.dumps(uc_catalog))
    assert isinstance(restored, UnityCatalog)
    assert restored._region == "us-west-2"


def test_resolved_source_with_filesystem_is_picklable():
    fs = pafs.S3FileSystem(
        access_key="AKIA", secret_key="secret", session_token="t", region="us-west-2"
    )
    src = ResolvedSource(path="s3://b/p", filesystem=fs, data_format=ReaderFormat.DELTA)
    restored = pickle.loads(pickle.dumps(src))
    assert restored.path == "s3://b/p"
    assert isinstance(restored.filesystem, pafs.S3FileSystem)
    assert restored.data_format is ReaderFormat.DELTA


# ---------------------------------------------------------------------------
# Deprecated read_unity_catalog shim
# ---------------------------------------------------------------------------


def test_read_unity_catalog_deprecation_delegates():
    with mock.patch("ray.data.read_api.read_delta") as read_delta:
        with pytest.warns(DeprecationWarning, match="read_unity_catalog"):
            ray.data.read_unity_catalog(
                table="main.db.tbl",
                url="https://h.databricks.com",
                token="t",
                data_format="delta",
            )

    read_delta.assert_called_once()
    _, kwargs = read_delta.call_args
    assert isinstance(kwargs["catalog"], UnityCatalog)


def test_read_unity_catalog_infers_format_from_cred_url():
    # Metadata omits both data_source_format and storage_location; the vended
    # credential URL extension must still identify the format.
    patcher = mock.patch("ray.data.catalog.requests")
    m = patcher.start()
    m.get.return_value = MockResponse(_json_data={"table_id": "tid"})
    m.post.return_value = MockResponse(_json_data={"url": "s3://bucket/data.parquet"})
    try:
        with mock.patch("ray.data.read_api.read_parquet") as read_parquet, pytest.warns(
            DeprecationWarning
        ):
            ray.data.read_unity_catalog(
                table="main.db.tbl", url="https://h.databricks.com", token="t"
            )
    finally:
        patcher.stop()

    read_parquet.assert_called_once()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
