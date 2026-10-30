"""Tests for the Gravitino Iceberg Catalog adapter."""

import base64
import builtins
import json
import pickle
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest import mock

import pytest

import ray
import ray.cloudpickle as cloudpickle
from ray.data.catalog import CatalogAccessMode, GravitinoCatalog, ReaderFormat


@pytest.fixture
def catalog():
    return GravitinoCatalog(
        uri="http://gravitino:8090",
        metalake="test",
        iceberg_rest_uri="http://iceberg:9001/iceberg/",
    )


def test_public_export():
    assert ray.data.GravitinoCatalog is GravitinoCatalog


@pytest.mark.parametrize("mode", list(CatalogAccessMode))
def test_iceberg_resolution(catalog, mode):
    resolved = catalog.resolve(
        "cat.sales.orders", reader=ReaderFormat.ICEBERG, mode=mode
    )
    assert resolved.table_identifier == "sales.orders"
    assert resolved.data_format is ReaderFormat.ICEBERG
    assert resolved.path is None
    assert resolved.catalog_kwargs == {
        "type": "rest",
        "uri": "http://iceberg:9001/iceberg/",
        "warehouse": "cat",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "auth": {"type": "noop"},
    }


def test_target_properties_and_caller_settings_are_preserved():
    properties = {
        "auth": {"type": "noop"},
        "header.Authorization": "Bearer rest-secret",
        "type": "hive",
        "uri": "https://wrong",
        "warehouse": "wrong",
        "header.X-Iceberg-Access-Delegation": "remote-signing",
        "s3.region": "us-east-1",
    }
    original = properties.copy()
    catalog = GravitinoCatalog(
        uri="http://gravitino:8090",
        metalake="test",
        iceberg_rest_uri="https://iceberg/iceberg",
        iceberg_rest_catalog_kwargs=properties,
    )
    properties["header.Authorization"] = "caller-change"
    first = catalog.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG)
    first_kwargs = first.catalog_kwargs
    assert first_kwargs is not None
    assert first_kwargs == {
        **original,
        "type": "rest",
        "uri": "https://iceberg/iceberg/",
        "warehouse": "cat",
    }
    first_kwargs["header.Authorization"] = "resolved-change"
    second = catalog.resolve("other.db.tbl", reader=ReaderFormat.ICEBERG)
    second_kwargs = second.catalog_kwargs
    assert second_kwargs is not None
    assert second_kwargs["header.Authorization"] == "Bearer rest-secret"
    assert second_kwargs["warehouse"] == "other"
    assert catalog.iceberg_rest_catalog_kwargs == original


@pytest.mark.parametrize(
    "auth",
    [
        {
            "auth": {
                "type": "basic",
                "basic": {"username": "u", "password": "rest-secret"},
            },
            "token": "unused-token",
            "credential": "unused-secret",
        },
        {
            "auth": {
                "type": "oauth2",
                "oauth2": {
                    "client_id": "client",
                    "client_secret": "rest-secret",
                    "token_url": "https://oauth/token",
                },
            },
        },
        {
            "auth": {"type": "noop"},
            "header.Authorization": "Bearer rest-secret",
        },
    ],
)
def test_explicit_iceberg_auth_is_preserved(auth):
    catalog = GravitinoCatalog(
        uri="http://gravitino:8090",
        metalake="test",
        iceberg_rest_uri="http://iceberg",
        iceberg_rest_catalog_kwargs=auth,
    )
    resolved = catalog.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG)
    catalog_kwargs = resolved.catalog_kwargs
    assert catalog_kwargs is not None
    for key, value in auth.items():
        assert catalog_kwargs[key] == value


@pytest.mark.parametrize(
    "auth",
    [None, {}, {"type": None}, {"type": ""}, {"type": " "}, {"type": 1}, [], "secret"],
)
def test_invalid_auth_is_secret_safe(auth):
    catalog = GravitinoCatalog(
        uri="http://gravitino:8090",
        metalake="test",
        iceberg_rest_uri="http://iceberg",
        iceberg_rest_catalog_kwargs={"auth": auth},
    )
    with pytest.raises(ValueError, match="auth must be a dictionary") as caught:
        catalog.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG)
    assert "secret" not in str(caught.value)


def test_iceberg_requires_explicit_rest_uri():
    catalog = GravitinoCatalog(uri="http://gravitino:8090", metalake="test")
    with pytest.raises(ValueError, match="iceberg_rest_uri is required"):
        catalog.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG)


@pytest.mark.parametrize("reader", [ReaderFormat.DELTA, ReaderFormat.PARQUET])
def test_deferred_formats_rejected(catalog, reader):
    with pytest.raises(ValueError, match="does not support format"):
        catalog.resolve("cat.db.tbl", reader=reader)


@pytest.mark.parametrize(
    "identifier",
    [None, 1, "", "table", "db.table", "a.b.c.d", "a..c", "a. .c", ".b.c", "a.b."],
)
def test_invalid_identifier(catalog, identifier):
    with pytest.raises(ValueError, match="three-part"):
        catalog.resolve(identifier, reader=ReaderFormat.ICEBERG)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"reader": None},
        {"reader": "iceberg"},
        {"reader": ReaderFormat.ICEBERG, "mode": None},
        {"reader": ReaderFormat.ICEBERG, "mode": "read"},
    ],
)
def test_invalid_reader_or_mode(catalog, kwargs):
    with pytest.raises(ValueError):
        catalog.resolve("cat.db.tbl", **kwargs)


def test_read_api_wiring(catalog):
    with (
        mock.patch(
            "ray.data._internal.datasource.iceberg_datasource.IcebergDatasource"
        ) as datasource,
        mock.patch("ray.data.read_api.read_datasource", return_value=object()) as read,
    ):
        result = ray.data.read_iceberg(table_identifier="cat.db.tbl", catalog=catalog)
    assert result is read.return_value
    assert datasource.call_args.kwargs["table_identifier"] == "db.tbl"
    assert datasource.call_args.kwargs["catalog_kwargs"]["warehouse"] == "cat"


def test_append_api_wiring(catalog):
    dataset = object.__new__(ray.data.Dataset)
    dataset._current_executor = None
    with (
        mock.patch("ray.data.dataset.IcebergDatasink") as sink,
        mock.patch.object(ray.data.Dataset, "write_datasink"),
        mock.patch.object(
            catalog.__class__, "resolve", wraps=catalog.resolve
        ) as resolve,
    ):
        dataset.write_iceberg("cat.db.tbl", catalog=catalog)
    assert sink.call_args.kwargs["table_identifier"] == "db.tbl"
    assert sink.call_args.kwargs["mode"] is ray.data.SaveMode.APPEND
    resolve.assert_called_once_with(
        "cat.db.tbl", reader=ReaderFormat.ICEBERG, mode=CatalogAccessMode.WRITE
    )


@pytest.mark.parametrize("version", ["0.9.1", "0.10.0rc1"])
def test_old_pyiceberg_rejected_during_resolution(catalog, monkeypatch, version):
    import pyiceberg

    monkeypatch.setattr(pyiceberg, "__version__", version)
    with pytest.raises(ImportError) as caught:
        catalog.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG)
    assert "requires pyiceberg >= 0.10.0" in str(caught.value)
    assert f"{version} is installed" in str(caught.value)
    assert 'pip install -U "pyiceberg>=0.10.0"' in str(caught.value)


def test_resolution_uses_neither_network_nor_metadata_client(catalog, monkeypatch):
    original_import = builtins.__import__

    def without_metadata_client(name, *args, **kwargs):
        if (
            name == "gravitino"
            or name.startswith("gravitino.")
            or name == "ray.data._internal.gravitino_client"
        ):
            raise AssertionError("The adapter must not import a metadata client")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_metadata_client)
    with mock.patch(
        "requests.sessions.Session.request",
        side_effect=AssertionError("resolve must not perform network I/O"),
    ) as request:
        assert (
            catalog.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG).table_identifier
            == "db.tbl"
        )
        request.assert_not_called()


@pytest.mark.parametrize(
    "uri",
    [
        None,
        1,
        "",
        "iceberg",
        "file:///iceberg",
        "http://iceberg:0",
        "http://iceberg:bad",
        "http://user:secret@iceberg",
        "http://iceberg?token=secret",
        "http://iceberg#secret",
    ],
)
def test_invalid_gravitino_uri_is_secret_safe(uri):
    with pytest.raises(ValueError) as caught:
        GravitinoCatalog(
            uri=uri,
            metalake="test",
            iceberg_rest_uri="http://iceberg",
        )
    assert "secret" not in str(caught.value)


@pytest.mark.parametrize(
    "iceberg_rest_uri",
    ["iceberg", "file:///iceberg", "http://iceberg?token=secret"],
)
def test_invalid_iceberg_rest_uri_is_secret_safe(iceberg_rest_uri):
    with pytest.raises(ValueError) as caught:
        GravitinoCatalog(
            uri="http://gravitino:8090",
            metalake="test",
            iceberg_rest_uri=iceberg_rest_uri,
        )
    assert "secret" not in str(caught.value)


@pytest.mark.parametrize("metalake", [None, 1, "", "   "])
def test_invalid_metalake(metalake):
    with pytest.raises(ValueError, match="metalake must be a non-empty string"):
        GravitinoCatalog(uri="http://gravitino:8090", metalake=metalake)


@pytest.mark.parametrize("properties", [[], "secret", 1, False])
def test_invalid_properties_are_secret_safe(properties):
    with pytest.raises(ValueError) as caught:
        GravitinoCatalog(
            uri="http://gravitino:8090",
            metalake="test",
            iceberg_rest_uri="http://iceberg",
            iceberg_rest_catalog_kwargs=properties,
        )
    assert "secret" not in str(caught.value)


@pytest.mark.parametrize("serializer", [pickle, cloudpickle])
def test_catalog_auth_configuration_roundtrips(serializer):
    catalog = GravitinoCatalog(
        uri="http://gravitino:8090",
        metalake="test",
        iceberg_rest_uri="http://iceberg",
        iceberg_rest_catalog_kwargs={
            "auth": {
                "type": "basic",
                "basic": {"username": "user-secret", "password": "rest-secret"},
            }
        },
    )
    assert "user-secret" not in repr(catalog)
    assert "rest-secret" not in repr(catalog)
    payload = serializer.dumps(catalog)
    restored = serializer.loads(payload)
    rest_kwargs = restored.iceberg_rest_catalog_kwargs
    expected_rest_kwargs = catalog.iceberg_rest_catalog_kwargs
    assert rest_kwargs is not None and expected_rest_kwargs is not None
    assert rest_kwargs == expected_rest_kwargs
    resolved = restored.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG)
    resolved_kwargs = resolved.catalog_kwargs
    assert resolved_kwargs is not None
    assert resolved_kwargs["auth"] == expected_rest_kwargs["auth"]


def test_anonymous_catalog_roundtrip(catalog):
    restored = pickle.loads(pickle.dumps(catalog))
    assert restored == catalog
    assert restored.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG).catalog_kwargs[
        "auth"
    ] == {"type": "noop"}


@pytest.mark.parametrize(
    "inherited_auth",
    [
        pytest.param({"token": "inherited-token"}, id="inherited-token"),
        pytest.param(
            {
                "auth": {
                    "type": "basic",
                    "basic": {
                        "username": "inherited-user",
                        "password": "inherited-password",
                    },
                }
            },
            id="inherited-basic",
        ),
    ],
)
@pytest.mark.parametrize(
    "rest_auth,expected,rejected",
    [
        ({}, None, False),
        (
            {
                "auth": {"type": "noop"},
                "header.Authorization": "Bearer rest-token",
            },
            "Bearer rest-token",
            False,
        ),
        (
            {
                "auth": {
                    "type": "basic",
                    "basic": {"username": "rest-user", "password": "rest-password"},
                }
            },
            "Basic " + base64.b64encode(b"rest-user:rest-password").decode(),
            False,
        ),
        ({"token": "rest-secret"}, None, True),
        ({"credential": "client:rest-secret"}, None, True),
    ],
)
def test_actual_pyiceberg_auth_configuration(
    rest_auth, expected, rejected, inherited_auth, monkeypatch
):
    """Exercise the installed PyIceberg API rather than only asserting kwargs."""
    from pyiceberg.catalog import load_catalog

    # Inherited authentication must not override explicit authentication or noop.
    monkeypatch.setattr(
        "pyiceberg.catalog._ENV_CONFIG.get_catalog_config",
        lambda name: inherited_auth,
    )
    observed = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            observed.append(self.headers.get("Authorization"))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"defaults": {}, "overrides": {}}).encode())

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        catalog = GravitinoCatalog(
            uri="http://gravitino:8090",
            metalake="test",
            iceberg_rest_uri=f"http://127.0.0.1:{server.server_port}/iceberg/",
            iceberg_rest_catalog_kwargs=rest_auth,
        )
        if rejected:
            with pytest.raises(ValueError, match="requires explicit auth") as caught:
                catalog.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG)
            assert "rest-secret" not in str(caught.value)
            assert observed == []
            return
        resolved = catalog.resolve("cat.db.tbl", reader=ReaderFormat.ICEBERG)
        iceberg = load_catalog("compatibility", **resolved.catalog_kwargs)
        try:
            assert observed == [expected]
        finally:
            iceberg._session.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
