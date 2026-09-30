"""End-to-end ``write_delta(catalog=...)`` tests over a real S3-compatible endpoint.

Everything else covering ``catalog=`` runs in-process against a test double.
These tests instead drive the whole path for real: a catalog resolves the table
to an ``s3://`` location and vends credentials, the datasink (with the catalog
attached) is pickled out to real Ray worker processes, each worker writes
Parquet over S3, and the driver commits one Delta transaction -- then the table
is read back and compared.

Storage is a local ``moto`` S3 server, via the ``s3_server`` fixture the rest of
the Ray Data suite already uses. That makes these tests hermetic; it also means
they only run where ``moto_server`` is installed, and skip cleanly otherwise.

Deliberately *not* covered here: recovery from a worker-side authentication
failure. moto does not enforce credentials -- writing with deliberately invalid
keys succeeds against it -- so there is no way to provoke a genuine auth error.
Loosening ``AUTH_ERROR_PATTERNS`` to match some other error would only make a
test pass, so that behavior stays unit-tested in ``test_write_delta.py``
(``test_worker_refresh_via_catalog_succeeds_for_aws_shaped_response`` and
friends) where the failure can be injected honestly.
"""

import uuid

import pytest

import ray
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.tests.catalog_test_utils import FakeCatalog
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

pa = pytest.importorskip("pyarrow")
pytest.importorskip("deltalake")

from packaging.version import parse as parse_version  # noqa: E402

_pa_version = get_pyarrow_version()
pytestmark = pytest.mark.skipif(
    _pa_version is None or _pa_version < parse_version("15.0.0"),
    reason="deltalake requires pyarrow >= 15.0",
)

# On pyarrow<19 ``read_delta`` hands partition columns back as the raw Hive path
# strings rather than their declared types, so a partitioned round trip can't be
# value-compared there. See the longer note in ``test_write_delta.py``; it's a
# read-path gap, unrelated to the catalog wiring this file covers.
_skip_without_typed_partition_values = pytest.mark.skipif(
    _pa_version is None or _pa_version < parse_version("19.0.0"),
    reason="read_delta returns partition values as untyped Hive path strings on pyarrow<19",
)


@pytest.fixture
def s3_delta_table(aws_credentials, s3_server):
    """A fresh S3 bucket on the local moto server, plus the ``storage_options``
    needed to reach it.

    Yields ``(path, storage_options)``. ``AWS_ALLOW_HTTP`` is required because
    ``deltalake``'s Rust object_store refuses a non-TLS endpoint otherwise, and
    ``AWS_ENDPOINT_URL`` is what points both the driver's commit and the
    workers' Parquet writes at moto rather than real AWS.
    """
    import pyarrow.fs as pafs

    bucket = f"delta-e2e-{uuid.uuid4().hex[:12]}"
    storage_options = {
        "AWS_ACCESS_KEY_ID": aws_credentials["access_key"],
        "AWS_SECRET_ACCESS_KEY": aws_credentials["secret_key"],
        "AWS_SESSION_TOKEN": aws_credentials["session_token"],
        "AWS_REGION": "us-west-2",
        "AWS_ENDPOINT_URL": s3_server,
        "AWS_ALLOW_HTTP": "true",
    }

    fs = pafs.S3FileSystem(
        access_key=aws_credentials["access_key"],
        secret_key=aws_credentials["secret_key"],
        session_token=aws_credentials["session_token"],
        region="us-west-2",
        endpoint_override=s3_server,
        allow_bucket_creation=True,
        allow_bucket_deletion=True,
    )
    fs.create_dir(bucket)
    yield f"s3://{bucket}/tbl", storage_options


def _catalog_for(path, storage_options):
    """A catalog that vends ``path`` plus an endpoint-aware S3 filesystem.

    Mirrors what ``DatabricksUnityCatalog`` returns for an AWS-backed Delta
    table -- an explicit, picklable filesystem carrying the credentials -- which
    is the shape the datasink's worker-side handling depends on. The real class
    can't be used verbatim here because ``_build_s3_filesystem`` has no endpoint
    override, so it would always target real AWS; the UC translation itself is
    covered in ``test_catalog.py`` instead.
    """
    import pyarrow.fs as pafs

    from ray.data.catalog import ResolvedSource

    fs = pafs.S3FileSystem(
        access_key=storage_options["AWS_ACCESS_KEY_ID"],
        secret_key=storage_options["AWS_SECRET_ACCESS_KEY"],
        session_token=storage_options["AWS_SESSION_TOKEN"],
        region=storage_options["AWS_REGION"],
        endpoint_override=storage_options["AWS_ENDPOINT_URL"],
    )
    return FakeCatalog(
        ResolvedSource(path=path, filesystem=fs, storage_options=storage_options)
    )


def _read_back(path, storage_options):
    return sorted(
        ray.data.read_delta(path, storage_options=storage_options).take_all(),
        key=lambda r: r["id"],
    )


def test_multi_block_write_via_catalog_round_trips(
    ray_start_regular_shared, s3_delta_table
):
    """The full distributed path: resolve on the driver, ship the datasink (and
    catalog) to several real workers, write Parquet to S3 in parallel, commit
    once, read back.

    This is the test that would have caught ``_write_parquet`` deriving the
    table root via ``FileSystem.from_uri``: that also *builds* a filesystem, so
    for an ``s3://`` URI it resolved the bucket against real AWS and failed
    outright against any S3-compatible endpoint.
    """
    path, storage_options = s3_delta_table
    catalog = _catalog_for(path, storage_options)
    rows = [{"id": i, "v": f"r{i}"} for i in range(12)]

    ray.data.from_items(rows, override_num_blocks=4).write_delta(
        "main.db.tbl", catalog=catalog
    )

    from ray.data.catalog import CatalogAccessMode, ReaderFormat

    assert catalog.calls == [
        ("main.db.tbl", ReaderFormat.DELTA, CatalogAccessMode.WRITE)
    ]
    assert _read_back(path, storage_options) == rows


@_skip_without_typed_partition_values
def test_partitioned_write_via_catalog_round_trips(
    ray_start_regular_shared, s3_delta_table
):
    """Partitioned writes through a catalog round-trip too.

    Worth covering separately: Delta takes a partition column's value from the
    ``AddAction`` metadata the workers report, not from the Parquet file, so the
    partition values only survive if that metadata makes it back to the driver
    intact across the S3 write path.
    """
    path, storage_options = s3_delta_table
    catalog = _catalog_for(path, storage_options)
    rows = [{"id": i, "year": 2024 + (i % 2), "v": f"r{i}"} for i in range(8)]

    ray.data.from_items(rows, override_num_blocks=2).write_delta(
        "main.db.tbl", catalog=catalog, partition_by=["year"]
    )

    out = _read_back(path, storage_options)
    assert out == rows
    assert {r["year"] for r in out} == {2024, 2025}


def test_append_via_catalog_accumulates(ray_start_regular_shared, s3_delta_table):
    """A second catalog-backed APPEND adds to the table rather than replacing
    it, and re-resolves the catalog for the new write."""
    path, storage_options = s3_delta_table
    catalog = _catalog_for(path, storage_options)

    first = [{"id": i, "v": "a"} for i in range(4)]
    second = [{"id": i + 100, "v": "b"} for i in range(4)]
    ray.data.from_items(first).write_delta("main.db.tbl", catalog=catalog)
    ray.data.from_items(second).write_delta("main.db.tbl", catalog=catalog)

    assert len(catalog.calls) == 2
    assert _read_back(path, storage_options) == first + second


def test_overwrite_via_catalog_replaces(ray_start_regular_shared, s3_delta_table):
    """OVERWRITE through a catalog replaces the table's data over S3."""
    from ray.data import SaveMode

    path, storage_options = s3_delta_table
    catalog = _catalog_for(path, storage_options)

    ray.data.from_items([{"id": i, "v": "old"} for i in range(6)]).write_delta(
        "main.db.tbl", catalog=catalog
    )
    new_rows = [{"id": i, "v": "new"} for i in range(2)]
    ray.data.from_items(new_rows).write_delta(
        "main.db.tbl", catalog=catalog, mode=SaveMode.OVERWRITE
    )

    assert _read_back(path, storage_options) == new_rows


def test_catalog_vended_credentials_reach_workers(
    ray_start_regular_shared, s3_delta_table, monkeypatch
):
    """Workers must authenticate with the catalog-vended filesystem, not with
    whatever credentials happen to be in their own environment.

    Ambient credentials are cleared from the driver's environment before the
    write, so nothing a worker inherits could substitute for the vended ones. A
    passing write means the credentials genuinely travelled with the datasink.
    """
    path, storage_options = s3_delta_table
    catalog = _catalog_for(path, storage_options)

    for key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_ENDPOINT_URL",
    ):
        monkeypatch.delenv(key, raising=False)

    rows = [{"id": i, "v": f"r{i}"} for i in range(6)]
    ray.data.from_items(rows, override_num_blocks=3).write_delta(
        "main.db.tbl", catalog=catalog
    )

    assert _read_back(path, storage_options) == rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
