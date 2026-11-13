import contextlib
import hashlib
import json
import sys
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

from ci.ray_ci.automation.trivy import registry, trivy_db
from ci.ray_ci.automation.trivy.fake_s3 import FakeS3
from ci.ray_ci.automation.trivy.scan_store import BUCKET, ScanStore
from ci.ray_ci.automation.trivy.trivy_db import (
    DB_SOURCES,
    TrivyDbError,
    db_layer,
    stage_db,
)

DB, JAVA_DB = DB_SOURCES
COMMIT = "80ceee083e49b1ae709c149d3a15862fe12ce8cb"
PREFIX = "trivy-scans/postmerge/master/20897-80ceee083e49"
DB_BYTES = b"trivy db tarball"
BLOB = "sha256:" + hashlib.sha256(DB_BYTES).hexdigest()


def store(s3: FakeS3) -> ScanStore:
    return ScanStore.for_build("postmerge", "master", "20897", COMMIT, client=s3)


def db_manifest(blob: str = BLOB, media_type: str = DB.media_type) -> dict:
    return {
        "annotations": {"org.opencontainers.image.created": "2026-11-10T19:09:25Z"},
        "layers": [{"mediaType": media_type, "digest": blob, "size": 133}],
    }


@contextlib.contextmanager
def fake_registry(ghcr_manifest=None):
    """Patch registry calls; yields the mock for fetch_blob."""

    def manifest(ref):
        if ref.startswith("ghcr.io"):
            if isinstance(ghcr_manifest, Exception):
                raise ghcr_manifest
            return ghcr_manifest or db_manifest()
        return db_manifest()

    fetch_blob = mock.Mock(side_effect=lambda repo, d, dest: dest.write_bytes(DB_BYTES))
    with mock.patch.multiple(
        trivy_db.registry,
        digest=mock.Mock(return_value="sha256:m1"),
        manifest=manifest,
        fetch_blob=fetch_blob,
    ):
        yield fetch_blob


@pytest.mark.parametrize(
    ("source", "expected"),
    [(DB, "trivy-db.tar.gz"), (JAVA_DB, "trivy-java-db.tar.gz")],
)
def test_file_names(source, expected: str):
    assert source.file_name == expected


def test_stage_db_uploads_the_pinned_blob_and_records_the_pin(tmp_path: Path):
    s3 = FakeS3()
    with fake_registry() as fetch_blob:
        pin = stage_db(DB, store(s3), tmp_path)
    # The blob comes from the manifest the digest pins, not from the tag.
    fetch_blob.assert_called_once_with(
        "public.ecr.aws/aquasecurity/trivy-db", BLOB, tmp_path / "trivy-db.tar.gz"
    )
    assert s3.get(BUCKET, f"{PREFIX}/db/trivy-db.tar.gz") == DB_BYTES
    assert not (tmp_path / "trivy-db.tar.gz").exists()
    assert pin == {
        "source": DB.upstream,
        "manifest": "sha256:m1",
        "blob": BLOB,
        "size": 133,
        "media_type": DB.media_type,
        "created": "2026-11-10T19:09:25Z",
        "object": f"s3://{BUCKET}/{PREFIX}/db/trivy-db.tar.gz",
        "ghcr_layer_matches": True,
    }


@pytest.mark.parametrize(
    ("ghcr", "expected"),
    [
        pytest.param(db_manifest(blob="sha256:" + "c" * 64), False, id="layer-differs"),
        pytest.param(registry.RegistryError("denied"), None, id="unreachable"),
    ],
)
def test_ghcr_disagreement_is_recorded_not_fatal(tmp_path: Path, ghcr, expected):
    with fake_registry(ghcr_manifest=ghcr):
        pin = stage_db(DB, store(FakeS3()), tmp_path)
    assert pin["ghcr_layer_matches"] is expected


def test_ghcr_check_can_be_skipped(tmp_path: Path):
    with fake_registry(ghcr_manifest=registry.RegistryError("never called")):
        pin = stage_db(DB, store(FakeS3()), tmp_path, check_ghcr=False)
    assert pin["ghcr_layer_matches"] is None


@pytest.mark.parametrize(
    "layers",
    [
        [],
        [{"mediaType": DB.media_type, "digest": BLOB}] * 2,
        [{"mediaType": "application/octet-stream", "digest": BLOB}],
    ],
    ids=["none", "two", "wrong-media-type"],
)
def test_db_layer_needs_exactly_one_db_layer(layers: list):
    with pytest.raises(TrivyDbError, match="expected one"):
        db_layer({"layers": layers}, DB.media_type)


def run_main(tmp_path: Path, s3: FakeS3, stage=None):
    out = tmp_path / "trivy" / "metadata.json"
    pin = {"version": "0.76.1", "tarball_sha256": "c7cb", "signer": "id"}
    uploads = []
    original = s3.upload_file

    def record(filename, bucket, key):
        uploads.append(key.rsplit("/", 1)[1])
        original(filename, bucket, key)

    s3.upload_file = record
    with mock.patch.object(
        trivy_db, "load_trivy_pin", return_value=pin
    ), mock.patch.object(
        trivy_db, "stage_db", side_effect=stage or (lambda s, *_: {"blob": s.name})
    ), mock.patch.object(
        trivy_db.scan_store.boto3, "client", return_value=s3
    ):
        result = CliRunner().invoke(
            trivy_db.main,
            ["--output", str(out), "--work-dir", str(tmp_path)],
            env={
                "BUILDKITE_PIPELINE_SLUG": "postmerge",
                "BUILDKITE_BRANCH": "master",
                "BUILDKITE_BUILD_NUMBER": "20897",
                "BUILDKITE_COMMIT": COMMIT,
            },
        )
    return result, out, pin, uploads


def test_main_writes_and_uploads_metadata(tmp_path: Path):
    s3 = FakeS3()
    result, out, pin, uploads = run_main(tmp_path, s3)
    assert result.exit_code == 0, result.output
    expected = {"trivy": pin, "db": {"blob": "db"}, "java_db": {"blob": "java_db"}}
    assert json.loads(out.read_text()) == expected
    assert json.loads(s3.get(BUCKET, f"{PREFIX}/db/metadata.json")) == expected
    assert uploads == ["metadata.json"]


def test_main_uploads_no_metadata_when_a_db_fails(tmp_path: Path):
    """Scan jobs read metadata.json first; it must only exist once every DB
    it names has been uploaded."""
    s3 = FakeS3()

    def stage(source, *_):
        if source.name == "java_db":
            raise registry.RegistryError("toomanyrequests")
        return {"blob": source.name}

    result, out, _, _ = run_main(tmp_path, s3, stage)
    assert result.exit_code != 0
    assert (BUCKET, f"{PREFIX}/db/metadata.json") not in s3.objects


def test_a_retried_job_keeps_the_builds_db_snapshot(tmp_path: Path):
    """Scan jobs may already have used the first snapshot; a retry must not
    give the rest of the build a newer DB."""
    s3 = FakeS3()
    first = b'{"db": {"blob": "first"}}\n'
    s3.put(BUCKET, f"{PREFIX}/db/metadata.json", first)

    def stage(source, *_):
        raise AssertionError("re-staged the DB")

    result, out, _, uploads = run_main(tmp_path, s3, stage)
    assert result.exit_code == 0, result.output
    assert out.read_bytes() == first
    assert uploads == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
