import contextlib
import hashlib
import json
import re
import sys
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

from ci.ray_ci.automation.trivy import registry, trivy_db
from ci.ray_ci.automation.trivy.fake_s3 import FakeS3
from ci.ray_ci.automation.trivy.scan_store import (
    BUCKET,
    DB_FOLDER,
    LocalStore,
    ScanStore,
)
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
def fake_registry():
    """Patch registry calls; yields the mock for fetch_blob."""

    def manifest(ref):
        # The tag may have moved on since digest() resolved it: only the
        # manifest that digest pins names BLOB.
        if ref.endswith("@sha256:m1"):
            return db_manifest()
        return db_manifest(blob="sha256:" + "f" * 64)

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
    }


def test_the_real_trivy_pin_is_readable():
    """The pin comes from the trivy_pin target's file, which only the real
    job reads otherwise."""
    pin = trivy_db.load_trivy_pin()
    assert re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", pin["version"])
    assert re.fullmatch(r"[0-9a-f]{64}", pin["tarball_sha256"])
    assert pin["signer"].endswith(f"@refs/tags/v{pin['version']}")


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
            ["--output", str(out)],
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


def test_stage_snapshot_stages_into_a_local_store_too(tmp_path: Path):
    """local_scan_images stages the same DBs into a local folder."""
    store = LocalStore(tmp_path / "work")
    staged = []

    def stage(source, target_store, workdir):
        # metadata.json comes last, once every DB is in place.
        staged.append((source.name, target_store.exists(DB_FOLDER, "metadata.json")))
        return {"blob": source.name}

    (tmp_path / "w").mkdir()
    with mock.patch.object(trivy_db, "stage_db", stage), mock.patch.object(
        trivy_db, "load_trivy_pin", return_value={"version": "0.76.1"}
    ):
        local_copy = trivy_db.stage_snapshot(store, tmp_path / "w")
    assert staged == [("db", False), ("java_db", False)]
    expected = {
        "trivy": {"version": "0.76.1"},
        "db": {"blob": "db"},
        "java_db": {"blob": "java_db"},
    }
    assert json.loads(store.path(DB_FOLDER, "metadata.json").read_text()) == expected
    assert json.loads(local_copy.read_text()) == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
