import json
import sys
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

from ci.ray_ci.automation.trivy import registry, trivy_db
from ci.ray_ci.automation.trivy.trivy_db import (
    DB_SOURCES,
    TrivyDbError,
    db_layer,
    mirror_db,
    mirror_ref,
)

DB, JAVA_DB = DB_SOURCES
WORK_REPO = "123.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp"
BLOB = "sha256:" + "b" * 64


def db_manifest(blob: str = BLOB, media_type: str = DB.media_type) -> dict:
    return {
        "annotations": {"org.opencontainers.image.created": "2026-11-10T19:09:25Z"},
        "layers": [{"mediaType": media_type, "digest": blob, "size": 133}],
    }


def fake_registry(
    upstream_digest: str = "sha256:m1",
    mirrored_digest: str = "sha256:m1",
    ghcr_manifest=None,
):
    """Patch registry calls; returns the mock for copy()."""
    digests = {DB.upstream: upstream_digest}

    def digest(ref, platform=None):
        return digests.get(ref, mirrored_digest)

    def manifest(ref):
        if ref.startswith("ghcr.io"):
            if isinstance(ghcr_manifest, Exception):
                raise ghcr_manifest
            return ghcr_manifest or db_manifest()
        return db_manifest()

    return mock.patch.multiple(
        trivy_db.registry, digest=digest, manifest=manifest, copy=mock.DEFAULT
    )


@pytest.mark.parametrize(
    ("source", "expected"),
    [(DB, f"{WORK_REPO}:abc-trivy-db"), (JAVA_DB, f"{WORK_REPO}:abc-trivy-java-db")],
)
def test_mirror_ref(source, expected: str):
    assert mirror_ref(WORK_REPO, "abc", source) == expected


def test_mirror_db_copies_by_digest_and_records_the_pin():
    with fake_registry() as mocks:
        pin = mirror_db(DB, WORK_REPO, "abc")
    mocks["copy"].assert_called_once_with(
        "public.ecr.aws/aquasecurity/trivy-db@sha256:m1", f"{WORK_REPO}:abc-trivy-db"
    )
    assert pin == {
        "source": DB.upstream,
        "manifest": "sha256:m1",
        "blob": BLOB,
        "size": 133,
        "media_type": DB.media_type,
        "created": "2026-11-10T19:09:25Z",
        "mirror": f"{WORK_REPO}:abc-trivy-db",
        "ghcr_layer_matches": True,
    }


def test_mirror_db_rejects_a_copy_that_changed_the_digest():
    with fake_registry(mirrored_digest="sha256:other"):
        with pytest.raises(TrivyDbError, match="after copying"):
            mirror_db(DB, WORK_REPO, "abc")


@pytest.mark.parametrize(
    ("ghcr", "expected"),
    [
        pytest.param(db_manifest(blob="sha256:" + "c" * 64), False, id="layer-differs"),
        pytest.param(registry.RegistryError("denied"), None, id="unreachable"),
    ],
)
def test_ghcr_disagreement_is_recorded_not_fatal(ghcr, expected):
    with fake_registry(ghcr_manifest=ghcr):
        pin = mirror_db(DB, WORK_REPO, "abc")
    assert pin["ghcr_layer_matches"] is expected


def test_ghcr_check_can_be_skipped():
    with fake_registry(ghcr_manifest=registry.RegistryError("never called")):
        pin = mirror_db(DB, WORK_REPO, "abc", check_ghcr=False)
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


def test_main_writes_metadata(tmp_path: Path):
    out = tmp_path / "trivy" / "metadata.json"
    pin = {"version": "0.76.1", "tarball_sha256": "c7cb", "signer": "id"}
    with mock.patch.object(trivy_db, "ecr_docker_login") as login, mock.patch.object(
        trivy_db, "load_trivy_pin", return_value=pin
    ), mock.patch.object(
        trivy_db, "mirror_db", side_effect=lambda s, *_: {"mirror": s.name}
    ):
        result = CliRunner().invoke(
            trivy_db.main,
            [
                "--rayci-work-repo",
                WORK_REPO,
                "--rayci-build-id",
                "abc",
                "--output",
                str(out),
            ],
        )
    assert result.exit_code == 0, result.output
    login.assert_called_once_with("123.dkr.ecr.us-west-2.amazonaws.com")
    assert json.loads(out.read_text()) == {
        "trivy": pin,
        "db": {"mirror": "db"},
        "java_db": {"mirror": "java_db"},
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
