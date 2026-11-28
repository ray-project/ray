import gzip
import hashlib
import json
import logging
import subprocess
import sys
import tempfile
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional
from unittest import mock

import click
import pytest
from click.testing import CliRunner

from ci.ray_ci.automation.trivy import local_scan_images, registry, scan_images
from ci.ray_ci.automation.trivy.fake_tools import FakeDocker, faked_scan_tools
from ci.ray_ci.automation.trivy.local_scan_images import (
    WorkDirLock,
    local_targets,
    read_images,
)
from ci.ray_ci.automation.trivy.scan_images import (
    GROUP_FAILED,
    check_target,
    write_status,
)
from ci.ray_ci.automation.trivy.scan_store import DB_FOLDER
from ci.ray_ci.automation.trivy.trivy_db import DB_METADATA
from ci.ray_ci.automation.trivy.trivy_report import (
    EXIT_CLEAN,
    EXIT_FINDINGS,
    EXIT_INCOMPLETE,
)

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
CLEAN_RESULT = {
    "SchemaVersion": 2,
    "Results": [{"Target": "debian", "Class": "os-pkgs", "Packages": [{}]}],
}
IMAGES = ("rayproject/ray:2.60.0-py312-cpu", "ubuntu:22.04")


@pytest.fixture(autouse=True)
def no_real_processes(monkeypatch):
    """Fail any test that would run real crane or docker."""

    def refuse(args, *a, **k):
        raise AssertionError(f"test tried to run {list(args)[:3]}")

    monkeypatch.setattr(scan_images.subprocess, "run", refuse)
    monkeypatch.setattr(registry.subprocess, "run", refuse)


def digest_of(ref: str) -> str:
    return "sha256:" + hashlib.sha256(ref.encode()).hexdigest()


@pytest.mark.parametrize(
    ("image", "name", "repo"),
    [
        (
            "rayproject/ray:2.60.0-py312-cpu",
            "rayproject_ray_2.60.0-py312-cpu",
            "rayproject/ray",
        ),
        ("ubuntu", "ubuntu", "ubuntu"),
        (
            "localhost:5000/team/img",
            "localhost_5000_team_img",
            "localhost:5000/team/img",
        ),
        ("localhost:5000/img:1.0", "localhost_5000_img_1.0", "localhost:5000/img"),
        (
            "ghcr.io/o/i@sha256:" + "a" * 64,
            "ghcr.io_o_i_sha256_" + "a" * 64,
            "ghcr.io/o/i",
        ),
        (
            "ghcr.io/o/i:v1@sha256:" + "b" * 64,
            "ghcr.io_o_i_v1_sha256_" + "b" * 64,
            "ghcr.io/o/i",
        ),
    ],
)
def test_a_local_target_takes_its_name_and_repo_from_the_image(
    image: str, name: str, repo: str
):
    (target,) = local_targets([image], "x86_64")
    assert (target.name, target.repo) == (name, repo)
    target.digest = "sha256:" + "c" * 64
    assert target.pinned_ref == f"{repo}@{target.digest}"


def test_a_local_target_records_no_ray_variant():
    (target,) = local_targets(["ubuntu:22.04"], "aarch64")
    record = target.status_record()
    assert {k: record[k] for k in ("name", "image_type", "python_version")} == {
        "name": "ubuntu_22.04",
        "image_type": "image",
        "python_version": "",
    }
    assert (record["platform"], record["architecture"]) == ("", "aarch64")
    assert (record["ref"], record["published_tags"], record["status"]) == (
        "ubuntu:22.04",
        [],
        "error",
    )


def test_images_come_from_the_options_then_the_file(tmp_path: Path):
    listed = tmp_path / "images.txt"
    listed.write_text("# images for the release\nubuntu:22.04  # base\n\nalpine:3.20\n")
    assert read_images(["rayproject/ray:latest"], listed) == [
        "rayproject/ray:latest",
        "ubuntu:22.04",
        "alpine:3.20",
    ]
    assert read_images(["a"], None) == ["a"]


@pytest.mark.parametrize(
    ("images", "error"),
    [
        (["-flag"], "is not an image name"),
        (["a b"], "is not an image name"),
        (["a|b"], "is not an image name"),
        (["a`b"], "is not an image name"),
        (["ubuntu:22.04", "ubuntu:22.04"], "ubuntu:22.04 is given twice"),
        (["a/b:c", "a_b:c"], "a/b:c and a_b:c would share the result file a_b_c"),
    ],
)
def test_images_it_cannot_scan_safely_are_rejected(images: list, error: str):
    with pytest.raises(click.BadParameter, match=error):
        local_targets(images, "x86_64")


def test_some_image_is_needed():
    with pytest.raises(click.UsageError, match="at least one --image"):
        local_targets([], "x86_64")


@pytest.mark.parametrize(
    ("arch", "platform"), [("x86_64", "linux/amd64"), ("aarch64", "linux/arm64")]
)
def test_check_target_resolves_a_multi_platform_image(arch: str, platform: str):
    """Public tags usually name an index; a local target scans the image for
    its architecture, where CI refuses an index."""
    (target,) = local_targets(["rayproject/ray:2.60.0-py312-cpu"], arch)
    digest = mock.Mock(return_value="sha256:" + "d" * 64)
    with mock.patch.multiple(
        scan_images.registry,
        digest=digest,
        manifest=mock.Mock(return_value={"mediaType": OCI_MANIFEST}),
        config=mock.Mock(return_value={"architecture": platform.split("/")[1]}),
    ):
        check_target(target, arch)
    digest.assert_called_once_with("rayproject/ray:2.60.0-py312-cpu", platform)
    assert (target.error, target.digest) == ("", "sha256:" + "d" * 64)


def write_metadata(store_root: Path, created: str) -> Path:
    path = store_root / DB_FOLDER / DB_METADATA
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"trivy": {"version": "0.76.1"}, "db": {"created": created}})
    )
    return path


class FakeStageSnapshot:
    """Stands in for trivy_db.stage_snapshot: records where each call staged
    the DBs, and whether the kept DBs in ``kept`` were still there then."""

    def __init__(
        self, error: Optional[Exception] = None, kept: Optional[Path] = None
    ) -> None:
        self.calls: list[tuple[Path, bool]] = []
        self.error = error
        self.kept = kept

    def __call__(self, store, workdir):
        kept = bool(self.kept and (self.kept / DB_FOLDER / DB_METADATA).is_file())
        self.calls.append((store.root, kept))
        if self.error:
            raise self.error
        return write_metadata(store.root, "2026-11-24T06:12:00Z")


class FakeScanGroup:
    """Stands in for scan_images.scan_group: every image gets ``result``, or
    fails with ``error``."""

    def __init__(self, result: Optional[dict] = None, error: str = "") -> None:
        self.result = result or CLEAN_RESULT
        self.error = error
        self.kw: dict = {}
        self.ignore_text = ""
        self.called = False

    def __call__(self, targets, architecture, store, results_dir, workdir, base, **kw):
        self.called = True
        self.kw = kw
        self.ignore_text = kw["ignore_files"]["image"].read_text()
        for target in targets:
            if self.error:
                target.error = self.error
            else:
                result = results_dir / f"{target.name}.json.gz"
                result.write_bytes(gzip.compress(json.dumps(self.result).encode()))
                target.scanned = True
            write_status(results_dir, target)


def resolves(ref: str, platform: Optional[str] = None) -> str:
    return digest_of(ref)


def invoke(
    tmp_path: Path, *args: str, stage_snapshot=None, scan_group=None, digest=resolves
):
    """Run main with the DB download and the scan faked. Images resolve
    unless ``digest`` raises."""
    stage_snapshot = stage_snapshot or FakeStageSnapshot()
    scan_group = scan_group or FakeScanGroup()
    with mock.patch.object(
        local_scan_images.trivy_db, "stage_snapshot", stage_snapshot
    ), mock.patch.object(
        local_scan_images.scan_images, "scan_group", scan_group
    ), mock.patch.multiple(
        scan_images.registry,
        digest=digest,
        manifest=mock.Mock(return_value={"mediaType": OCI_MANIFEST}),
        config=mock.Mock(return_value={"architecture": "amd64"}),
    ):
        result = CliRunner().invoke(
            local_scan_images.main,
            ["--image", "ubuntu:22.04", "--output-dir", str(tmp_path / "out"), *args],
            env={"BUILD_WORKING_DIRECTORY": str(tmp_path)},
        )
    return result, stage_snapshot, scan_group


def warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def test_without_a_work_dir_the_dbs_are_fetched_with_a_warning(tmp_path: Path, caplog):
    caplog.set_level(logging.INFO)
    result, stage_snapshot, _ = invoke(tmp_path)
    assert result.exit_code == EXIT_CLEAN, result.output
    ((root, _),) = stage_snapshot.calls
    assert not root.exists()  # a temporary directory, gone after the run
    (warning,) = warnings(caplog)
    assert "for this run only" in warning
    assert f"temporary directory under {tempfile.gettempdir()}" in warning
    assert "Pass --work-dir DIR to keep them" in warning
    assert "reuse them, saving bandwidth and disk" in warning
    assert "grows stale, so pass --refresh-db" in warning


def test_a_new_work_dir_fetches_and_keeps_the_dbs_with_a_warning(
    tmp_path: Path, caplog
):
    caplog.set_level(logging.INFO)
    result, stage_snapshot, _ = invoke(tmp_path, "--work-dir", "cache")
    assert result.exit_code == EXIT_CLEAN, result.output
    work = tmp_path / "cache"
    ((root, _),) = stage_snapshot.calls
    # Downloaded beside the kept DBs, then moved into place.
    assert root.is_relative_to(work) and root != work
    assert (work / DB_FOLDER / DB_METADATA).is_file()
    # The scan's temporary files are gone; the DBs and the lock file stay.
    assert sorted(p.name for p in work.iterdir()) == [".lock", DB_FOLDER]
    (warning,) = warnings(caplog)
    assert f"{work} has no Trivy DBs yet" in warning
    assert f"Later runs with --work-dir {work} reuse them" in warning
    assert "grows stale, so pass --refresh-db" in warning


def test_kept_dbs_are_reused_without_a_warning(tmp_path: Path, caplog):
    caplog.set_level(logging.INFO)
    created = (date.today() - timedelta(days=3)).isoformat()
    write_metadata(tmp_path / "cache", f"{created}T06:12:00Z")
    result, stage_snapshot, scan_group = invoke(tmp_path, "--work-dir", "cache")
    assert result.exit_code == EXIT_CLEAN, result.output
    assert stage_snapshot.calls == []
    assert scan_group.called
    assert warnings(caplog) == []
    assert (
        f"Reusing the Trivy DBs in {tmp_path / 'cache'} (DB created {created}, "
        "3 days ago); pass --refresh-db to download fresh ones."
    ) in caplog.text


def test_refresh_db_replaces_kept_dbs_without_a_warning(tmp_path: Path, caplog):
    caplog.set_level(logging.INFO)
    work = tmp_path / "cache"
    write_metadata(work, "2026-01-01T00:00:00Z")
    stage_snapshot = FakeStageSnapshot(kept=work)
    result, _, _ = invoke(
        tmp_path, "--work-dir", "cache", "--refresh-db", stage_snapshot=stage_snapshot
    )
    assert result.exit_code == EXIT_CLEAN, result.output
    # The kept DBs stayed until the new ones were complete.
    ((_, kept),) = stage_snapshot.calls
    assert kept
    assert sorted(p.name for p in work.iterdir()) == [".lock", DB_FOLDER]
    assert warnings(caplog) == []
    assert "Downloading fresh Trivy DBs" in caplog.text
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["scanner"]["db"]["created"] == "2026-11-24T06:12:00Z"


def test_refresh_db_needs_a_work_dir(tmp_path: Path):
    result, stage_snapshot, _ = invoke(tmp_path, "--refresh-db")
    assert result.exit_code == 2
    assert "--refresh-db needs --work-dir" in result.output
    assert stage_snapshot.calls == []


def test_a_failed_db_download_stops_the_run_with_its_cause(tmp_path: Path):
    stage_snapshot = FakeStageSnapshot(
        error=registry.RegistryError("crane digest failed")
    )
    result, _, scan_group = invoke(tmp_path, stage_snapshot=stage_snapshot)
    assert result.exit_code == 1
    assert "Could not download the Trivy DBs: crane digest failed" in result.output
    assert not scan_group.called


@pytest.mark.parametrize("name", ["mine.trivyignore.yaml", "mine.yml"])
def test_the_given_ignore_file_applies_to_every_image(tmp_path: Path, name: str):
    mine = tmp_path / name
    mine.write_text("vulnerabilities:\n  - id: CVE-2026-0001\n")
    result, _, scan_group = invoke(tmp_path, "--ignorefile", name)
    assert result.exit_code == EXIT_CLEAN, result.output
    assert scan_group.kw["ignore_files"] == {"image": mine}


def test_without_an_ignore_file_nothing_is_ignored(tmp_path: Path):
    result, _, scan_group = invoke(tmp_path)
    assert result.exit_code == EXIT_CLEAN, result.output
    assert scan_group.ignore_text.startswith("#")
    assert "vulnerabilities" not in scan_group.ignore_text


@pytest.mark.parametrize("option", ["--ignorefile", "--images-file"])
def test_a_missing_input_file_is_a_usage_error(tmp_path: Path, option: str):
    result, stage_snapshot, _ = invoke(tmp_path, option, "gone.txt")
    assert result.exit_code == 2
    assert f"{tmp_path / 'gone.txt'} is not a file" in result.output
    assert stage_snapshot.calls == []


def test_images_scan_end_to_end_into_the_report(tmp_path: Path):
    """The real scan_group, with crane and docker faked: each image's variant for
    the architecture is scanned with the given ignore file, and the report
    names the images by the names given."""
    mine = tmp_path / "mine.trivyignore.yaml"
    mine.write_text("vulnerabilities: []\n")
    docker = FakeDocker()
    digest = mock.Mock(side_effect=lambda ref, platform=None: digest_of(ref))
    staged = {}

    def stage(workdir, layout, dbs, image_types, ignore_files=None):
        staged.update(image_types=image_types, ignore_files=ignore_files)
        (workdir / "staging").mkdir()
        return workdir / "staging"

    args = [a for image in IMAGES for a in ("--image", image)]
    with faked_scan_tools(
        tmp_path, docker, digest, lambda ref: {"mediaType": OCI_MANIFEST}, stage
    ), mock.patch.object(
        local_scan_images.trivy_db, "stage_snapshot", FakeStageSnapshot()
    ):
        result = CliRunner().invoke(
            local_scan_images.main,
            [*args, "--ignorefile", str(mine), "--output-dir", str(tmp_path / "out")],
        )
    # The fixture has fixable CRITICAL/HIGH findings.
    assert result.exit_code == EXIT_FINDINGS, result.output
    assert {c.args for c in digest.call_args_list} == {
        (image, "linux/amd64") for image in IMAGES
    }
    assert staged == {"image_types": {"image"}, "ignore_files": {"image": mine}}
    runs = [c for c in docker.calls if c[:2] == ["docker", "run"]]
    assert len(runs) == 2
    for run in runs:
        assert "/data/ignore/image.trivyignore.yaml" in run

    out = tmp_path / "out"
    summary = json.loads((out / "summary.json").read_text())
    assert [i["name"] for i in summary["images"]] == list(IMAGES)
    assert all(i["scanned"] and i["python_version"] == "" for i in summary["images"])
    assert [i["digest"] for i in summary["images"]] == [
        digest_of(image) for image in IMAGES
    ]
    report = (out / "report.md").read_text()
    annotation = (out / "annotation.md").read_text()
    assert annotation.startswith("## Trivy image scan: CRITICAL/HIGH")
    for image in IMAGES:
        assert f"| `{image}` |" in report
    # A finding in both images reads "all 2 scanned images"; one in a single
    # image would name it.
    assert "all 2 scanned images" in report
    assert "py ·" not in report
    assert "Scanned each image for linux/amd64." in report
    for image in IMAGES:
        name = local_targets([image], "x86_64")[0].name
        assert (out / "results" / f"{name}.status.json").is_file()
        assert (out / "results" / f"{name}.json.gz").is_file()


def test_an_image_that_fails_is_reported_not_scanned(tmp_path: Path):
    def scan_group(targets, *a, **kw):
        for target in targets:
            target.error = "crane digest failed: MANIFEST_UNKNOWN"
            write_status(a[2], target)

    result, _, _ = invoke(tmp_path, scan_group=scan_group)
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    report = (tmp_path / "out" / "report.md").read_text()
    assert "- `ubuntu:22.04`: crane digest failed: MANIFEST_UNKNOWN" in report


def test_a_failed_ecr_login_only_warns(caplog):
    """One registry's failed login mustn't stop the other images."""
    ecr = "123.dkr.ecr.us-east-1.amazonaws.com"
    targets = local_targets([f"{ecr}/app:1", f"{ecr}/tool:2", "ubuntu:22.04"], "x86_64")
    login = mock.Mock(side_effect=subprocess.CalledProcessError(1, ["docker"]))
    with mock.patch.object(local_scan_images, "ecr_docker_login", login):
        local_scan_images.login(targets)
    login.assert_called_once_with(ecr)
    (warning,) = warnings(caplog)
    assert warning.startswith(f"Could not log in to {ecr}: CalledProcessError")


@pytest.mark.parametrize("name", ["mine.trivyignore", "mine.txt"])
def test_an_ignore_file_must_be_yaml(tmp_path: Path, name: str):
    """Trivy reads a plain ignore file staged as .yaml as YAML, and fails."""
    (tmp_path / name).write_text("CVE-2026-0001\n")
    result, stage_snapshot, _ = invoke(tmp_path, "--ignorefile", name)
    assert result.exit_code == 2
    assert "isn't a Trivy YAML ignore file (.yaml or .yml)" in result.output
    assert stage_snapshot.calls == []


LIBRARY_ONLY_RESULT = {
    "SchemaVersion": 2,
    "Results": [
        {
            "Target": "app",
            "Class": "lang-pkgs",
            "Type": "gobinary",
            "Packages": [{"Name": "stdlib"}],
            "Vulnerabilities": [
                {
                    "VulnerabilityID": "CVE-2026-0001",
                    "Severity": "HIGH",
                    "PkgName": "stdlib",
                    "InstalledVersion": "1.22.0",
                    "FixedVersion": "1.22.1",
                    "Status": "fixed",
                }
            ],
        }
    ],
}


def test_an_image_with_no_os_packages_keeps_its_other_findings(tmp_path: Path, caplog):
    """E.g. a FROM scratch image: its libraries' findings still count."""
    caplog.set_level(logging.INFO)
    scan_group = FakeScanGroup(result=LIBRARY_ONLY_RESULT)
    result, _, _ = invoke(tmp_path, scan_group=scan_group)
    assert result.exit_code == EXIT_FINDINGS, result.output
    report = (tmp_path / "out" / "report.md").read_text()
    assert "CVE-2026-0001" in report
    assert "### Incomplete" not in report
    assert (
        "`ubuntu:22.04`: no OS packages detected (OS: not detected); "
        "its other packages were checked."
    ) in report
    assert any("no OS packages detected" in w for w in warnings(caplog))


def test_a_kept_db_older_than_a_week_warns(tmp_path: Path, caplog):
    caplog.set_level(logging.INFO)
    created = (date.today() - timedelta(days=30)).isoformat()
    write_metadata(tmp_path / "cache", f"{created}T00:00:00Z")
    result, stage_snapshot, _ = invoke(tmp_path, "--work-dir", "cache")
    assert result.exit_code == EXIT_CLEAN, result.output
    assert stage_snapshot.calls == []
    (warning,) = warnings(caplog)
    assert (
        f"(DB created {created}, 30 days ago): CVEs published since then are missing"
    ) in warning
    report = (tmp_path / "out" / "report.md").read_text()
    assert "The reused DB was 30 days old" in report


def test_images_are_resolved_before_any_download(tmp_path: Path):
    """A mistyped image name mustn't cost a 1 GB DB download first."""

    def unknown(ref, platform=None):
        raise registry.RegistryError(f"crane digest {ref} failed: MANIFEST_UNKNOWN")

    result, stage_snapshot, scan_group = invoke(tmp_path, digest=unknown)
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    assert stage_snapshot.calls == []
    assert not scan_group.called
    report = (tmp_path / "out" / "report.md").read_text()
    assert "- `ubuntu:22.04`: crane digest ubuntu:22.04 failed: MANIFEST_UNKNOWN" in (
        report
    )
    assert (tmp_path / "out" / "results" / "ubuntu_22.04.status.json").is_file()


def test_a_failed_refresh_keeps_the_kept_dbs(tmp_path: Path):
    work = tmp_path / "cache"
    kept = write_metadata(work, "2026-01-01T00:00:00Z")
    (work / DB_FOLDER / "trivy-db.tar.gz").write_bytes(b"the kept DB")
    before = kept.read_text()
    failing = FakeStageSnapshot(error=registry.RegistryError("toomanyrequests"))
    result, _, _ = invoke(
        tmp_path, "--work-dir", "cache", "--refresh-db", stage_snapshot=failing
    )
    assert result.exit_code == 1
    assert kept.read_text() == before
    assert (work / DB_FOLDER / "trivy-db.tar.gz").read_bytes() == b"the kept DB"
    # The next run reuses them.
    result, stage_snapshot, _ = invoke(tmp_path, "--work-dir", "cache")
    assert result.exit_code == EXIT_CLEAN, result.output
    assert stage_snapshot.calls == []


def test_runs_share_a_work_dir_and_a_download_waits(tmp_path: Path, caplog):
    """Scans reading the kept DBs share the lock; a download waits for
    them."""
    caplog.set_level(logging.INFO)
    reading, also_reading = WorkDirLock(tmp_path), WorkDirLock(tmp_path)
    reading.shared()
    also_reading.shared()
    assert "Waiting" not in caplog.text
    downloading = WorkDirLock(tmp_path)
    got = threading.Event()
    thread = threading.Thread(target=lambda: (downloading.exclusive(), got.set()))
    thread.start()
    time.sleep(0.2)
    assert not got.is_set()
    reading.close()
    also_reading.close()
    thread.join(timeout=5)
    assert got.is_set()
    downloading.close()
    assert f"Waiting for another local_scan_images run using {tmp_path}" in caplog.text


def test_this_runs_earlier_results_are_cleared(tmp_path: Path):
    results = tmp_path / "out" / "results"
    results.mkdir(parents=True)
    (results / "ubuntu_22.04.json.gz").write_bytes(b"an earlier run's")
    (results / "notes.txt").write_text("not ours")
    failing = FakeScanGroup(error="docker run --rm failed: boom")
    result, _, _ = invoke(tmp_path, scan_group=failing)
    assert result.exit_code == EXIT_INCOMPLETE
    assert not (results / "ubuntu_22.04.json.gz").exists()
    assert (results / "notes.txt").read_text() == "not ours"


def test_a_group_failure_on_kept_dbs_suggests_refreshing(tmp_path: Path):
    write_metadata(tmp_path / "cache", f"{date.today().isoformat()}T00:00:00Z")
    failing = FakeScanGroup(error=f"{GROUP_FAILED}: copy of db/x.tar.gz failed")
    result, _, _ = invoke(tmp_path, "--work-dir", "cache", scan_group=failing)
    assert result.exit_code == EXIT_INCOMPLETE
    report = (tmp_path / "out" / "report.md").read_text()
    assert "while reusing the DBs in" in report
    assert "pass --refresh-db to download fresh ones" in report


def test_another_runs_containers_are_left_for_a_day(tmp_path: Path):
    """A local run may go on for hours."""
    result, _, scan_group = invoke(tmp_path)
    assert result.exit_code == EXIT_CLEAN, result.output
    assert scan_group.kw["stale_after"] == timedelta(days=1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
