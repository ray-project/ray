import hashlib
import io
import json
import sys
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Optional
from unittest import mock

import click
import pytest
from click.testing import CliRunner

from ci.ray_ci.automation import push_ray_image
from ci.ray_ci.automation.trivy import registry, scan_images
from ci.ray_ci.automation.trivy.fake_s3 import FakeS3
from ci.ray_ci.automation.trivy.scan_images import (
    ContainedTrivy,
    ScanError,
    build_targets,
    check_target,
    parse_image,
    scan_group,
)
from ci.ray_ci.automation.trivy.scan_store import BUCKET, ScanStore
from ci.ray_ci.automation.trivy.trivy_db import DB_SOURCES, TrivyDbError
from ci.ray_ci.automation.trivy.trivy_report import load_scans

FIXTURE = Path(__file__).parent / "testdata" / "ray-py3.12-cpu.json"
WORK_REPO = "123.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
COMMIT = "80ceee083e49b1ae709c149d3a15862fe12ce8cb"
BUILD_ENV = {
    "BUILDKITE_PIPELINE_SLUG": "postmerge",
    "BUILDKITE_BRANCH": "master",
    "BUILDKITE_BUILD_NUMBER": "20897",
    "BUILDKITE_COMMIT": COMMIT,
}


def store(s3: Optional[FakeS3] = None) -> ScanStore:
    return ScanStore.for_build("postmerge", "master", "20897", COMMIT, client=s3)


@pytest.fixture(autouse=True)
def no_real_processes(monkeypatch):
    """Fail any test that would run real crane or docker, or make an S3
    client, i.e. reach a registry, the docker host or S3 instead of the fakes."""

    def refuse(args, *a, **k):
        raise AssertionError(f"test tried to run {list(args)[:3]}")

    monkeypatch.setattr(scan_images.subprocess, "run", refuse)
    monkeypatch.setattr(scan_images.scan_store.boto3, "client", refuse)


def targets(*images: tuple, platform: str = "cpu", arch: str = "x86_64") -> list:
    return build_targets(
        list(images),
        platform,
        arch,
        WORK_REPO,
        "abc123",
        "releases/2.61.0",
        "80ceee083e49b1ae709c149d3a15862fe12ce8cb",
        "",
        "false",
    )


@pytest.mark.parametrize(
    ("spec", "expected"),
    [("ray:3.12", ("ray", "3.12")), ("ray-llm-extra:3.12", ("ray-llm-extra", "3.12"))],
)
def test_parse_image(spec: str, expected: tuple):
    assert parse_image(spec) == expected


@pytest.mark.parametrize("spec", ["ray", "ray:", ":3.12"])
def test_parse_image_rejects_bad_specs(spec: str):
    with pytest.raises(click.BadParameter):
        parse_image(spec)


def test_build_targets_uses_the_wanda_ref_and_published_tags():
    (target,) = targets(
        ("ray-extra", "3.12"), platform="cu12.8.1-cudnn", arch="aarch64"
    )
    assert target.name == "ray-extra-py3.12-cu12.8.1-cudnn-aarch64"
    assert target.ref == f"{WORK_REPO}:abc123-ray-extra-py3.12-cu12.8.1-cudnn-aarch64"
    assert target.repo == WORK_REPO
    record = target.status_record()
    assert record["published_tags"] == [
        "rayproject/ray:2.61.0.80ceee-extra-py312-cu128-aarch64"
    ]


def test_build_targets_rejects_unpublished_variants():
    with pytest.raises(click.BadParameter):
        targets(("ray-llm", "3.12"), platform="tpu")


@pytest.mark.parametrize(
    ("manifest_type", "arch", "expected_error"),
    [
        (OCI_MANIFEST, "amd64", ""),
        (OCI_INDEX, "amd64", "not a single-platform image manifest"),
        (OCI_MANIFEST, "arm64", "is arm64, expected amd64"),
    ],
)
def test_check_target(manifest_type: str, arch: str, expected_error: str):
    (target,) = targets(("ray", "3.12"))
    with mock.patch.multiple(
        scan_images.registry,
        manifest=mock.Mock(return_value={"mediaType": manifest_type}),
        config=mock.Mock(return_value={"architecture": arch}),
        digest=mock.Mock(return_value="sha256:d1"),
    ):
        check_target(target, "x86_64")
    assert expected_error in target.error
    assert target.digest == ("" if expected_error else "sha256:d1")


def test_docker_run_args_keep_trivy_contained():
    (target,) = targets(("ray", "3.12"))
    target.digest = "sha256:d1"
    trivy = ContainedTrivy("base@sha256:b")
    args = trivy.docker_run_args(target, "trivy-scan-x-scan-0")
    assert args == [
        "docker",
        "run",
        "--rm",
        "--name",
        "trivy-scan-x-scan-0",
        "--label",
        "ray-trivy-scan=1",
        "--network",
        "none",
        "--user",
        "65534:65534",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--workdir",
        "/work/tmp",
        "-e",
        "HOME=/work/home",
        "-e",
        "XDG_CACHE_HOME=/work/home/.cache",
        "-e",
        "XDG_DATA_HOME=/work/home/.local",
        "-e",
        "TMPDIR=/work/tmp",
        "-v",
        f"{trivy.data}:/data:ro",
        "-v",
        f"{trivy.work}:/work",
        "base@sha256:b",
        "/data/trivy",
        "image",
        "--config",
        "/data/trivy.yaml",
        "--input",
        "/data/layout@sha256:d1",
        "--cache-dir",
        "/work/cache",
        "--skip-db-update",
        "--skip-java-db-update",
        "--offline-scan",
        "--skip-version-check",
        "--disable-telemetry",
        "--scanners",
        "vuln",
        "--pkg-types",
        "os,library",
        "--list-all-pkgs",
        "--ignorefile",
        "/data/ignore/ray.trivyignore.yaml",
        "--show-suppressed",
        "--exit-code",
        "0",
        "--no-progress",
        "--quiet",
        "--timeout",
        "30m",
        "--format",
        "json",
        "--output",
        "/work/out/ray-py3.12-cpu.json",
    ]
    # Nothing from the job's environment or filesystem reaches the container.
    assert not any("AWS" in a or ".docker" in a or a.startswith("/home") for a in args)


class FakeDocker:
    """Stands in for _run: records docker commands, fakes Trivy's output."""

    def __init__(self, fail_scan_of: Optional[str] = None) -> None:
        self.calls: list[list[str]] = []
        self.fail_scan_of = fail_scan_of

    def __call__(self, args, check=True):
        args = list(args)
        self.calls.append(args)
        if args[:2] == ["docker", "run"] and self.fail_scan_of:
            if f"/work/out/{self.fail_scan_of}.json" in args:
                raise ScanError("trivy exited 1")
        if args[:2] == ["docker", "cp"] and ":/work/out/" in args[2]:
            Path(args[3]).write_bytes(FIXTURE.read_bytes())
        return ""

    def commands(self) -> list[str]:
        return [" ".join(c[:3]) for c in self.calls]


def fake_pull(refs, layout, platform):
    layout.mkdir()
    digests = [ref.rsplit("@", 1)[1] for ref in refs]
    (layout / "index.json").write_text(
        json.dumps({"manifests": [{"digest": d} for d in digests]})
    )


def run_group(
    tmp_path: Path, group: list, docker: FakeDocker, manifests: dict, report=None
):
    results = tmp_path / "results"
    results.mkdir()

    tags_by_digest = {}

    def digest(ref, platform=None):
        d = "sha256:" + hashlib.sha256(ref.encode()).hexdigest()
        tags_by_digest[d] = ref
        return d

    def manifest(ref):
        # Manifests are fetched by digest; `manifests` is keyed by tag.
        tag = tags_by_digest[ref.rsplit("@", 1)[1]]
        return {"mediaType": manifests.get(tag, OCI_MANIFEST)}

    def fake_stage(workdir, layout, dbs, image_types):
        (workdir / "staging").mkdir()
        return workdir / "staging"

    with mock.patch.multiple(
        scan_images.registry,
        manifest=manifest,
        config=mock.Mock(return_value={"architecture": "amd64"}),
        digest=digest,
        pull=fake_pull,
    ), mock.patch.object(scan_images, "_run", docker), mock.patch.object(
        scan_images, "load_db_pins", return_value={}
    ), mock.patch.object(
        scan_images, "fetch_db", side_effect=lambda s, *_: (tmp_path, {"blob": s.name})
    ), mock.patch.object(
        scan_images, "stage", fake_stage
    ):
        scan_group(
            group,
            "x86_64",
            store(),
            results,
            tmp_path,
            "base",
            report=report or (lambda target: None),
        )
    return results


def test_scan_group_writes_the_report_contract_and_isolates_bad_refs(tmp_path: Path):
    group = targets(("ray", "3.12"), ("ray-extra", "3.12"))
    docker = FakeDocker()
    index_ref = group[1].ref
    results = run_group(tmp_path, group, docker, {index_ref: OCI_INDEX})

    scans = {s.image.name: s for s in load_scans(results)}
    assert scans["ray-py3.12-cpu"].scanned
    assert scans["ray-py3.12-cpu"].findings
    assert not scans["ray-extra-py3.12-cpu"].scanned
    assert "single-platform" in scans["ray-extra-py3.12-cpu"].error
    status = json.loads((results / "ray-py3.12-cpu.status.json").read_text())
    assert status["trivy_db"] == {"db": {"blob": "db"}, "java_db": {"blob": "java_db"}}

    commands = docker.commands()
    creates = [
        c for c in commands if c.startswith(("docker volume create", "docker create"))
    ]
    assert creates == [
        "docker volume create",
        "docker volume create",
        "docker create --name",
    ]
    for call in docker.calls:
        if call[:3] == ["docker", "volume", "create"] or call[:2] == [
            "docker",
            "create",
        ]:
            assert "ray-trivy-scan=1" in call
    assert commands.count("docker run --rm") == 1
    # The containers and volumes are removed afterwards.
    assert commands[-2:] == ["docker rm -f", "docker volume rm"]


def test_one_failed_scan_does_not_stop_the_others(tmp_path: Path):
    group = targets(("ray", "3.11"), ("ray", "3.12"))
    docker = FakeDocker(fail_scan_of="ray-py3.11-cpu")
    results = run_group(tmp_path, group, docker, {})
    scans = {s.image.name: s for s in load_scans(results)}
    assert "trivy exited 1" in scans["ray-py3.11-cpu"].error
    assert scans["ray-py3.12-cpu"].scanned


def test_a_group_failure_marks_every_image(tmp_path: Path):
    group = targets(("ray", "3.11"), ("ray", "3.12"))

    def broken_pull(refs, layout, platform):
        raise registry.RegistryError("toomanyrequests")

    results = tmp_path / "results"
    results.mkdir()
    with mock.patch.multiple(
        scan_images.registry,
        manifest=mock.Mock(return_value={"mediaType": OCI_MANIFEST}),
        config=mock.Mock(return_value={"architecture": "amd64"}),
        digest=mock.Mock(return_value="sha256:d"),
        pull=broken_pull,
    ), mock.patch.object(
        scan_images, "load_db_pins", return_value={}
    ), mock.patch.object(
        scan_images, "fetch_db", return_value=(tmp_path, {})
    ):
        scan_group(group, "x86_64", store(), results, tmp_path, "base")
    for scan in load_scans(results):
        assert "group failed before scanning: toomanyrequests" in scan.error


def make_tar(path: Path, members: dict) -> None:
    with tarfile.open(path, "w:gz") as tar:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


@pytest.mark.parametrize(
    ("members", "error"),
    [
        ({"trivy.db": b"x", "metadata.json": b"{}"}, None),
        ({"../trivy.db": b"x", "metadata.json": b"{}"}, "unexpected entry"),
        ({"sub/trivy.db": b"x", "metadata.json": b"{}"}, "unexpected entry"),
        ({"metadata.json": b"{}"}, "lacks"),
    ],
    ids=["ok", "traversal", "nested", "missing"],
)
def test_db_extraction_only_takes_the_expected_files(
    tmp_path: Path, members: dict, error: Optional[str]
):
    tarball = tmp_path / "db.tar.gz"
    make_tar(tarball, members)
    dest = tmp_path / "db"
    if error:
        with pytest.raises(ScanError, match=error):
            scan_images._safe_extract(tarball, dest, {"trivy.db", "metadata.json"})
    else:
        scan_images._safe_extract(tarball, dest, {"trivy.db", "metadata.json"})
        assert sorted(p.name for p in dest.iterdir()) == ["metadata.json", "trivy.db"]


def test_stage_needs_an_ignore_file_for_every_image_type(tmp_path: Path):
    files = tmp_path / "runfiles"
    files.mkdir()
    (files / "trivy").write_bytes(b"\x7fELF")
    (files / "ray.trivyignore.yaml").write_text("vulnerabilities: []\n")
    rlocation = mock.Mock()
    rlocation.Rlocation = lambda p: str(files / Path(p).name)
    layout = tmp_path / "layout"
    layout.mkdir()
    dbs = {"db": tmp_path / "db", "java_db": tmp_path / "jdb"}
    for d in dbs.values():
        d.mkdir()
    with mock.patch.object(scan_images.runfiles, "Create", return_value=rlocation):
        with pytest.raises(ScanError, match="no ignore file for ray-extra"):
            scan_images.stage(tmp_path / "a", layout, dbs, {"ray", "ray-extra"})
        staging = scan_images.stage(tmp_path / "b", layout, dbs, {"ray"})
    assert (staging / "data" / "ignore" / "ray.trivyignore.yaml").is_file()
    assert (staging / "work" / "cache" / "db").readlink() == Path("/data/db")
    assert oct((staging / "work" / "out").stat().st_mode & 0o7777) == "0o1777"


def mode(path: Path) -> str:
    return oct(path.stat().st_mode & 0o7777)


def test_stage_makes_the_data_tree_readable_by_the_scan_user(tmp_path: Path):
    """crane writes blobs 0600 and docker cp makes root the owner, so the
    non-root scan user can only read what is world-readable."""
    files = tmp_path / "runfiles"
    files.mkdir()
    (files / "trivy").write_bytes(b"\x7fELF")
    (files / "trivy").chmod(0o500)
    (files / "ray.trivyignore.yaml").write_text("vulnerabilities: []\n")
    rlocation = mock.Mock()
    rlocation.Rlocation = lambda p: str(files / Path(p).name)
    layout = tmp_path / "layout"
    blob = layout / "blobs" / "sha256" / "abc"
    blob.parent.mkdir(parents=True)
    blob.write_bytes(b"layer")
    blob.chmod(0o600)
    blob.parent.chmod(0o700)
    db = tmp_path / "db"
    db.mkdir()
    (db / "trivy.db").write_bytes(b"db")
    (db / "trivy.db").chmod(0o600)
    with mock.patch.object(scan_images.runfiles, "Create", return_value=rlocation):
        staging = scan_images.stage(tmp_path / "w", layout, {"db": db}, {"ray"})
    data = staging / "data"
    assert mode(data / "trivy") == "0o755"
    assert mode(data / "layout" / "blobs" / "sha256" / "abc") == "0o644"
    assert mode(data / "layout" / "blobs" / "sha256") == "0o755"
    assert mode(data / "db" / "trivy.db") == "0o644"
    assert mode(data / "ignore" / "ray.trivyignore.yaml") == "0o644"


def invoke_main(results: Path, *images: str, s3: Optional[FakeS3] = None):
    args = ["--architecture", "x86_64", "--platform", "cpu"]
    for image in images:
        args += ["--image", image]
    args += ["--results-dir", str(results)]
    args += ["--rayci-work-repo", WORK_REPO, "--rayci-build-id", "abc123"]
    with mock.patch.object(
        scan_images.scan_store.boto3, "client", return_value=s3 or FakeS3()
    ):
        return CliRunner().invoke(scan_images.main, args, env=BUILD_ENV)


@pytest.mark.parametrize(("errors", "code"), [([], 0), (["boom"], 1)])
def test_main_exits_nonzero_when_any_image_was_not_scanned(
    tmp_path: Path, errors: list, code: int
):
    def fake_scan_group(group, *args, login, report):
        login()
        for target, error in zip(group, errors):
            target.error = error

    with mock.patch.object(
        scan_images, "scan_group", fake_scan_group
    ), mock.patch.object(scan_images.registry, "login_if_ecr") as login:
        result = invoke_main(tmp_path / "r", "ray:3.12")
    assert result.exit_code == code, result.output
    # Only the images need a login; the DBs come from S3.
    assert [c.args[0] for c in login.call_args_list] == [WORK_REPO]


RESULTS_PREFIX = "trivy-scans/postmerge/master/20897-80ceee083e49/results"


def scan_writing_results(*scanned: str, then=None):
    """A fake scan_group that writes what the real one would, so main has
    something to upload: a result for each scanned image, and a status for
    every image."""

    def fake(group, architecture, store, results_dir, *rest, login, report):
        for target in group:
            if target.name in scanned:
                (results_dir / f"{target.name}.json.gz").write_bytes(b"gz")
                target.scanned = True
            else:
                target.error = "boom"
            scan_images.write_status(results_dir, target)
            report(target)
        if then:
            then()

    return fake


def test_main_uploads_each_result_before_its_status(tmp_path: Path):
    s3 = FakeS3()
    uploaded = []
    upload = s3.upload_file

    def record(filename, bucket, key):
        uploaded.append(key.rsplit("/", 1)[1])
        upload(filename, bucket, key)

    s3.upload_file = record
    with mock.patch.object(
        scan_images, "scan_group", scan_writing_results("ray-py3.12-cpu")
    ):
        result = invoke_main(tmp_path / "r", "ray:3.11", "ray:3.12", s3=s3)
    assert result.exit_code == 1, result.output
    assert uploaded == [
        "ray-py3.11-cpu.status.json",
        "ray-py3.12-cpu.json.gz",
        "ray-py3.12-cpu.status.json",
    ]
    assert s3.get(BUCKET, f"{RESULTS_PREFIX}/ray-py3.12-cpu.json.gz") == b"gz"


def test_a_failed_upload_fails_the_job_and_keeps_the_status_back(
    tmp_path: Path, caplog
):
    """A status record in S3 must never point at a result that isn't there."""
    s3 = FakeS3()
    s3.denied.add(f"{RESULTS_PREFIX}/ray-py3.12-cpu.json.gz")
    with mock.patch.object(
        scan_images,
        "scan_group",
        scan_writing_results("ray-py3.11-cpu", "ray-py3.12-cpu"),
    ):
        result = invoke_main(tmp_path / "r", "ray:3.11", "ray:3.12", s3=s3)
    assert result.exit_code == 1, result.output
    assert "Upload failed: ray-py3.12-cpu: upload to s3://" in caplog.text
    assert "AccessDenied" in caplog.text
    keys = sorted(k for _, k in s3.objects)
    assert keys == [
        f"{RESULTS_PREFIX}/ray-py3.11-cpu.json.gz",
        f"{RESULTS_PREFIX}/ray-py3.11-cpu.status.json",
    ]


def scan_and_report(
    tmp_path: Path,
    group: list,
    s3: FakeS3,
    cancel_at: Optional[str] = None,
    manifests: Optional[dict] = None,
) -> list:
    """Run the real scan_group, uploading to ``s3`` as main does. Returns
    the order of scans and uploads."""
    events = []
    original = scan_images.ContainedTrivy.scan

    def scan(self, target, dest):
        events.append(("scan", target.name))
        if target.name == cancel_at:
            scan_images._exit_on_sigterm(15, None)
        original(self, target, dest)

    def report(target):
        events.append(("report", target.name))
        assert not scan_images.upload_result(store(s3), tmp_path / "results", target)

    with mock.patch.object(scan_images.ContainedTrivy, "scan", scan):
        run_group(tmp_path, group, FakeDocker(), manifests or {}, report)
    return events


def test_each_image_is_uploaded_as_soon_as_it_is_scanned(tmp_path: Path):
    """A job killed after its grace period then keeps what it scanned."""
    group = targets(("ray", "3.11"), ("ray", "3.12"))
    events = scan_and_report(tmp_path, group, FakeS3())
    assert events == [
        ("scan", "ray-py3.11-cpu"),
        ("report", "ray-py3.11-cpu"),
        ("scan", "ray-py3.12-cpu"),
        ("report", "ray-py3.12-cpu"),
    ]


def test_an_image_that_fails_its_check_is_reported_before_any_scan(tmp_path: Path):
    """Its status is final at once; it mustn't wait behind the group's
    scans, where a hard kill would lose it."""
    group = targets(("ray", "3.12"), ("ray-extra", "3.12"))
    events = scan_and_report(
        tmp_path, group, FakeS3(), manifests={group[1].ref: OCI_INDEX}
    )
    assert events == [
        ("report", "ray-extra-py3.12-cpu"),
        ("scan", "ray-py3.12-cpu"),
        ("report", "ray-py3.12-cpu"),
    ]


def test_one_status_that_fails_to_record_does_not_stop_the_others(
    tmp_path: Path, caplog
):
    reported = []

    def report(target):
        if target.name == "ray-py3.11-cpu":
            raise OSError("No space left on device")
        reported.append(target.name)

    def cancelled(self, target, dest):
        scan_images._exit_on_sigterm(15, None)

    group = targets(("ray", "3.11"), ("ray", "3.12"))
    with mock.patch.object(scan_images.ContainedTrivy, "scan", cancelled):
        with pytest.raises(SystemExit):
            run_group(tmp_path, group, FakeDocker(), {}, report)
    assert reported == ["ray-py3.12-cpu"]
    assert "ray-py3.11-cpu" in caplog.text
    assert "No space left on device" in caplog.text


def test_a_cancelled_job_keeps_what_it_scanned_and_reports_the_rest(tmp_path: Path):
    s3 = FakeS3()
    group = targets(("ray", "3.11"), ("ray", "3.12"), ("ray", "3.13"))
    with pytest.raises(SystemExit):
        scan_and_report(tmp_path, group, s3, cancel_at="ray-py3.12-cpu")

    def uploaded(name: str) -> dict:
        return json.loads(s3.get(BUCKET, f"{RESULTS_PREFIX}/{name}.status.json"))

    assert uploaded("ray-py3.11-cpu")["status"] == "scanned"
    assert (BUCKET, f"{RESULTS_PREFIX}/ray-py3.11-cpu.json.gz") in s3.objects
    for name in ("ray-py3.12-cpu", "ray-py3.13-cpu"):
        assert uploaded(name)["error"] == "scan did not complete"


def statuses(results: Path) -> dict:
    return {
        p.name.removesuffix(".status.json"): json.loads(p.read_text())
        for p in results.glob("*.status.json")
    }


def test_an_unexpected_group_error_is_recorded_for_every_image(tmp_path: Path):
    group = targets(("ray", "3.11"), ("ray", "3.12"))
    with mock.patch.multiple(
        scan_images.registry,
        manifest=mock.Mock(return_value={"mediaType": OCI_MANIFEST}),
        config=mock.Mock(return_value={"architecture": "amd64"}),
        digest=mock.Mock(return_value="sha256:d"),
    ), mock.patch.object(
        scan_images, "load_db_pins", return_value={}
    ), mock.patch.object(
        scan_images, "fetch_db", side_effect=TrivyDbError("expected one layer, found 0")
    ):
        scan_group(group, "x86_64", store(), tmp_path, tmp_path, "base")
    for record in statuses(tmp_path).values():
        assert record["status"] == "error"
        assert "TrivyDbError: expected one layer, found 0" in record["error"]


def test_an_unexpected_check_error_is_recorded(tmp_path: Path):
    group = targets(("ray", "3.12"))
    with mock.patch.multiple(
        scan_images.registry,
        digest=mock.Mock(return_value="sha256:d1"),
        manifest=mock.Mock(side_effect=json.JSONDecodeError("bad", "", 0)),
    ):
        scan_group(group, "x86_64", store(), tmp_path, tmp_path, "base")
    (record,) = statuses(tmp_path).values()
    assert record["status"] == "error"
    assert "JSONDecodeError" in record["error"]


def test_a_login_failure_is_recorded_for_every_image(tmp_path: Path):
    results = tmp_path / "r"
    with mock.patch.object(
        scan_images.registry, "login_if_ecr", side_effect=RuntimeError("ExpiredToken")
    ):
        result = invoke_main(results, "ray:3.11", "ray:3.12")
    assert result.exit_code == 1, result.output
    records = statuses(results)
    assert sorted(records) == ["ray-py3.11-cpu", "ray-py3.12-cpu"]
    for record in records.values():
        assert record["status"] == "error"
        assert "RuntimeError: ExpiredToken" in record["error"]


def test_an_image_without_a_result_is_never_reported_scanned(tmp_path: Path):
    group = targets(("ray", "3.12"))
    docker = FakeDocker()
    with mock.patch.object(scan_images.ContainedTrivy, "scan", lambda *a: None):
        results = run_group(tmp_path, group, docker, {})
    (record,) = statuses(results).values()
    assert record["status"] == "error"


def test_a_failed_setup_removes_what_it_created():
    docker = FakeDocker()
    original = docker.__call__

    def failing(args, check=True):
        if args[:2] == ["docker", "create"]:
            docker.calls.append(list(args))
            raise ScanError("docker create failed")
        return original(args, check)

    with mock.patch.object(scan_images, "_run", failing):
        with pytest.raises(ScanError):
            with ContainedTrivy("base"):
                pass
    removed = [c for c in docker.calls if c[:3] == ["docker", "volume", "rm"]]
    assert removed and "-f" in removed[0]


def test_a_cancelled_job_still_cleans_up_and_writes_statuses(tmp_path: Path):
    group = targets(("ray", "3.11"), ("ray", "3.12"))
    docker = FakeDocker()

    def cancelled(self, target, dest):
        scan_images._exit_on_sigterm(15, None)

    with mock.patch.object(scan_images.ContainedTrivy, "scan", cancelled):
        with pytest.raises(SystemExit) as exit_info:
            run_group(tmp_path, group, docker, {})
    assert exit_info.value.code == 143
    for record in statuses(tmp_path / "results").values():
        assert record["status"] == "error"
        assert record["error"] == "scan did not complete"
    assert docker.commands()[-2:] == ["docker rm -f", "docker volume rm"]


def test_cleanup_removes_named_scan_containers(tmp_path: Path):
    group = targets(("ray", "3.11"), ("ray", "3.12"))
    docker = FakeDocker()
    run_group(tmp_path, group, docker, {})
    runs = [c for c in docker.calls if c[:2] == ["docker", "run"]]
    names = [c[c.index("--name") + 1] for c in runs]
    assert len(set(names)) == 2
    (rm,) = [c for c in docker.calls if c[:3] == ["docker", "rm", "-f"]]
    assert set(names) <= set(rm)


def test_stale_leftovers_from_earlier_jobs_are_removed():
    now = datetime(2026, 11, 12, 12, 0, 0)
    listings = {
        ("docker", "ps"): "trivy-scan-old-loader\ntrivy-scan-new-loader\n",
        ("docker", "volume", "ls"): (
            "trivy-scan-old-data\ntrivy-scan-new-data\ntrivy-scan-new-local-data\n"
        ),
    }
    created = {
        "trivy-scan-old-loader": "2026-11-12T01:00:00.123456789Z",
        "trivy-scan-new-loader": "2026-11-12T11:30:00.5Z",
        "trivy-scan-old-data": "2026-11-11T23:00:00Z",
        "trivy-scan-new-data": "2026-11-12T11:30:00Z",
        # Volumes report local time with an offset: 03:30-08:00 is 11:30 UTC.
        "trivy-scan-new-local-data": "2026-11-12T03:30:00-08:00",
    }
    calls = []

    def run(args, check=True):
        args = list(args)
        calls.append(args)
        if args[:2] == ["docker", "ps"]:
            return listings[("docker", "ps")]
        if args[:3] == ["docker", "volume", "ls"]:
            return listings[("docker", "volume", "ls")]
        if "inspect" in args:
            return created[args[-1]] + "\n"
        return ""

    with mock.patch.object(scan_images, "_run", run):
        scan_images._prune_stale(now)
    removed = [c[-1] for c in calls if "rm" in c]
    assert removed == ["trivy-scan-old-loader", "trivy-scan-old-data"]
    assert all(
        "label=ray-trivy-scan=1" in " ".join(c) for c in calls if "ls" in c or "ps" in c
    )


def test_check_target_validates_the_digest_it_will_scan():
    """The tag may be re-pushed at any time; the checks must apply to the
    exact digest that gets pulled and scanned."""
    (target,) = targets(("ray", "3.12"))
    seen = []

    def manifest(ref):
        seen.append(("manifest", ref))
        return {"mediaType": OCI_MANIFEST}

    def config(ref):
        seen.append(("config", ref))
        return {"architecture": "amd64"}

    with mock.patch.multiple(
        scan_images.registry,
        manifest=manifest,
        config=config,
        digest=mock.Mock(return_value="sha256:d1"),
    ):
        check_target(target, "x86_64")
    assert target.digest == "sha256:d1"
    assert seen == [
        ("manifest", f"{target.repo}@sha256:d1"),
        ("config", f"{target.repo}@sha256:d1"),
    ]


def test_recorded_tags_do_not_depend_on_when_the_scan_runs():
    """Nightly tags embed the publish date, which a scan after midnight
    can't reproduce; only tags the scan can know for sure are recorded."""
    (target,) = build_targets(
        [("ray", "3.12")],
        "cpu",
        "x86_64",
        WORK_REPO,
        "abc123",
        "master",
        "80ceee083e49b1ae709c149d3a15862fe12ce8cb",
        "nightly",
        "false",
    )

    def tags_at(when: datetime) -> list:
        with mock.patch.object(push_ray_image, "datetime") as clock:
            clock.now.return_value = when
            return target.status_record()["published_tags"]

    before = tags_at(datetime(2026, 11, 12, 23, 50))
    after = tags_at(datetime(2026, 11, 13, 0, 10))
    assert before == after
    assert "rayproject/ray:nightly-py312-cpu" in before
    assert not any(".2611" in tag for tag in before)


def test_db_extraction_reads_the_tarball_once(tmp_path: Path):
    """The DBs are 0.14 and 1 GB compressed; random access into a gzip
    stream re-inflates it, so extraction must be a single streaming pass."""
    tarball = tmp_path / "db.tar.gz"
    make_tar(tarball, {"trivy.db": b"x", "metadata.json": b"{}"})
    modes = []
    real_open = tarfile.open

    def spy(name, mode="r", *args, **kwargs):
        modes.append(mode)
        return real_open(name, mode, *args, **kwargs)

    with mock.patch.object(scan_images.tarfile, "open", spy):
        scan_images._safe_extract(
            tarball, tmp_path / "db", {"trivy.db", "metadata.json"}
        )
    assert modes == ["r|gz"]


DB, JAVA_DB = DB_SOURCES
DB_TARBALL_MEMBERS = {"trivy.db": b"x", "metadata.json": b"{}"}


MANIFEST = "sha256:" + "a" * 64


def put_db(s3: FakeS3, tmp_path: Path) -> dict:
    """Upload a DB tarball as trivy_db would; returns the pins."""
    tarball = tmp_path / "upload.tar.gz"
    make_tar(tarball, DB_TARBALL_MEMBERS)
    data = tarball.read_bytes()
    s3.put(BUCKET, f"{store().prefix}/db/{DB.file_name}", data)
    blob = "sha256:" + hashlib.sha256(data).hexdigest()
    return {"db": {"source": DB.upstream, "manifest": MANIFEST, "blob": blob}}


UPSTREAM_CREATED = "2026-11-10T19:01:23Z"


def upstream(layer_digest: str):
    """public.ecr.aws's manifest for MANIFEST, with this DB layer."""
    manifest = {
        "annotations": {"org.opencontainers.image.created": UPSTREAM_CREATED},
        "layers": [{"mediaType": DB.media_type, "digest": layer_digest}],
    }
    return mock.patch.object(
        scan_images.registry, "manifest", mock.Mock(return_value=manifest)
    )


def test_fetch_db_extracts_the_pinned_tarball(tmp_path: Path):
    s3 = FakeS3()
    pins = put_db(s3, tmp_path)
    # The pin's own "source" and "created" are never trusted: what is
    # recorded comes from DB_SOURCES and the verified manifest.
    pins["db"]["source"] = "registry.example/not-trivy:1"
    pins["db"]["created"] = "2099-01-01T00:00:00Z"
    with upstream(pins["db"]["blob"]) as manifest:
        dest, pin = scan_images.fetch_db(DB, store(s3), pins, tmp_path)
    manifest.assert_called_once_with(f"{DB.upstream_repo}@{MANIFEST}")
    assert sorted(p.name for p in dest.iterdir()) == ["metadata.json", "trivy.db"]
    assert pin == {
        "source": DB.upstream,
        "manifest": MANIFEST,
        "blob": pins["db"]["blob"],
        "created": UPSTREAM_CREATED,
    }
    assert not (tmp_path / DB.file_name).exists()


@pytest.mark.parametrize(
    ("pins", "upstream_layer", "error"),
    [
        pytest.param({}, None, "no valid db pin", id="no-pin"),
        pytest.param(
            {"db": {"blob": "sha256:short", "manifest": MANIFEST}},
            None,
            "no valid db pin",
            id="bad-blob",
        ),
        pytest.param(
            {"db": {"blob": "sha256:" + "0" * 64, "manifest": "latest"}},
            None,
            "no valid db pin",
            id="bad-manifest",
        ),
        pytest.param(
            {"db": {"blob": "sha256:" + "0" * 64, "manifest": MANIFEST}},
            "sha256:" + "0" * 64,
            "has digest",
            id="tarball-differs",
        ),
        pytest.param(
            {"db": {"blob": "sha256:" + "0" * 64, "manifest": MANIFEST}},
            "sha256:" + "1" * 64,
            "but public.ecr.aws/aquasecurity/trivy-db@",
            id="pin-is-not-upstream",
        ),
    ],
)
def test_fetch_db_refuses_a_db_its_pin_does_not_vouch_for(
    tmp_path: Path, pins: dict, upstream_layer: Optional[str], error: str
):
    s3 = FakeS3()
    put_db(s3, tmp_path)
    with upstream(upstream_layer or "sha256:" + "f" * 64):
        with pytest.raises(ScanError, match=error):
            scan_images.fetch_db(DB, store(s3), pins, tmp_path)
    assert not (tmp_path / "db").exists()


def test_a_forged_pin_is_refused_before_the_download(tmp_path: Path):
    """Anyone who can write the bucket could replace both the tarball and
    its pin; only the upstream manifest vouches for the pin."""
    s3 = FakeS3()
    pins = put_db(s3, tmp_path)
    with upstream("sha256:" + "1" * 64):
        with pytest.raises(ScanError, match="but public.ecr.aws"):
            scan_images.fetch_db(DB, store(s3), pins, tmp_path)
    assert not (tmp_path / DB.file_name).exists()


def test_a_build_without_staged_dbs_records_why(tmp_path: Path):
    """trivy_db failed, so metadata.json was never uploaded."""
    group = targets(("ray", "3.12"))
    with mock.patch.multiple(
        scan_images.registry,
        manifest=mock.Mock(return_value={"mediaType": OCI_MANIFEST}),
        config=mock.Mock(return_value={"architecture": "amd64"}),
        digest=mock.Mock(return_value="sha256:d"),
    ):
        scan_group(group, "x86_64", store(FakeS3()), tmp_path, tmp_path, "base")
    (record,) = statuses(tmp_path).values()
    assert record["status"] == "error"
    assert record["error"].startswith("group failed before scanning: download of s3://")
    assert "/db/metadata.json failed" in record["error"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
