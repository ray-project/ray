import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from unittest import mock

import click
import pytest

from ci.ray_ci.automation import push_ray_image
from ci.ray_ci.automation.trivy import scan_images
from ci.ray_ci.automation.trivy.scan_images import (
    ContainedTrivy,
    ScanError,
    build_targets,
    check_target,
    parse_image,
)

FIXTURE = Path(__file__).parent / "testdata" / "ray-py3.12-cpu.json"
WORK_REPO = "123.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"


@pytest.fixture(autouse=True)
def no_real_processes(monkeypatch):
    """Fail any test that would run real crane or docker, i.e. reach a
    registry or the docker host instead of the fakes."""

    def refuse(args, *a, **k):
        raise AssertionError(f"test tried to run {list(args)[:3]}")

    monkeypatch.setattr(scan_images.subprocess, "run", refuse)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
