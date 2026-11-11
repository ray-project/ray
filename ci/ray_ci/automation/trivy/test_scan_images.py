import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path
from typing import Optional
from unittest import mock

import click
import pytest
from click.testing import CliRunner

from ci.ray_ci.automation.trivy import registry, scan_images
from ci.ray_ci.automation.trivy.scan_images import (
    ContainedTrivy,
    ScanError,
    build_targets,
    check_target,
    parse_image,
    scan_group,
)
from ci.ray_ci.automation.trivy.trivy_report import load_scans

FIXTURE = Path(__file__).parent / "testdata" / "ray-py3.12-cpu.json"
WORK_REPO = "123.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"


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
    args = trivy.docker_run_args(target)
    assert args == [
        "docker",
        "run",
        "--rm",
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


def run_group(tmp_path: Path, group: list, docker: FakeDocker, manifests: dict):
    results = tmp_path / "results"
    results.mkdir()

    def manifest(ref):
        return {"mediaType": manifests.get(ref, OCI_MANIFEST)}

    def digest(ref, platform=None):
        return "sha256:" + hashlib.sha256(ref.encode()).hexdigest()

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
        scan_images, "fetch_db", side_effect=lambda s, *_: (tmp_path, {"blob": s.name})
    ), mock.patch.object(
        scan_images, "stage", fake_stage
    ):
        scan_group(group, "x86_64", WORK_REPO, "abc123", results, tmp_path, "base")
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
    assert commands[:3] == [
        "docker volume create",
        "docker volume create",
        "docker create --name",
    ]
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
    ), mock.patch.object(scan_images, "fetch_db", return_value=(tmp_path, {})):
        scan_group(group, "x86_64", WORK_REPO, "abc123", results, tmp_path, "base")
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


@pytest.mark.parametrize(("errors", "code"), [([], 0), (["boom"], 1)])
def test_main_exits_nonzero_when_any_image_was_not_scanned(
    tmp_path: Path, errors: list, code: int
):
    def fake_scan_group(group, *args):
        for target, error in zip(group, errors):
            target.error = error

    with mock.patch.object(
        scan_images, "scan_group", fake_scan_group
    ), mock.patch.object(scan_images.registry, "login_if_ecr") as login:
        result = CliRunner().invoke(
            scan_images.main,
            [
                "--architecture",
                "x86_64",
                "--platform",
                "cpu",
                "--image",
                "ray:3.12",
                "--results-dir",
                str(tmp_path / "r"),
                "--rayci-work-repo",
                WORK_REPO,
                "--rayci-build-id",
                "abc123",
                "--db-repo",
                "localhost:5000/citemp",
                "--branch",
                "master",
                "--commit",
                "80ceee083e49b1ae709c149d3a15862fe12ce8cb",
            ],
        )
    assert result.exit_code == code, result.output
    assert {c.args[0] for c in login.call_args_list} == {
        WORK_REPO,
        "localhost:5000/citemp",
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
