import csv
import gzip
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional
from unittest import mock

import pytest
from click.testing import CliRunner

from ci.ray_ci.automation import trivy_report
from ci.ray_ci.automation.trivy_report import (
    EXIT_CLEAN,
    EXIT_FINDINGS,
    EXIT_INCOMPLETE,
    Finding,
    Image,
    ImageScan,
    describe_images,
    exit_code,
    group_findings,
    load_scans,
    parse_trivy_report,
    render_annotation,
)

FIXTURES = Path(__file__).parent / "testdata" / "trivy"

# (name, image_type, python_version, platform, architecture, fixture)
CPU = ("ray-py3.12-cpu", "ray", "3.12", "cpu", "x86_64", "ray-py3.12-cpu.json")
CPU_ARM = (
    "ray-py3.12-cpu-aarch64",
    "ray",
    "3.12",
    "cpu",
    "aarch64",
    "ray-py3.12-cpu-aarch64.json",
)
CU128 = (
    "ray-py3.12-cu12.8.1-cudnn",
    "ray",
    "3.12",
    "cu12.8.1-cudnn",
    "x86_64",
    "ray-py3.12-cu12.8.1-cudnn.json",
)
LLM = (
    "ray-llm-py3.12-cu13.0.0-cudnn",
    "ray-llm",
    "3.12",
    "cu13.0.0-cudnn",
    "x86_64",
    "ray-llm-py3.12-cu13.0.0-cudnn.json",
)

SCANNER = {
    "trivy": {"version": "0.76.1", "tarball_sha256": "c7cbe313"},
    "db": {
        "blob": "sha256:f8fa030ece295d1e057521fa1dd9621ee06e547c6453f962014329ba7fb2fdee",
        "updated_at": "2026-11-10T19:01:23Z",
    },
}


def write_scan(
    results_dir: Path,
    spec: tuple,
    *,
    status: str = "scanned",
    error: str = "",
    report: Optional[dict] = None,
    gz: bool = False,
) -> None:
    """Write a status record and (optionally) a result file, as a scan job does."""
    name, image_type, python_version, platform, architecture, fixture = spec
    record = {
        "name": name,
        "image_type": image_type,
        "python_version": python_version,
        "platform": platform,
        "architecture": architecture,
        "ref": f"citemp:build-{name}",
        "digest": f"sha256:{name}",
        "published_tags": [f"rayproject/{image_type}:nightly-{name}"],
        "status": status,
        "error": error,
    }
    (results_dir / f"{name}.status.json").write_text(json.dumps(record))
    if status != "scanned":
        return
    payload = (
        json.dumps(report).encode()
        if report is not None
        else (FIXTURES / fixture).read_bytes()
    )
    if gz:
        (results_dir / f"{name}.json.gz").write_bytes(gzip.compress(payload))
    else:
        (results_dir / f"{name}.json").write_bytes(payload)


def make_vuln(**overrides) -> dict:
    vuln = {
        "VulnerabilityID": "CVE-2026-0001",
        "PkgName": "pkg",
        "InstalledVersion": "1.0",
        "FixedVersion": "1.1",
        "Status": "fixed",
        "Severity": "HIGH",
        "PrimaryURL": "https://avd.aquasec.com/nvd/cve-2026-0001",
    }
    vuln.update(overrides)
    return vuln


def make_report(*vulns: dict, packages: int = 1) -> dict:
    return {
        "SchemaVersion": 2,
        "Metadata": {"OS": {"Family": "ubuntu", "Name": "22.04"}},
        "Results": [
            {
                "Target": "Python",
                "Class": "lang-pkgs",
                "Type": "python-pkg",
                "Packages": [{"Name": f"p{i}"} for i in range(packages)],
                "Vulnerabilities": list(vulns),
            }
        ],
    }


def make_image(
    image_type: str = "ray",
    python_version: str = "3.12",
    platform: str = "cpu",
    architecture: str = "x86_64",
) -> Image:
    name = f"{image_type}-py{python_version}-{platform}-{architecture}"
    return Image(
        name=name,
        image_type=image_type,
        python_version=python_version,
        platform=platform,
        architecture=architecture,
    )


@pytest.mark.parametrize(
    ("fixed", "status", "expected"),
    [
        ("1.1", "fixed", True),
        ("1.1", "", True),
        ("", "fixed", False),
        ("", "affected", False),
        ("1.1", "affected", False),
        ("1.1", "will_not_fix", False),
        ("1.1", "fix_deferred", False),
        ("1.1", "end_of_life", False),
    ],
)
def test_fixable(fixed: str, status: str, expected: bool):
    finding = parse_trivy_report(
        make_report(make_vuln(FixedVersion=fixed, Status=status))
    ).findings[0]
    assert finding.fixable is expected


SITE = "home/ray/anaconda3/lib/python3.12/site-packages"


@pytest.mark.parametrize(
    ("pkg_class", "pkg_type", "path", "expected"),
    [
        ("os-pkgs", "ubuntu", "", "os"),
        ("lang-pkgs", "python-pkg", f"{SITE}/PyJWT-2.12.1.dist-info/METADATA", SITE),
        # A jar or binary shipped inside a wheel is fixed by rebuilding it,
        # not by a pip upgrade, so it keeps its own path.
        (
            "lang-pkgs",
            "jar",
            f"{SITE}/ray/jars/ray_dist.jar",
            f"{SITE}/ray/jars/ray_dist.jar",
        ),
        ("lang-pkgs", "gobinary", f"{SITE}/tool/bin/tool", f"{SITE}/tool/bin/tool"),
        ("lang-pkgs", "gobinary", "usr/local/bin/azcopy", "usr/local/bin/azcopy"),
    ],
)
def test_location(pkg_class: str, pkg_type: str, path: str, expected: str):
    finding = Finding(
        vuln_id="CVE-1",
        severity="HIGH",
        pkg_name="p",
        installed="1",
        fixed="2",
        status="fixed",
        pkg_class=pkg_class,
        pkg_type=pkg_type,
        path=path,
        url="",
    )
    assert finding.location == expected


def test_parse_real_report_with_suppressed_finding():
    report = json.loads((FIXTURES / "ray-py3.12-cpu-suppressed.json").read_text())
    parsed = parse_trivy_report(report)

    assert parsed.os_name == "ubuntu 22.04"
    assert parsed.packages == 10
    suppressed = [f for f in parsed.findings if f.suppressed]
    assert [(f.vuln_id, f.pkg_name) for f in suppressed] == [
        ("CVE-2026-102268", "PyJWT")
    ]
    assert suppressed[0].statement == "test: accepted until fixed upstream"
    assert not any(
        f.vuln_id == "CVE-2026-102268" for f in parsed.findings if not f.suppressed
    )


@pytest.mark.parametrize("gz", [False, True])
def test_load_scans_reads_plain_and_gzipped_results(tmp_path: Path, gz: bool):
    write_scan(tmp_path, CPU, gz=gz)
    (scan,) = load_scans(tmp_path)
    assert scan.scanned
    assert scan.image.name == "ray-py3.12-cpu"
    assert scan.image.published_tags == ("rayproject/ray:nightly-ray-py3.12-cpu",)
    assert scan.findings


@pytest.mark.parametrize(
    ("status", "error", "report", "expected_error"),
    [
        ("error", "crane pull failed", None, "crane pull failed"),
        ("scanned", "", make_report(make_vuln(), packages=0), "no packages detected"),
    ],
)
def test_load_scans_marks_images_not_scanned(
    tmp_path: Path, status: str, error: str, report: Optional[dict], expected_error: str
):
    write_scan(tmp_path, CPU, status=status, error=error, report=report)
    (scan,) = load_scans(tmp_path)
    assert not scan.scanned
    assert expected_error in scan.error


def test_load_scans_flags_missing_result_file(tmp_path: Path):
    write_scan(tmp_path, CPU)
    (tmp_path / "ray-py3.12-cpu.json").unlink()
    (scan,) = load_scans(tmp_path)
    assert not scan.scanned
    assert "no result file" in scan.error


GZ_FIXTURE = gzip.compress((FIXTURES / "ray-py3.12-cpu.json").read_bytes())


@pytest.mark.parametrize(
    ("filename", "payload"),
    [
        pytest.param(
            "x.json.gz", GZ_FIXTURE[: len(GZ_FIXTURE) // 2], id="truncated-gzip"
        ),
        pytest.param(
            "x.json.gz",
            GZ_FIXTURE[:10] + b"\x00" * 200 + GZ_FIXTURE[-8:],
            id="corrupt-deflate",
        ),
        pytest.param("x.json.gz", b"not gzip at all", id="not-gzip"),
        pytest.param("x.json", b'{"Results": [', id="truncated-json"),
    ],
)
def test_load_scans_isolates_unreadable_results(
    tmp_path: Path, filename: str, payload: bytes
):
    write_scan(tmp_path, CPU)
    bad = ("x", "ray", "3.12", "tpu", "x86_64", "")
    write_scan(tmp_path, bad, report={})
    (tmp_path / "x.json").unlink()
    (tmp_path / filename).write_bytes(payload)

    scans = {s.image.name: s for s in load_scans(tmp_path)}
    assert scans["ray-py3.12-cpu"].scanned
    assert not scans["x"].scanned
    assert scans["x"].error.startswith("unreadable result")


@pytest.mark.parametrize(
    ("content", "expected_error"),
    [
        pytest.param('{"name": "x", "image_', "malformed status record", id="partial"),
        pytest.param(
            json.dumps({"name": "x", "image_type": "ray", "status": "scanned"}),
            "missing python_version",
            id="missing-keys",
        ),
        pytest.param("[1, 2]", "malformed status record", id="not-an-object"),
    ],
)
def test_load_scans_isolates_malformed_status_records(
    tmp_path: Path, content: str, expected_error: str
):
    write_scan(tmp_path, CPU)
    (tmp_path / "x.status.json").write_text(content)

    scans = {s.image.name: s for s in load_scans(tmp_path)}
    assert scans["ray-py3.12-cpu"].scanned
    assert not scans["x"].scanned
    assert expected_error in scans["x"].error


def test_load_scans_accepts_non_numeric_python_versions(tmp_path: Path):
    write_scan(tmp_path, CPU)
    write_scan(tmp_path, ("ray-py3.13t-cpu", "ray", "3.13t", "cpu", "x86_64", CPU[5]))
    scans = load_scans(tmp_path)
    assert [s.image.python_version for s in scans] == ["3.12", "3.13t"]
    assert all(s.scanned for s in scans)
    assert describe_images([s.image for s in scans], []) == (
        "ray py3.12, py3.13t · cpu · x86_64"
    )


def test_group_findings_dedupes_across_images_and_skips_noise(tmp_path: Path):
    for spec in (CPU, CPU_ARM, CU128, LLM):
        write_scan(tmp_path, spec)
    scans = load_scans(tmp_path)
    groups = {g.finding.key: g for g in group_findings(scans)}

    pyjwt = groups[("CVE-2026-102268", "PyJWT", "2.12.1")]
    assert len(pyjwt.images) == 4
    # MEDIUM/LOW findings (wget, rustls) never reach the report.
    assert not any(key[1] in ("wget", "rustls") for key in groups)
    # The kernel-header CVEs come only from the CUDA images, and have no fix.
    libc = [g for key, g in groups.items() if key[1] == "linux-libc-dev"]
    assert libc and all(not g.fixable for g in libc)
    assert all(
        {i.name for i in g.images}
        == {"ray-py3.12-cu12.8.1-cudnn", "ray-llm-py3.12-cu13.0.0-cudnn"}
        for g in libc
    )


def two_path_scan() -> ImageScan:
    dup = make_report(
        make_vuln(PkgPath="b/site-packages/x"),
        make_vuln(PkgPath="a/site-packages/x"),
        make_vuln(PkgPath="a/site-packages/x"),
    )
    parsed = parse_trivy_report(dup)
    return ImageScan(image=make_image(), findings=parsed.findings, packages=1)


def test_group_findings_counts_an_image_once_but_keeps_every_location():
    scan = two_path_scan()
    (group,) = group_findings([scan])
    assert group.images == [scan.image]
    assert group.locations == ["a/site-packages", "b/site-packages"]


def test_report_and_csv_list_every_install_location(tmp_path: Path):
    scan = two_path_scan()
    report = trivy_report.render_markdown([scan], [], SCANNER)
    assert "`a/site-packages`, `b/site-packages`" in report

    trivy_report.write_csv(tmp_path / "v.csv", [scan], trivy_report.build_info({}))
    with open(tmp_path / "v.csv") as f:
        paths = [row["path"] for row in csv.DictReader(f)]
    assert paths == ["a/site-packages/x", "b/site-packages/x"]


def scan_of(image_type: str, vuln: dict) -> ImageScan:
    parsed = parse_trivy_report(make_report(vuln))
    return ImageScan(
        image=make_image(image_type=image_type), findings=parsed.findings, packages=1
    )


@pytest.mark.parametrize("fixed_first", [True, False])
def test_fixability_is_decided_per_image_not_by_image_order(fixed_first: bool):
    """Same CVE, package and version: fixed in one image, will_not_fix in another."""
    fixed = make_vuln(FixedVersion="3.0.3", Status="fixed")
    wont = make_vuln(FixedVersion="", Status="will_not_fix")
    # Scans arrive in image sort order, so "ray" is always grouped first.
    first, second = (fixed, wont) if fixed_first else (wont, fixed)
    scans = [scan_of("ray", first), scan_of("zzz", second)]
    fixed_type = "ray" if fixed_first else "zzz"
    wont_type = "zzz" if fixed_first else "ray"

    assert exit_code(scans, []) == EXIT_FINDINGS
    groups = group_findings(scans)
    fixable = [g for g in groups if g.fixable]
    no_fix = [g for g in groups if not g.fixable]
    assert [i.image_type for g in fixable for i in g.images] == [fixed_type]
    assert [i.image_type for g in no_fix for i in g.images] == [wont_type]
    assert fixable[0].finding.fixed == "3.0.3"


def test_group_findings_suppressed_are_separate(tmp_path: Path):
    spec = CPU[:5] + ("ray-py3.12-cpu-suppressed.json",)
    write_scan(tmp_path, spec)
    scans = load_scans(tmp_path)
    assert not any(
        g.finding.vuln_id == "CVE-2026-102268" for g in group_findings(scans)
    )
    (suppressed,) = group_findings(scans, suppressed=True)
    assert suppressed.finding.vuln_id == "CVE-2026-102268"


ALL_PY = ["3.11", "3.12", "3.13", "3.14"]


@pytest.mark.parametrize(
    ("images", "everything", "expected"),
    [
        pytest.param(
            [make_image(python_version=v) for v in ALL_PY],
            [make_image(python_version=v) for v in ALL_PY],
            "all 4 images",
            id="every-scanned-image",
        ),
        pytest.param(
            [make_image(python_version=v, platform="cu12.8.1-cudnn") for v in ALL_PY],
            [],
            "ray py3.11–3.14 · cu12.8 · x86_64",
            id="python-range",
        ),
        pytest.param(
            [
                make_image(image_type=t, architecture=a)
                for t in ("ray", "ray-extra")
                for a in ("x86_64", "aarch64")
            ],
            [],
            "ray, ray-extra py3.12 · cpu · aarch64, x86_64",
            id="types-share-variants",
        ),
        pytest.param(
            [make_image(python_version="3.11"), make_image(python_version="3.13")],
            [],
            "ray py3.11, py3.13 · cpu · x86_64",
            id="python-gap",
        ),
        pytest.param(
            [
                make_image(python_version="3.11"),
                make_image(python_version="3.12", platform="cu12.8.1-cudnn"),
            ],
            [],
            "ray py3.11 · cpu · x86_64; ray py3.12 · cu12.8 · x86_64",
            id="not-a-cross-product",
        ),
        pytest.param(
            [make_image(platform="tpu")],
            [make_image(platform="tpu")],
            "ray py3.12 · tpu · x86_64",
            id="single-image-is-named",
        ),
        pytest.param(
            [
                make_image(python_version="3.11", platform="cu12.8.1-cudnn"),
                make_image(python_version="3.12", platform="cu12.8.1"),
                make_image(python_version="3.13", platform="cu12.8.1-cudnn"),
            ],
            [],
            "ray py3.12 · cu12.8.1 · x86_64; ray py3.11, py3.13 · cu12.8.1-cudnn · x86_64",
            id="same-cuda-minor-stays-distinct",
        ),
        pytest.param(
            [make_image(platform=p) for p in ("cu12.8.1-cudnn", "cu12.8.1")],
            [],
            "ray py3.12 · cu12.8.1, cu12.8.1-cudnn · x86_64",
            id="colliding-labels-use-full-names",
        ),
    ],
)
def test_describe_images(images: list, everything: list, expected: str):
    assert describe_images(images, everything) == expected


def scan_with(*vulns: dict, error: str = "") -> ImageScan:
    findings = parse_trivy_report(make_report(*vulns)).findings
    return ImageScan(image=make_image(), findings=findings, packages=1, error=error)


@pytest.mark.parametrize(
    ("scans", "failed_steps", "expected"),
    [
        ([scan_with(make_vuln(FixedVersion="", Status="affected"))], [], EXIT_CLEAN),
        ([scan_with(make_vuln(Severity="MEDIUM"))], [], EXIT_CLEAN),
        ([scan_with(make_vuln())], [], EXIT_FINDINGS),
        ([scan_with(error="crane pull failed")], [], EXIT_INCOMPLETE),
        ([scan_with()], ["images_scan--platformcpu"], EXIT_INCOMPLETE),
        ([scan_with(make_vuln()), scan_with(error="boom")], [], EXIT_INCOMPLETE),
    ],
)
def test_exit_code(scans: list, failed_steps: list, expected: int):
    assert exit_code(scans, failed_steps) == expected


def test_annotation_stays_under_limit_and_says_so():
    vulns = [
        make_vuln(VulnerabilityID=f"CVE-2026-{i:05d}", PkgName=f"pkg{i}")
        for i in range(3000)
    ]
    scans = [scan_with(*vulns)]
    full = render_annotation(scans, [], SCANNER, limit=10**9)
    assert "truncated" not in full

    limit = 20_000
    body = render_annotation(scans, [], SCANNER, limit=limit)
    assert len(body.encode()) <= limit
    assert "truncated" in body
    assert "3000 fixable" in body


def test_annotate_posts_to_buildkite():
    with mock.patch.object(trivy_report.subprocess, "run") as run:
        trivy_report.annotate("body", "error")
    run.assert_called_once_with(
        [
            "buildkite-agent",
            "annotate",
            "--context",
            "trivy-scan",
            "--style",
            "error",
        ],
        input=b"body",
        check=True,
    )


def run_cli(tmp_path: Path, *args: str, env: Optional[dict] = None):
    results = tmp_path / "results"
    out = tmp_path / "out"
    metadata = tmp_path / "metadata.json"
    metadata.write_text(json.dumps(SCANNER))
    runner = CliRunner()
    return runner.invoke(
        trivy_report.main,
        [str(results), "--output-dir", str(out), "--metadata", str(metadata), *args],
        env=env or {},
    )


def test_cli_end_to_end(tmp_path: Path):
    results = tmp_path / "results"
    results.mkdir()
    for spec in (CPU, CPU_ARM, CU128, LLM):
        write_scan(results, spec, gz=spec is LLM)
    broken = ("ray-py3.12-tpu", "ray", "3.12", "tpu", "x86_64", "")
    write_scan(results, broken, status="error", error="crane pull failed")

    env = {
        "BUILDKITE_PIPELINE_SLUG": "postmerge",
        "BUILDKITE_BUILD_NUMBER": "123",
        "BUILDKITE_COMMIT": "80ceee083e49b1ae709c149d3a15862fe12ce8cb",
        "BUILDKITE_BRANCH": "master",
    }
    result = run_cli(tmp_path, "--failed-step", "images_scan--platformtpu", env=env)

    assert result.exit_code == EXIT_INCOMPLETE, result.output
    out = tmp_path / "out"
    report = (out / "report.md").read_text()
    assert "4/5 images scanned" in report
    assert "`ray-py3.12-tpu`: crane pull failed" in report
    assert "`images_scan--platformtpu`" in report
    assert "CVE-2026-102268" in report and "| all 4 images |" in report
    assert "`linux-libc-dev`" in report
    assert "Trivy 0.76.1" in report and "DB 2026-11-10" in report

    with open(out / "vulns.csv") as f:
        rows = list(csv.DictReader(f))
    assert rows and {r["severity"] for r in rows} <= {"CRITICAL", "HIGH"}
    assert {r["build_number"] for r in rows} == {"123"}

    summary = json.loads((out / "summary.json").read_text())
    assert summary["schema_version"] == 1
    assert summary["build"]["pipeline"] == "postmerge"
    assert summary["scanner"] == SCANNER
    assert summary["failed_steps"] == ["images_scan--platformtpu"]
    by_name = {i["name"]: i for i in summary["images"]}
    assert by_name["ray-py3.12-tpu"]["scanned"] is False
    assert by_name["ray-py3.12-cpu"]["digest"] == "sha256:ray-py3.12-cpu"
    assert "CVE-2026-102268|PyJWT|2.12.1" in by_name["ray-py3.12-cpu"]["findings"]
    assert summary["totals"]["images"] == 5
    assert summary["totals"]["scanned"] == 4


def test_cli_clean_build_exits_zero(tmp_path: Path):
    results = tmp_path / "results"
    results.mkdir()
    write_scan(results, CPU, report=make_report(make_vuln(Severity="LOW")))
    result = run_cli(tmp_path)
    assert result.exit_code == EXIT_CLEAN, result.output
    assert "**0 fixable CRITICAL/HIGH**" in (tmp_path / "out" / "report.md").read_text()


def test_cli_annotates_when_asked(tmp_path: Path):
    results = tmp_path / "results"
    results.mkdir()
    write_scan(results, CPU)
    with mock.patch.object(trivy_report, "annotate") as annotate:
        result = run_cli(tmp_path, "--annotate")
    assert result.exit_code == EXIT_FINDINGS, result.output
    body, style = annotate.call_args.args
    assert style == "error"
    assert "fixable CRITICAL/HIGH" in body


@pytest.mark.parametrize("content", [None, "{not json"], ids=["missing", "invalid"])
def test_cli_flags_unreadable_metadata(tmp_path: Path, content: Optional[str]):
    results = tmp_path / "results"
    results.mkdir()
    write_scan(results, CPU, report=make_report(make_vuln(Severity="LOW")))
    metadata = tmp_path / "metadata.json"
    if content is not None:
        metadata.write_text(content)

    result = CliRunner().invoke(
        trivy_report.main,
        [
            str(results),
            "--output-dir",
            str(tmp_path / "out"),
            "--metadata",
            str(metadata),
        ],
    )

    # The scan itself is clean, but its results can't be tied to a DB.
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    assert "scanner metadata" in (tmp_path / "out" / "report.md").read_text()
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["issues"] and "scanner metadata" in summary["issues"][0]


def test_cli_without_metadata_flag_has_no_issue(tmp_path: Path):
    results = tmp_path / "results"
    results.mkdir()
    write_scan(results, CPU, report=make_report(make_vuln(Severity="LOW")))
    result = CliRunner().invoke(
        trivy_report.main, [str(results), "--output-dir", str(tmp_path / "out")]
    )
    assert result.exit_code == EXIT_CLEAN, result.output


@pytest.mark.parametrize(
    "error",
    [
        subprocess.CalledProcessError(1, "buildkite-agent"),
        FileNotFoundError("buildkite-agent"),
    ],
    ids=["agent-error", "agent-missing"],
)
def test_cli_keeps_exit_code_when_annotation_fails(tmp_path: Path, error: Exception):
    results = tmp_path / "results"
    results.mkdir()
    write_scan(results, CPU, report=make_report(make_vuln(Severity="LOW")))
    with mock.patch.object(trivy_report.subprocess, "run", side_effect=error):
        result = run_cli(tmp_path, "--annotate")
    assert result.exit_code == EXIT_CLEAN, result.output
    assert "could not post the annotation" in result.output
    assert (tmp_path / "out" / "summary.json").is_file()


def test_cli_reports_healthy_images_next_to_a_corrupt_one(tmp_path: Path):
    results = tmp_path / "results"
    results.mkdir()
    write_scan(results, CPU)
    (results / "x.status.json").write_text('{"name": "x", "image_')
    result = run_cli(tmp_path)
    assert result.exit_code == EXIT_INCOMPLETE, result.output
    report = (tmp_path / "out" / "report.md").read_text()
    assert "CVE-2026-102268" in report
    assert "`x`: malformed status record" in report


def test_cli_rejects_empty_results_dir(tmp_path: Path):
    (tmp_path / "results").mkdir()
    result = run_cli(tmp_path)
    assert result.exit_code == EXIT_INCOMPLETE
    assert "no scan status records" in (tmp_path / "out" / "report.md").read_text()


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
