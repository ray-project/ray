import csv
import gzip
import json
import sys
from pathlib import Path
from typing import Optional

import pytest

from ci.ray_ci.automation.trivy import trivy_report
from ci.ray_ci.automation.trivy.trivy_report import (
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

FIXTURES = Path(__file__).parent / "testdata"

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

NOTE = "Files: in this test's folder."
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
    (results_dir / f"{name}.json.gz").write_bytes(gzip.compress(payload))


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


def test_load_scans_reads_gzipped_results(tmp_path: Path):
    write_scan(tmp_path, CPU)
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


def test_load_scans_ignores_an_uncompressed_result(tmp_path: Path):
    """Scan jobs only write <name>.json.gz."""
    write_scan(tmp_path, CPU)
    result = tmp_path / "ray-py3.12-cpu.json.gz"
    (tmp_path / "ray-py3.12-cpu.json").write_bytes(gzip.decompress(result.read_bytes()))
    result.unlink()
    (scan,) = load_scans(tmp_path)
    assert scan.error == "no result file"


def test_load_scans_flags_missing_result_file(tmp_path: Path):
    write_scan(tmp_path, CPU)
    (tmp_path / "ray-py3.12-cpu.json.gz").unlink()
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
        pytest.param("x.json.gz", gzip.compress(b'{"Results": ['), id="truncated-json"),
    ],
)
def test_load_scans_isolates_unreadable_results(
    tmp_path: Path, filename: str, payload: bytes
):
    write_scan(tmp_path, CPU)
    bad = ("x", "ray", "3.12", "tpu", "x86_64", "")
    write_scan(tmp_path, bad, report={})
    (tmp_path / "x.json.gz").unlink()
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
    report = trivy_report.render_markdown([scan], SCANNER, files_note=NOTE)
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

    assert exit_code(scans) == EXIT_FINDINGS
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
    ("scans", "issues", "expected"),
    [
        ([scan_with(make_vuln(FixedVersion="", Status="affected"))], [], EXIT_CLEAN),
        ([scan_with(make_vuln(Severity="MEDIUM"))], [], EXIT_CLEAN),
        ([scan_with(make_vuln())], [], EXIT_FINDINGS),
        ([scan_with(error="crane pull failed")], [], EXIT_INCOMPLETE),
        ([scan_with()], ["scanner metadata unreadable"], EXIT_INCOMPLETE),
        ([scan_with(make_vuln()), scan_with(error="boom")], [], EXIT_INCOMPLETE),
        ([], [], EXIT_INCOMPLETE),
    ],
)
def test_exit_code(scans: list, issues: list, expected: int):
    assert exit_code(scans, issues) == expected


def test_annotation_stays_under_limit_and_says_so():
    vulns = [
        make_vuln(VulnerabilityID=f"CVE-2026-{i:05d}", PkgName=f"pkg{i}")
        for i in range(3000)
    ]
    scans = [scan_with(*vulns)]
    full = render_annotation(scans, SCANNER, files_note=NOTE, limit=10**9)
    assert "truncated" not in full

    limit = 20_000
    body = render_annotation(scans, SCANNER, files_note=NOTE, limit=limit)
    assert len(body.encode()) <= limit
    assert "truncated" in body
    assert "3000 fixable" in body


def report_folder(tmp_path: Path, *specs: tuple, **kwargs) -> Path:
    results = tmp_path / "results"
    results.mkdir()
    for spec in specs:
        write_scan(results, spec, **kwargs)
    return results


def write_outputs(tmp_path: Path, results: Path, issues=(), env=None) -> int:
    return trivy_report.write_report(
        tmp_path / "out",
        load_scans(results),
        SCANNER,
        trivy_report.build_info(env or {}),
        issues=issues,
        files_note=NOTE,
    )


def test_write_report_end_to_end(tmp_path: Path):
    results = report_folder(tmp_path, CPU, CPU_ARM, CU128, LLM)
    broken = ("ray-py3.12-tpu", "ray", "3.12", "tpu", "x86_64", "")
    write_scan(results, broken, status="error", error="crane pull failed")
    env = {
        "BUILDKITE_PIPELINE_SLUG": "postmerge",
        "BUILDKITE_BUILD_NUMBER": "123",
        "BUILDKITE_COMMIT": "80ceee083e49b1ae709c149d3a15862fe12ce8cb",
        "BUILDKITE_BRANCH": "master",
    }

    assert write_outputs(tmp_path, results, env=env) == EXIT_INCOMPLETE
    out = tmp_path / "out"
    report = (out / "report.md").read_text()
    assert "4/5 images scanned" in report
    assert "`ray-py3.12-tpu`: crane pull failed" in report
    assert "CVE-2026-102268" in report and "| all 4 images |" in report
    assert "`linux-libc-dev`" in report
    assert "Trivy 0.76.1" in report and "DB 2026-11-10" in report
    assert report.endswith(NOTE + "\n")

    with open(out / "vulns.csv") as f:
        rows = list(csv.DictReader(f))
    assert rows and {r["severity"] for r in rows} <= {"CRITICAL", "HIGH"}
    assert {r["build_number"] for r in rows} == {"123"}

    summary = json.loads((out / "summary.json").read_text())
    assert summary["schema_version"] == 1
    assert summary["build"]["pipeline"] == "postmerge"
    assert summary["scanner"] == SCANNER
    assert "failed_steps" not in summary
    by_name = {i["name"]: i for i in summary["images"]}
    assert by_name["ray-py3.12-tpu"]["scanned"] is False
    assert by_name["ray-py3.12-cpu"]["digest"] == "sha256:ray-py3.12-cpu"
    assert "CVE-2026-102268|PyJWT|2.12.1" in by_name["ray-py3.12-cpu"]["findings"]
    assert summary["totals"]["images"] == 5
    assert summary["totals"]["scanned"] == 4


def test_write_report_for_a_clean_build(tmp_path: Path):
    results = report_folder(
        tmp_path, CPU, report=make_report(make_vuln(Severity="LOW"))
    )
    assert write_outputs(tmp_path, results) == EXIT_CLEAN
    assert "**0 fixable CRITICAL/HIGH**" in (tmp_path / "out" / "report.md").read_text()


def test_write_report_shows_healthy_images_next_to_a_corrupt_one(tmp_path: Path):
    results = report_folder(tmp_path, CPU)
    (results / "x.status.json").write_text('{"name": "x", "image_')
    assert write_outputs(tmp_path, results) == EXIT_INCOMPLETE
    report = (tmp_path / "out" / "report.md").read_text()
    assert "CVE-2026-102268" in report
    assert "`x`: malformed status record" in report


def test_write_report_without_any_status_record(tmp_path: Path):
    results = report_folder(tmp_path)
    assert write_outputs(tmp_path, results) == EXIT_INCOMPLETE
    assert "no scan status records" in (tmp_path / "out" / "report.md").read_text()


def test_the_footer_is_the_given_files_note():
    scans = [ImageScan(image=Image(*CPU[:5]), packages=1)]
    body = render_annotation(scans, SCANNER, files_note="Files: `s3://b/p/`")
    assert body.endswith("Files: `s3://b/p/`\n")
    assert "artifacts" not in body


def test_write_report_writes_the_three_files(tmp_path: Path):
    scans = [ImageScan(image=Image(*CPU[:5]), packages=1)]
    build = trivy_report.build_info({"BUILDKITE_BUILD_NUMBER": "7"})
    code = trivy_report.write_report(
        tmp_path / "out", scans, SCANNER, build, files_note=NOTE
    )
    assert code == EXIT_CLEAN
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == [
        "report.md",
        "summary.json",
        "vulns.csv",
    ]


@pytest.mark.parametrize(
    "report",
    [
        pytest.param([], id="not-an-object"),
        pytest.param({"Results": [{"Vulnerabilities": [{}]}]}, id="no-vuln-id"),
        pytest.param({"Results": ["debian"]}, id="result-not-an-object"),
    ],
)
def test_a_wrongly_shaped_result_fails_only_its_image(tmp_path: Path, report):
    """Results come from a shared bucket; one bad file must not take the
    whole report down."""
    write_scan(tmp_path, CPU, report=report)
    write_scan(tmp_path, CU128)
    scans = {s.image.name: s for s in load_scans(tmp_path)}
    assert scans[CPU[0]].error.startswith("unparseable result: ")
    assert scans[CU128[0]].scanned


def render_all(results: Path) -> str:
    scans = load_scans(results)
    return trivy_report.render_markdown(scans, SCANNER, files_note=NOTE)


@pytest.mark.parametrize(
    "field", ["PkgName", "InstalledVersion", "FixedVersion", "PkgPath", "Status"]
)
def test_a_null_text_field_reads_as_empty(tmp_path: Path, field: str):
    """Trivy omits empty fields; a null must not crash rendering or sorting
    later, which would cost every image's results."""
    write_scan(tmp_path, CPU, report=make_report(make_vuln(**{field: None})))
    write_scan(tmp_path, CU128, report=make_report(make_vuln(PkgName="other")))
    report = render_all(tmp_path)
    scans = {s.image.name: s for s in load_scans(tmp_path)}
    assert scans[CPU[0]].scanned and scans[CU128[0]].scanned
    assert "CVE-2026-0001" in report


@pytest.mark.parametrize(
    "vuln",
    [
        pytest.param(make_vuln(PkgName=7), id="number"),
        pytest.param(make_vuln(VulnerabilityID=123), id="numeric-id"),
        pytest.param(make_vuln(Severity=["HIGH"]), id="list"),
        pytest.param(make_vuln(VulnerabilityID=None), id="null-id"),
    ],
)
def test_a_non_text_field_fails_only_its_image(tmp_path: Path, vuln: dict):
    write_scan(tmp_path, CPU, report=make_report(vuln))
    write_scan(tmp_path, CU128, report=make_report(make_vuln(PkgName="other")))
    report = render_all(tmp_path)
    scans = {s.image.name: s for s in load_scans(tmp_path)}
    assert scans[CPU[0]].error.startswith("unparseable result: ")
    assert scans[CU128[0]].scanned
    assert "CVE-2026-0001" in report


# Exit codes rayci retries on its own (rayci raycicmd/command_step.go,
# defaultRayRetry, v0.51.0): a result reported with one of them reruns the
# report up to 3 more times.
RAYCI_RETRIED = {-1, 1, 3, 53, 125, 126, 127, 255}
# Bazel's own exit codes (bazel src/main/java/com/google/devtools/build/lib/
# util/ExitCode.java). The report runs under `bazel run`, so a result code
# among them could be read as Bazel's.
BAZEL_EXIT_CODES = {0, 1, 2, 3, 4, 6, 7, 8, 9, 32, 33, 34, 36, 37, 38, 39, 45, 48}


def test_result_exit_codes_are_not_retried_or_mistaken_for_bazels():
    assert EXIT_CLEAN == 0
    # The pipeline file and both module docstrings name these numbers.
    assert (EXIT_FINDINGS, EXIT_INCOMPLETE) == (10, 11)
    for code in (EXIT_FINDINGS, EXIT_INCOMPLETE):
        assert code not in RAYCI_RETRIED
        assert code not in BAZEL_EXIT_CODES


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
