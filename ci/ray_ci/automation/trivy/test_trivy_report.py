import gzip
import json
import sys
from pathlib import Path
from typing import Optional

import pytest

from ci.ray_ci.automation.trivy.trivy_report import (
    Finding,
    load_scans,
    parse_trivy_report,
)

FIXTURES = Path(__file__).parent / "testdata"

# (name, image_type, python_version, platform, architecture, fixture)
CPU = ("ray-py3.12-cpu", "ray", "3.12", "cpu", "x86_64", "ray-py3.12-cpu.json")
CU128 = (
    "ray-py3.12-cu12.8.1-cudnn",
    "ray",
    "3.12",
    "cu12.8.1-cudnn",
    "x86_64",
    "ray-py3.12-cu12.8.1-cudnn.json",
)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
