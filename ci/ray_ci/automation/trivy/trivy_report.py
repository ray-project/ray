"""Read and judge the results of a build's Trivy image scans.

Each scan job writes, per image, a ``<name>.status.json`` record and, when the
scan succeeded, Trivy's JSON result as ``<name>.json.gz``. ``load_scans``
reads them all; ``group_findings`` merges each CRITICAL/HIGH finding across
the images it appears in; ``build_summary`` and ``write_csv`` produce
``summary.json`` and ``vulns.csv``.

Only fixable CRITICAL/HIGH findings count against the build. ``exit_code``
gives 0 clean, 10 fixable CRITICAL/HIGH findings, 11 incomplete coverage
(takes precedence over 10).
"""

import csv
import gzip
import json
import re
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

GATED_SEVERITIES = ("CRITICAL", "HIGH")
# Trivy statuses meaning no usable fix exists, even if FixedVersion is set.
NO_FIX_STATUSES = frozenset({"affected", "will_not_fix", "fix_deferred", "end_of_life"})

# Results, not failures, so neither may be a code rayci retries on its own
# (it reruns 3, for example, up to 3 times) or one Bazel uses under `bazel run`.
EXIT_CLEAN = 0
EXIT_FINDINGS = 10
EXIT_INCOMPLETE = 11
SUMMARY_SCHEMA_VERSION = 1

CSV_COLUMNS = (
    "pipeline",
    "build_number",
    "commit",
    "image",
    "image_type",
    "python_version",
    "platform",
    "architecture",
    "digest",
    "vuln_id",
    "severity",
    "fixable",
    "status",
    "suppressed",
    "pkg_class",
    "pkg_type",
    "pkg_name",
    "installed",
    "fixed",
    "location",
    "path",
)


@dataclass(frozen=True)
class Image:
    """One scanned image variant, as recorded by its scan job."""

    name: str
    image_type: str
    python_version: str
    platform: str
    architecture: str
    ref: str = ""
    digest: str = ""
    published_tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Finding:
    vuln_id: str
    severity: str
    pkg_name: str
    installed: str
    fixed: str
    status: str
    pkg_class: str
    pkg_type: str
    path: str
    url: str
    suppressed: bool = False
    statement: str = ""

    @property
    def gated(self) -> bool:
        return self.severity in GATED_SEVERITIES

    @property
    def fixable(self) -> bool:
        return bool(self.fixed) and self.status not in NO_FIX_STATUSES

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.vuln_id, self.pkg_name, self.installed)

    @property
    def location(self) -> str:
        """Where the fix goes: "os" for distro packages, the environment for
        Python packages, else the jar or binary itself (even one shipped
        inside a wheel, which is fixed by rebuilding it, not by pip)."""
        if self.pkg_class == "os-pkgs":
            return "os"
        if self.pkg_type == "python-pkg":
            match = re.match(r"(.*?/(?:site|dist)-packages)/", self.path)
            if match:
                return match.group(1)
        return self.path


@dataclass
class ParsedReport:
    findings: list[Finding]
    packages: int
    os_name: str


@dataclass
class ImageScan:
    image: Image
    findings: list[Finding] = field(default_factory=list)
    packages: int = 0
    os_name: str = ""
    error: str = ""

    @property
    def scanned(self) -> bool:
        return not self.error

    def gated(
        self, suppressed: bool = False
    ) -> dict[tuple[str, str, str], list[Finding]]:
        """CRITICAL/HIGH findings by key, with one finding per install path.

        A package installed in two paths counts once per image, but both
        paths are kept so the report can show every place to fix.
        """
        by_key: dict[tuple[str, str, str], list[Finding]] = {}
        for finding in self.findings:
            if finding.gated and finding.suppressed == suppressed:
                same_key = by_key.setdefault(finding.key, [])
                if all(f.path != finding.path for f in same_key):
                    same_key.append(finding)
        return by_key


def _any_fixable(findings: Iterable[Finding]) -> bool:
    return any(f.fixable for f in findings)


@dataclass
class Group:
    """One finding deduplicated across the images and paths it appears in.

    Fixability is part of the group's identity: if the same CVE has a fix in
    one image but not in another, each image lands in its own group.
    """

    fixable: bool
    findings: list[Finding]
    images: list[Image]

    @property
    def finding(self) -> Finding:
        """A representative finding, one with a usable fix if the group has one."""
        if self.fixable:
            return next(f for f in self.findings if f.fixable)
        return self.findings[0]

    @property
    def locations(self) -> list[str]:
        return sorted({f.location for f in self.findings})


def _text(record: Mapping[str, Any], key: str, required: bool = False) -> str:
    """A text field. Trivy omits empty ones, so missing or null reads as
    empty; any other type raises, which fails only this image's result."""
    value = record.get(key)
    if value is None:
        if required:
            raise ValueError(f"{key} is missing")
        return ""
    if not isinstance(value, str):
        raise TypeError(f"{key} is a {type(value).__name__}, not a string")
    return value


def _finding(
    vuln: Mapping[str, Any],
    result: Mapping[str, Any],
    suppressed: bool = False,
    statement: str = "",
) -> Finding:
    return Finding(
        vuln_id=_text(vuln, "VulnerabilityID", required=True),
        severity=_text(vuln, "Severity") or "UNKNOWN",
        pkg_name=_text(vuln, "PkgName"),
        installed=_text(vuln, "InstalledVersion"),
        fixed=_text(vuln, "FixedVersion"),
        status=_text(vuln, "Status"),
        pkg_class=_text(result, "Class"),
        pkg_type=_text(result, "Type"),
        path=_text(vuln, "PkgPath") or _text(result, "Target"),
        url=_text(vuln, "PrimaryURL"),
        suppressed=suppressed,
        statement=statement,
    )


def parse_trivy_report(report: Mapping[str, Any]) -> ParsedReport:
    """Parse a Trivy ``--format json`` image report (schema version 2)."""
    findings: list[Finding] = []
    packages = 0
    for result in report.get("Results") or []:
        packages += len(result.get("Packages") or [])
        for vuln in result.get("Vulnerabilities") or []:
            findings.append(_finding(vuln, result))
        # --show-suppressed moves ignored findings here.
        for modified in result.get("ExperimentalModifiedFindings") or []:
            if modified.get("Type") == "vulnerability":
                findings.append(
                    _finding(
                        modified["Finding"],
                        result,
                        suppressed=True,
                        statement=_text(modified, "Statement"),
                    )
                )
    os_meta = (report.get("Metadata") or {}).get("OS") or {}
    os_name = " ".join(filter(None, (_text(os_meta, "Family"), _text(os_meta, "Name"))))
    return ParsedReport(findings=findings, packages=packages, os_name=os_name)


def _read_result(results_dir: Path, name: str) -> Optional[Mapping[str, Any]]:
    path = results_dir / f"{name}.json.gz"
    if not path.is_file():
        return None
    return json.loads(gzip.decompress(path.read_bytes()))


def _load_scan(results_dir: Path, image: Image, record: Mapping[str, Any]) -> ImageScan:
    status = record.get("status", "")
    if status != "scanned":
        return ImageScan(image=image, error=record.get("error") or f"status {status!r}")
    try:
        report = _read_result(results_dir, image.name)
    except (OSError, EOFError, ValueError, zlib.error) as e:
        # Truncated gzip raises EOFError and corrupt deflate zlib.error; one
        # bad upload must not take the other images' results down with it.
        return ImageScan(image=image, error=f"unreadable result: {e!r}")
    if report is None:
        return ImageScan(image=image, error="no result file")
    try:
        parsed = parse_trivy_report(report)
    except Exception as e:
        # Valid JSON in the wrong shape.
        return ImageScan(
            image=image, error=f"unparseable result: {type(e).__name__}: {e}"
        )
    # A scan that saw nothing must not look clean.
    error = "" if parsed.packages else "no packages detected"
    return ImageScan(
        image=image,
        findings=parsed.findings,
        packages=parsed.packages,
        os_name=parsed.os_name,
        error=error,
    )


_IMAGE_FIELDS = ("image_type", "python_version", "platform", "architecture")
_STATUS_SUFFIX = ".status.json"


def _load_status(status_path: Path) -> tuple[Image, Mapping[str, Any], str]:
    """Parse one status record. A broken record becomes that image's error."""
    stem = status_path.name[: -len(_STATUS_SUFFIX)]
    try:
        record = json.loads(status_path.read_text())
        if not isinstance(record, dict):
            raise ValueError("not a JSON object")
    except (OSError, ValueError) as e:
        return Image(stem, "", "", "", ""), {}, f"malformed status record: {e}"
    missing = [
        key
        for key in ("name", *_IMAGE_FIELDS, "status")
        if not isinstance(record.get(key), str) or not record[key]
    ]
    tags = record.get("published_tags")
    image = Image(
        name=str(record.get("name") or stem),
        **{key: str(record.get(key) or "") for key in _IMAGE_FIELDS},
        ref=str(record.get("ref") or ""),
        digest=str(record.get("digest") or ""),
        published_tags=tuple(str(t) for t in tags) if isinstance(tags, list) else (),
    )
    error = f"malformed status record: missing {', '.join(missing)}" if missing else ""
    return image, record, error


def load_scans(results_dir: Path) -> list[ImageScan]:
    """Load every ``*.status.json`` record and its result in ``results_dir``."""
    scans = []
    for status_path in results_dir.glob(f"*{_STATUS_SUFFIX}"):
        image, record, error = _load_status(status_path)
        if error:
            scans.append(ImageScan(image=image, error=error))
        else:
            scans.append(_load_scan(results_dir, image, record))
    return sorted(scans, key=lambda s: _image_sort_key(s.image))


def _python_key(version: str) -> tuple[int, ...]:
    """Numeric sort key that tolerates suffixes such as "3.13t"."""
    return tuple(int(n) for n in re.findall(r"\d+", version))


def _platform_key(platform: str) -> tuple[int, ...]:
    if platform == "cpu":
        return (0,)
    if platform == "tpu":
        return (1,)
    match = re.match(r"cu(\d+)\.(\d+)", platform)
    return (2, int(match.group(1)), int(match.group(2))) if match else (3,)


def _image_sort_key(image: Image) -> tuple:
    return (
        image.image_type,
        image.architecture,
        _platform_key(image.platform),
        _python_key(image.python_version),
    )


def _severity_rank(severity: str) -> int:
    return GATED_SEVERITIES.index(severity) if severity in GATED_SEVERITIES else 9


def group_findings(scans: Sequence[ImageScan], suppressed: bool = False) -> list[Group]:
    """Deduplicate scanned images' CRITICAL/HIGH findings across images."""
    groups: dict[tuple[str, str, str, bool], Group] = {}
    for scan in scans:
        if not scan.scanned:
            continue
        for key, findings in scan.gated(suppressed).items():
            fixable = _any_fixable(findings)
            group = groups.setdefault(
                (*key, fixable), Group(fixable=fixable, findings=[], images=[])
            )
            group.findings.extend(findings)
            group.images.append(scan.image)
    return sorted(
        groups.values(),
        key=lambda g: (
            _severity_rank(g.finding.severity),
            -len(g.images),
            g.finding.pkg_name,
            g.finding.vuln_id,
        ),
    )


def exit_code(scans: Sequence[ImageScan], issues: Sequence[str] = ()) -> int:
    if issues or not scans or any(not s.scanned for s in scans):
        return EXIT_INCOMPLETE
    if any(g.fixable for g in group_findings(scans)):
        return EXIT_FINDINGS
    return EXIT_CLEAN


def _split_by_fixable(scan: ImageScan) -> tuple[list[Finding], list[Finding]]:
    """One representative finding per key, split by whether the key is fixable."""
    fixable, no_fix = [], []
    for findings in scan.gated().values():
        (fixable if _any_fixable(findings) else no_fix).append(findings[0])
    return fixable, no_fix


def build_info(env: Mapping[str, str]) -> dict[str, Any]:
    return {
        "pipeline": env.get("BUILDKITE_PIPELINE_SLUG", ""),
        "build_number": env.get("BUILDKITE_BUILD_NUMBER", ""),
        "build_url": env.get("BUILDKITE_BUILD_URL", ""),
        "commit": env.get("BUILDKITE_COMMIT", ""),
        "branch": env.get("BUILDKITE_BRANCH", ""),
        "rayci_build_id": env.get("RAYCI_BUILD_ID", ""),
        "triggered_from": {
            "pipeline": env.get("BUILDKITE_TRIGGERED_FROM_BUILD_PIPELINE_SLUG", ""),
            "build_number": env.get("BUILDKITE_TRIGGERED_FROM_BUILD_NUMBER", ""),
        },
    }


def write_csv(path: Path, scans: Sequence[ImageScan], build: Mapping[str, Any]) -> None:
    """One row per image and CRITICAL/HIGH finding, suppressed ones included."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for scan in scans:
            if not scan.scanned:
                continue
            image = scan.image
            for suppressed in (False, True):
                for findings in scan.gated(suppressed).values():
                    for finding in sorted(findings, key=lambda f: f.path):
                        _write_row(writer, build, image, finding)


def _write_row(
    writer: csv.DictWriter, build: Mapping[str, Any], image: Image, finding: Finding
) -> None:
    writer.writerow(
        {
            "pipeline": build["pipeline"],
            "build_number": build["build_number"],
            "commit": build["commit"],
            "image": image.name,
            "image_type": image.image_type,
            "python_version": image.python_version,
            "platform": image.platform,
            "architecture": image.architecture,
            "digest": image.digest,
            "vuln_id": finding.vuln_id,
            "severity": finding.severity,
            "fixable": finding.fixable,
            "status": finding.status,
            "suppressed": finding.suppressed,
            "pkg_class": finding.pkg_class,
            "pkg_type": finding.pkg_type,
            "pkg_name": finding.pkg_name,
            "installed": finding.installed,
            "fixed": finding.fixed,
            "location": finding.location,
            "path": finding.path,
        }
    )


def build_summary(
    scans: Sequence[ImageScan],
    scanner: Mapping[str, Any],
    build: Mapping[str, Any],
    issues: Sequence[str] = (),
) -> dict[str, Any]:
    groups = group_findings(scans)
    images = []
    for scan in scans:
        fixable, no_fix = _split_by_fixable(scan)
        counts = {
            f"{severity.lower()}_{label}": sum(f.severity == severity for f in found)
            for severity in GATED_SEVERITIES
            for label, found in (("fixable", fixable), ("no_fix", no_fix))
        }
        counts["suppressed"] = len(scan.gated(suppressed=True))
        images.append(
            {
                "name": scan.image.name,
                "image_type": scan.image.image_type,
                "python_version": scan.image.python_version,
                "platform": scan.image.platform,
                "architecture": scan.image.architecture,
                "ref": scan.image.ref,
                "digest": scan.image.digest,
                "published_tags": list(scan.image.published_tags),
                "scanned": scan.scanned,
                "error": scan.error,
                "packages": scan.packages,
                "counts": counts,
                "findings": sorted("|".join(key) for key in scan.gated()),
            }
        )
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "build": dict(build),
        "scanner": dict(scanner),
        "issues": list(issues),
        "totals": {
            "images": len(scans),
            "scanned": sum(s.scanned for s in scans),
            "fixable": sum(g.fixable for g in groups),
            "no_fix": sum(not g.fixable for g in groups),
            "suppressed": len(group_findings(scans, suppressed=True)),
        },
        "images": images,
    }
