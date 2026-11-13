"""Read the results of a build's Trivy image scans.

Each scan job writes, per image, a ``<name>.status.json`` record and, when the
scan succeeded, Trivy's JSON result as ``<name>.json.gz``. ``load_scans``
reads them all into one ``ImageScan`` per image: its findings and the
packages Trivy saw, or why the image wasn't scanned.
"""

import gzip
import json
import re
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

GATED_SEVERITIES = ("CRITICAL", "HIGH")
# Trivy statuses meaning no usable fix exists, even if FixedVersion is set.
NO_FIX_STATUSES = frozenset({"affected", "will_not_fix", "fix_deferred", "end_of_life"})


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
