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
from typing import Any, Mapping

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
    os_packages: int
    os_name: str


@dataclass
class ImageScan:
    image: Image
    findings: list[Finding] = field(default_factory=list)
    packages: int = 0
    error: str = ""

    @property
    def scanned(self) -> bool:
        return not self.error


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


def _list(record: Mapping[str, Any], key: str) -> list:
    """A list field: missing or null reads as empty, any other type raises."""
    value = record.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError(f"{key} is a {type(value).__name__}, not a list")
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


def parse_trivy_report(report: Any) -> ParsedReport:
    """Parse a Trivy ``--format json`` image report. Anything but schema
    version 2 raises, so a format change fails loudly instead of misreading."""
    if not isinstance(report, dict):
        raise TypeError(f"the report is a {type(report).__name__}, not an object")
    if report.get("SchemaVersion") != 2:
        raise ValueError(f"SchemaVersion is {report.get('SchemaVersion')!r}, not 2")
    findings: list[Finding] = []
    packages = os_packages = 0
    for result in _list(report, "Results"):
        count = len(_list(result, "Packages"))
        packages += count
        if _text(result, "Class") == "os-pkgs":
            os_packages += count
        for vuln in _list(result, "Vulnerabilities"):
            findings.append(_finding(vuln, result))
        # --show-suppressed moves ignored findings here.
        for modified in _list(result, "ExperimentalModifiedFindings"):
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
    return ParsedReport(
        findings=findings, packages=packages, os_packages=os_packages, os_name=os_name
    )


def _load_scan(results_dir: Path, image: Image, record: Mapping[str, Any]) -> ImageScan:
    status = record.get("status", "")
    if status != "scanned":
        return ImageScan(image=image, error=record.get("error") or f"status {status!r}")
    path = results_dir / f"{image.name}.json.gz"
    if not path.is_file():
        return ImageScan(image=image, error="no result file")
    try:
        report = json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, EOFError, ValueError, RecursionError, zlib.error) as e:
        # Truncated gzip raises EOFError, corrupt deflate zlib.error and JSON
        # nested too deep RecursionError; one bad upload must not take the
        # other images' results down with it.
        return ImageScan(image=image, error=f"unreadable result: {e!r}")
    try:
        parsed = parse_trivy_report(report)
    except Exception as e:
        # Valid JSON in the wrong shape.
        return ImageScan(
            image=image, error=f"unparseable result: {type(e).__name__}: {e}"
        )
    # A scan that saw nothing, or no distro packages, must not look clean:
    # Trivy only warns when it can't detect the OS, and skips its packages.
    if not parsed.packages:
        error = "no packages detected"
    elif not parsed.os_packages:
        error = f"no OS packages detected (OS: {parsed.os_name or 'not detected'})"
    else:
        error = ""
    return ImageScan(
        image=image, findings=parsed.findings, packages=parsed.packages, error=error
    )


_IMAGE_FIELDS = ("image_type", "python_version", "platform", "architecture")
_OPTIONAL_TEXT = ("ref", "digest", "error")
_STATUS_SUFFIX = ".status.json"


def _load_status(status_path: Path) -> tuple[Image, Mapping[str, Any], str]:
    """Parse one status record. A broken record becomes that image's error.

    The image is named after the file, and a record naming another image is
    broken, so no record can stand in for another image or count it twice.
    """
    stem = status_path.name[: -len(_STATUS_SUFFIX)]
    try:
        record = json.loads(status_path.read_text())
        if not isinstance(record, dict):
            raise ValueError("not a JSON object")
    except (OSError, ValueError, RecursionError) as e:
        return Image(stem, "", "", "", ""), {}, f"malformed status record: {e}"
    text = {key: value for key, value in record.items() if isinstance(value, str)}
    problems = []
    missing = [key for key in ("name", *_IMAGE_FIELDS, "status") if not text.get(key)]
    if missing:
        problems.append(f"missing {', '.join(missing)}")
    not_text = [
        k for k in _OPTIONAL_TEXT if record.get(k) is not None and k not in text
    ]
    tags = record.get("published_tags")
    tags = [] if tags is None else tags
    tags_ok = isinstance(tags, list) and all(isinstance(t, str) for t in tags)
    if not tags_ok:
        not_text.append("published_tags")
    if not_text:
        problems.append(f"{', '.join(not_text)} not text")
    if text.get("name") and text["name"] != stem:
        problems.append(f"name {text['name']!r} doesn't match the file name")
    image = Image(
        name=stem,
        **{key: text.get(key, "") for key in _IMAGE_FIELDS},
        ref=text.get("ref", ""),
        digest=text.get("digest", ""),
        published_tags=tuple(tags) if tags_ok else (),
    )
    error = f"malformed status record: {'; '.join(problems)}" if problems else ""
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
        # Ties, such as "3.13" and "3.13t" or two broken records, by text,
        # so the report's order never depends on the directory listing.
        image.platform,
        image.python_version,
        image.name,
    )
