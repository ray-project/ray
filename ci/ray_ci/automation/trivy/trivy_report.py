"""Report the CRITICAL/HIGH findings of a build's Trivy image scans.

Each scan job writes, per image, a ``<name>.status.json`` record and, when the
scan succeeded, Trivy's JSON result as ``<name>.json.gz``. ``load_scans``
reads them all; ``write_report`` writes ``report.md``, ``vulns.csv`` and
``summary.json``; ``render_annotation`` renders the build's annotation.
``build_report`` drives all of it in CI.

Only fixable CRITICAL/HIGH findings count against the build. ``exit_code``
gives 0 clean, 10 fixable CRITICAL/HIGH findings, 11 incomplete coverage
(takes precedence over 10).
"""

import csv
import gzip
import json
import os
import re
import zlib
from collections import Counter
from dataclasses import dataclass, field
from itertools import groupby
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

ANNOTATION_CONTEXT = "trivy-scan"
MISSING_STATUS = "no status record: its scan job did not run or did not upload"
# A package with more no-fix CVEs than this in one image shows a count: kernel
# headers alone carry ~500 per CUDA image, too many to list for every image.
NO_FIX_LIST_LIMIT = 10
# Buildkite rejects annotation bodies over 1 MiB; keep some headroom.
ANNOTATION_LIMIT_BYTES = 1024 * 1024 - 16 * 1024
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

    def gated_by_key(
        self, suppressed: bool = False
    ) -> dict[tuple[str, str, str], list[Finding]]:
        """CRITICAL/HIGH findings by key, with one finding per install path.

        A package installed in two paths counts once per image, but both
        paths are kept so the report can show every place to fix. If one path
        lists a finding twice, the copy with a fix wins, so the order Trivy
        lists them in can't hide a fix.
        """
        by_key: dict[tuple[str, str, str], list[Finding]] = {}
        for finding in self.findings:
            if finding.gated and finding.suppressed == suppressed:
                same_key = by_key.setdefault(finding.key, [])
                same_path = [
                    i for i, f in enumerate(same_key) if f.path == finding.path
                ]
                if not same_path:
                    same_key.append(finding)
                elif finding.fixable and not same_key[same_path[0]].fixable:
                    same_key[same_path[0]] = finding
        return by_key


def _any_fixable(findings: Iterable[Finding]) -> bool:
    return any(f.fixable for f in findings)


def _representative(findings: Sequence[Finding]) -> Finding:
    """One with a usable fix if any has one."""
    return next((f for f in findings if f.fixable), findings[0])


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
        return _representative(self.findings)

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


def add_missing(
    scans: Sequence[ImageScan],
    expected: Sequence[Image],
    reasons: Optional[Mapping[str, str]] = None,
) -> list[ImageScan]:
    """Add each expected image that has no status record, as not scanned,
    with its reason from ``reasons`` when one is known."""
    found = {s.image.name for s in scans}
    reasons = reasons or {}
    missing = [
        ImageScan(image=image, error=reasons.get(image.name, MISSING_STATUS))
        for image in expected
        if image.name not in found
    ]
    return sorted([*scans, *missing], key=lambda s: _image_sort_key(s.image))


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


def _platform_label(platform: str) -> str:
    """cu12.8.1-cudnn -> cu12.8; cpu and tpu stay as they are."""
    match = re.match(r"(cu\d+\.\d+)", platform)
    return match.group(1) if match else platform


def _platform_labels(platforms: Iterable[str]) -> dict[str, str]:
    """Short labels, or full names where two platforms would share a label."""
    short = {p: _platform_label(p) for p in platforms}
    uses = Counter(short.values())
    return {p: label if uses[label] == 1 else p for p, label in short.items()}


def _platform_sort_key(platform: str) -> tuple:
    return (_platform_key(platform), platform)


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


def _severity_rank(severity: str) -> int:
    return GATED_SEVERITIES.index(severity) if severity in GATED_SEVERITIES else 9


def group_findings(scans: Sequence[ImageScan], suppressed: bool = False) -> list[Group]:
    """Deduplicate scanned images' CRITICAL/HIGH findings across images."""
    groups: dict[tuple[str, str, str, bool], Group] = {}
    for scan in scans:
        if not scan.scanned:
            continue
        for key, findings in scan.gated_by_key(suppressed).items():
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


def _python_range(versions: Sequence[str]) -> str:
    unique = sorted(set(versions), key=lambda v: (_python_key(v), v))
    keys = [_python_key(v) for v in unique]
    # Only plain versions form a range; "3.13t" is listed on its own.
    contiguous = (
        len(unique) > 1
        and all(re.fullmatch(r"\d+(\.\d+)*", v) for v in unique)
        and all(b[:-1] == a[:-1] and b[-1] == a[-1] + 1 for a, b in zip(keys, keys[1:]))
    )
    if contiguous:
        return f"py{unique[0]}–{unique[-1]}"
    return ", ".join(f"py{v}" for v in unique)


def _variant_parts(images: Sequence[Image], labels: Mapping[str, str]) -> list[str]:
    """Describe one image type's variants, as one cross product if possible."""

    def cross_product(subset: Sequence[Image]) -> Optional[str]:
        pythons = {i.python_version for i in subset}
        platforms = sorted({i.platform for i in subset}, key=_platform_sort_key)
        archs = sorted({i.architecture for i in subset})
        combos = {(i.python_version, i.platform, i.architecture) for i in subset}
        if len(combos) != len(pythons) * len(platforms) * len(archs):
            return None
        names = ", ".join(labels[p] for p in platforms)
        return f"{_python_range(sorted(pythons))} · {names} · {', '.join(archs)}"

    whole = cross_product(images)
    if whole:
        return [whole]
    by_arch = {
        arch: list(group)
        for arch, group in groupby(
            sorted(images, key=lambda i: i.architecture), key=lambda i: i.architecture
        )
    }
    per_arch = [cross_product(group) for group in by_arch.values()]
    if all(per_arch):
        return per_arch
    # Sort on the full platform name too, so groupby sees each variant as one run.
    by_variant = sorted(
        images, key=lambda i: (_platform_sort_key(i.platform), i.architecture)
    )
    return [
        cross_product(list(group))
        for _, group in groupby(by_variant, key=lambda i: (i.platform, i.architecture))
    ]


def describe_images(images: Sequence[Image], everything: Sequence[Image]) -> str:
    """Compact description of the image variants a finding appears in.

    For example "ray, ray-extra py3.11–3.14 · cu12.8 · x86_64", or
    "all 184 scanned images" when it appears in every scanned image.
    """
    if len(images) > 1 and set(images) == set(everything):
        return f"all {len(images)} scanned images"
    # Labels from every image, so a label means the same platform in every row.
    labels = _platform_labels({i.platform for i in [*images, *everything]})
    by_type: dict[str, list[Image]] = {}
    for image in images:
        by_type.setdefault(image.image_type, []).append(image)
    # Image types with identical variants share one description.
    types_by_parts: dict[tuple[str, ...], list[str]] = {}
    for image_type in sorted(by_type):
        parts = tuple(_variant_parts(by_type[image_type], labels))
        types_by_parts.setdefault(parts, []).append(image_type)
    return "; ".join(
        f"{', '.join(types)} {part}"
        for parts, types in types_by_parts.items()
        for part in parts
    )


def exit_code(scans: Sequence[ImageScan], issues: Sequence[str] = ()) -> int:
    if issues or not scans or any(not s.scanned for s in scans):
        return EXIT_INCOMPLETE
    if any(g.fixable for g in group_findings(scans)):
        return EXIT_FINDINGS
    return EXIT_CLEAN


def _cell(value: str) -> str:
    # Any line break, a bare "\r" included, would end the table row.
    return re.sub(r"\s+", " ", value).replace("|", "\\|")


def _cve(finding: Finding) -> str:
    return f"[{finding.vuln_id}]({finding.url})" if finding.url else finding.vuln_id


def _limited(rows: list[str], max_rows: Optional[int]) -> list[str]:
    if max_rows is None or len(rows) <= max_rows:
        return rows
    hidden = len(rows) - max_rows
    return rows[:max_rows] + [
        "",
        f"_{hidden} more rows not shown here (truncated); see `report.md` "
        "in the build artifacts._",
    ]


def _where(group: Group) -> str:
    return ", ".join(f"`{_cell(location)}`" for location in group.locations)


def _fixable_table(
    groups: list[Group], everything: list[Image], max_rows: Optional[int]
) -> list[str]:
    rows = [
        f"| {_cve(g.finding)} | {g.finding.severity[0]} | `{_cell(g.finding.pkg_name)}` "
        f"| {_cell(g.finding.installed)} → {_cell(g.finding.fixed)} "
        f"| {_where(g)} | {describe_images(g.images, everything)} |"
        for g in groups
    ]
    header = [
        "| CVE | Sev | Package | Installed → Fixed | Where | Images |",
        "|---|---|---|---|---|---|",
    ]
    return header + _limited(rows, max_rows)


def _no_fix_table(
    groups: list[Group], everything: list[Image], max_rows: Optional[int]
) -> list[str]:
    """One row per package: kernel headers alone carry ~500 CVEs per image."""
    by_package: dict[tuple[str, str, str], list[Group]] = {}
    for g in groups:
        f = g.finding
        by_package.setdefault((f.pkg_name, f.installed, _where(g)), []).append(g)
    rows = []
    for (pkg, installed, where), pkg_groups in sorted(
        by_package.items(), key=lambda kv: -len(kv[1])
    ):
        critical = sum(g.finding.severity == "CRITICAL" for g in pkg_groups)
        statuses = sorted({g.finding.status or "unknown" for g in pkg_groups})
        images = {i for g in pkg_groups for i in g.images}
        rows.append(
            f"| `{_cell(pkg)}` | {_cell(installed)} | {where} "
            f"| {len(pkg_groups)} ({critical}/{len(pkg_groups) - critical}) "
            f"| {', '.join(statuses)} "
            f"| {describe_images(sorted(images, key=_image_sort_key), everything)} |"
        )
    header = [
        "| Package | Installed | Where | CVEs (C/H) | Status | Images |",
        "|---|---|---|---|---|---|",
    ]
    return header + _limited(rows, max_rows)


def _statements(group: Group) -> str:
    # Each image type has its own ignore file, so the reasons can differ.
    return "; ".join(
        sorted({_cell(f.statement) for f in group.findings if f.statement})
    )


def _suppressed_table(
    groups: list[Group], everything: list[Image], max_rows: Optional[int]
) -> list[str]:
    rows = [
        f"| {_cve(g.finding)} | {g.finding.severity[0]} | `{_cell(g.finding.pkg_name)}` "
        f"| {_statements(g)} | {describe_images(g.images, everything)} |"
        for g in groups
    ]
    header = ["| CVE | Sev | Package | Statement | Images |", "|---|---|---|---|---|"]
    return header + _limited(rows, max_rows)


def _critical_high(findings: Sequence[Finding]) -> str:
    critical = sum(f.severity == "CRITICAL" for f in findings)
    return f"{critical} / {len(findings) - critical}"


def _split_by_fixable(scan: ImageScan) -> tuple[list[Finding], list[Finding]]:
    """One representative finding per key, split by whether the key is fixable."""
    fixable, no_fix = [], []
    for findings in scan.gated_by_key().values():
        finding = _representative(findings)
        (fixable if finding.fixable else no_fix).append(finding)
    return fixable, no_fix


def _image_cves(findings: Sequence[Finding], list_limit: Optional[int]) -> str:
    """One image's CVEs by package, CRITICAL first and in bold; a package with
    more than ``list_limit`` CVEs shows a count instead."""
    by_package: dict[tuple[str, str], list[Finding]] = {}
    for f in findings:
        by_package.setdefault((f.pkg_name, f.installed), []).append(f)

    def impact(item: tuple[tuple[str, str], list[Finding]]) -> tuple:
        (pkg, installed), found = item
        critical = sum(f.severity == "CRITICAL" for f in found)
        return (-critical, -len(found), pkg, installed)

    parts = []
    for (pkg, installed), found in sorted(by_package.items(), key=impact):
        found = sorted(found, key=lambda f: (_severity_rank(f.severity), f.vuln_id))
        if list_limit is not None and len(found) > list_limit:
            critical = sum(f.severity == "CRITICAL" for f in found)
            cves = f"{len(found)} CVEs ({critical} C / {len(found) - critical} H)"
        else:
            cves = ", ".join(
                f"**{f.vuln_id}**" if f.severity == "CRITICAL" else f.vuln_id
                for f in found
            )
        parts.append(f"`{_cell(pkg)}` {_cell(installed)}: {cves}")
    return "; ".join(parts)


def _digest(image: Image) -> str:
    # A status record may lack a digest; an empty code span shows as "``".
    return f"`{image.digest[:19]}`" if image.digest else ""


def _image_cve_table(
    scans: Sequence[ImageScan],
    fixable: bool,
    max_rows: Optional[int],
    list_limit: Optional[int],
) -> list[str]:
    """One row per image with such findings, most affected first."""
    ranked = []
    for scan in scans:
        if not scan.scanned:
            continue
        found = _split_by_fixable(scan)[0 if fixable else 1]
        if not found:
            continue
        critical = sum(f.severity == "CRITICAL" for f in found)
        row = (
            f"| `{scan.image.name}` | {_digest(scan.image)} "
            f"| {_critical_high(found)} | {_image_cves(found, list_limit)} |"
        )
        ranked.append(((-critical, -len(found), _image_sort_key(scan.image)), row))
    label = "Fixable C/H" if fixable else "No fix C/H"
    header = [
        f"| Image | Digest | {label} | CVEs by package |",
        "|---|---|---|---|",
    ]
    return header + _limited([row for _, row in sorted(ranked)], max_rows)


def _collapsed(title: str, count: int, table: list[str], note: str = "") -> list[str]:
    """A heading over a table that stays folded until opened, so the
    annotation stays short on the build page."""
    lines = [f"### {title} ({count})", ""]
    if not count:
        return lines + ["None.", ""]
    lines += ["<details><summary>Show the table</summary>", "", *table, ""]
    if note:
        lines += [note, ""]
    return lines + ["</details>", ""]


def _per_image_table(scans: Sequence[ImageScan], max_rows: Optional[int]) -> list[str]:
    rows = []
    for scan in scans:
        if not scan.scanned:
            rows.append(f"| `{scan.image.name}` | | | not scanned | | |")
            continue
        fixable, no_fix = _split_by_fixable(scan)
        suppressed = len(scan.gated_by_key(suppressed=True))
        rows.append(
            f"| `{scan.image.name}` | {_digest(scan.image)} | {scan.packages} "
            f"| {_critical_high(fixable)} | {_critical_high(no_fix)} | {suppressed} |"
        )
    header = [
        "| Image | Digest | Packages | Fixable C/H | No fix C/H | Suppressed |",
        "|---|---|---|---|---|---|",
    ]
    return header + _limited(rows, max_rows)


def _scanner_text(scanner: Mapping[str, Any]) -> str:
    """Trivy's version and the DB's date. A field of the wrong type reads as
    missing, so bad metadata can't stop the report."""

    def field(section: str, key: str) -> str:
        record = scanner.get(section)
        value = record.get(key) if isinstance(record, dict) else None
        return value if isinstance(value, str) else ""

    version = field("trivy", "version")
    created, blob = field("db", "created"), field("db", "blob")
    text = f" · Trivy {version}" if version else ""
    if created:
        text += f" · DB {created[:10]}"
        if blob:
            text += f" (`{blob[:19]}`)"
    return text


def render_markdown(
    scans: Sequence[ImageScan],
    scanner: Mapping[str, Any],
    max_rows: Optional[int] = None,
    per_image: bool = True,
    issues: Sequence[str] = (),
    *,
    files_note: str,
    cve_list_limit: Optional[int] = None,
) -> str:
    """Render the report. ``max_rows`` caps each table and ``cve_list_limit``
    each package's CVE list in the per-image tables, for the annotation."""
    everything = [s.image for s in scans if s.scanned]
    groups = group_findings(scans)
    fixable = [g for g in groups if g.fixable]
    no_fix = [g for g in groups if not g.fixable]
    suppressed = group_findings(scans, suppressed=True)
    scanned = [s for s in scans if s.scanned]
    fixable_images = sum(1 for s in scanned if _split_by_fixable(s)[0])
    no_fix_images = sum(1 for s in scanned if _split_by_fixable(s)[1])
    no_fix_limit = (
        NO_FIX_LIST_LIMIT
        if cve_list_limit is None
        else min(NO_FIX_LIST_LIMIT, cve_list_limit)
    )

    lines = [
        "## Trivy image scan: CRITICAL/HIGH",
        "",
        f"**{len(fixable)} fixable CRITICAL/HIGH** · {len(no_fix)} with no fix yet · "
        f"{len(suppressed)} suppressed · {len(everything)}/{len(scans)} images scanned"
        + _scanner_text(scanner),
        "",
    ]

    not_scanned = [s for s in scans if not s.scanned]
    if not_scanned or issues or not scans:
        lines += ["### Incomplete", ""]
        if not scans:
            lines.append("- no scan status records found in the results directory")
        lines += [f"- `{s.image.name}`: {_cell(s.error)}" for s in not_scanned]
        lines += [f"- {_cell(issue)}" for issue in issues]
        lines.append("")

    capped = (
        f"_A package with more than {cve_list_limit} CVEs in an image shows a "
        "count here; `report.md` lists every CVE._"
        if cve_list_limit is not None
        else ""
    )
    lines += _collapsed(
        "Fixable CVEs", len(fixable), _fixable_table(fixable, everything, max_rows)
    )
    lines += _collapsed(
        "Fixable Images",
        fixable_images,
        _image_cve_table(scans, True, max_rows, cve_list_limit),
        capped,
    )
    lines += _collapsed(
        "No Fix Yet", len(no_fix), _no_fix_table(no_fix, everything, max_rows)
    )
    lines += _collapsed(
        "No Fix Yet Per Image",
        no_fix_images,
        _image_cve_table(scans, False, max_rows, no_fix_limit),
        f"_A package with more than {no_fix_limit} CVEs in an image shows a count; "
        "`vulns.csv` lists every CVE._",
    )
    if suppressed:
        lines += _collapsed(
            "Suppressed",
            len(suppressed),
            _suppressed_table(suppressed, everything, max_rows),
        )
    if per_image:
        lines += _collapsed("Per Image", len(scans), _per_image_table(scans, max_rows))
    else:
        lines += [
            f"### Per Image ({len(scans)})",
            "",
            "_Omitted here (truncated); see `report.md`._",
            "",
        ]
    lines.append(files_note)
    return "\n".join(lines) + "\n"


def render_annotation(
    scans: Sequence[ImageScan],
    scanner: Mapping[str, Any],
    limit: int = ANNOTATION_LIMIT_BYTES,
    issues: Sequence[str] = (),
    *,
    files_note: str,
) -> str:
    """The longest rendering of the report that fits in ``limit`` bytes:
    shorter CVE lists first, then fewer rows."""
    for max_rows, per_image, cve_list_limit in (
        (None, True, None),
        (None, True, 20),
        (None, True, 5),
        (200, True, 5),
        (200, False, 5),
        (50, False, 3),
        (10, False, 1),
        (0, False, 0),
    ):
        body = render_markdown(
            scans,
            scanner,
            max_rows,
            per_image,
            issues,
            files_note=files_note,
            cve_list_limit=cve_list_limit,
        )
        if len(body.encode()) <= limit:
            return body
    return body.encode()[:limit].decode(errors="ignore")


def annotation_style(scans: Sequence[ImageScan], code: int) -> str:
    if any(g.fixable for g in group_findings(scans)):
        return "error"
    return "success" if code == EXIT_CLEAN else "warning"


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
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for scan in scans:
            if not scan.scanned:
                continue
            image = scan.image
            for suppressed in (False, True):
                for findings in scan.gated_by_key(suppressed).values():
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
    """The build's result for tracking over time: its verdict, per-image
    counts and findings, and totals.

    Like the report's tables, the totals count groups: a finding fixable in
    one image and not in another counts once in each of ``fixable`` and
    ``no_fix``. An image that wasn't scanned counts nothing, as in the totals
    and vulns.csv, even if Trivy returned some findings for it.
    """
    groups = group_findings(scans)
    images = []
    for scan in scans:
        gated = scan.gated_by_key() if scan.scanned else {}
        suppressed = scan.gated_by_key(suppressed=True) if scan.scanned else {}
        fixable, no_fix = _split_by_fixable(scan) if scan.scanned else ([], [])
        counts = {
            f"{severity.lower()}_{label}": sum(f.severity == severity for f in found)
            for severity in GATED_SEVERITIES
            for label, found in (("fixable", fixable), ("no_fix", no_fix))
        }
        counts["suppressed"] = len(suppressed)
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
                "findings": sorted("|".join(key) for key in gated),
            }
        )
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "exit_code": exit_code(scans, issues),
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


def write_report(
    output_dir: Path,
    scans: Sequence[ImageScan],
    scanner: Mapping[str, Any],
    build: Mapping[str, Any],
    issues: Sequence[str] = (),
    *,
    files_note: str,
) -> int:
    """Write report.md, vulns.csv and summary.json; return the exit code."""
    output_dir.mkdir(parents=True, exist_ok=True)
    report = render_markdown(scans, scanner, issues=issues, files_note=files_note)
    (output_dir / "report.md").write_text(report, encoding="utf-8")
    write_csv(output_dir / "vulns.csv", scans, build)
    summary = build_summary(scans, scanner, build, issues)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    return exit_code(scans, issues)


def resolve_path(path: Path) -> Path:
    """Resolve relative paths against the workspace under ``bazel run``."""
    if path.is_absolute():
        return path
    return Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")) / path


def load_metadata(path: Path, source: str) -> tuple[dict[str, Any], str]:
    """Read the scanner metadata, downloaded from ``source``, or say why it
    can't be used.

    Without it, results can't be tied to a Trivy binary or DB snapshot, so a
    missing file is an issue, not something to skip silently.
    """
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError, RecursionError) as e:
        return {}, f"scanner metadata unreadable ({source}): {e}"
    if not isinstance(data, dict):
        return {}, f"scanner metadata in {source} is not a JSON object"
    return data, ""
