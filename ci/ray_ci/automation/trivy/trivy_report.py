"""Report the CRITICAL/HIGH findings of a build's Trivy image scans.

Each scan job writes, per image, a ``<name>.status.json`` record and, when the
scan succeeded, Trivy's JSON result as ``<name>.json`` or ``<name>.json.gz``.
This tool reads them all and writes ``report.md``, ``vulns.csv`` and
``summary.json``, and can post the report as a Buildkite annotation.

Only fixable CRITICAL/HIGH findings count against the build. Exit codes:
0 clean, 3 fixable CRITICAL/HIGH findings, 4 incomplete coverage (takes
precedence over 3).

Example:
    bazel run //ci/ray_ci/automation/trivy:trivy_report -- \\
        trivy/results --output-dir trivy --metadata trivy/metadata.json --annotate
"""

import csv
import gzip
import json
import os
import re
import subprocess
import sys
import zlib
from collections import Counter
from dataclasses import dataclass, field
from itertools import groupby
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

import click

GATED_SEVERITIES = ("CRITICAL", "HIGH")
# Trivy statuses meaning no usable fix exists, even if FixedVersion is set.
NO_FIX_STATUSES = frozenset({"affected", "will_not_fix", "fix_deferred", "end_of_life"})

EXIT_CLEAN = 0
EXIT_FINDINGS = 3
EXIT_INCOMPLETE = 4

ANNOTATION_CONTEXT = "trivy-scan"
FILES_NOTE = (
    "`report.md`, `vulns.csv`, `summary.json` and the raw Trivy results are "
    "in this build's `trivy/` artifacts."
)
MISSING_STATUS = "no status record: its scan job did not run or did not upload"
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
    gz_path = results_dir / f"{name}.json.gz"
    if gz_path.is_file():
        return json.loads(gzip.decompress(gz_path.read_bytes()))
    path = results_dir / f"{name}.json"
    if path.is_file():
        return json.loads(path.read_text())
    return None


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


def _variant_parts(images: Sequence[Image]) -> list[str]:
    """Describe one image type's variants, as one cross product if possible."""
    labels = _platform_labels({i.platform for i in images})

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
    "all 184 images" when it appears in every scanned image.
    """
    if len(images) > 1 and set(images) == set(everything):
        return f"all {len(images)} images"
    by_type: dict[str, list[Image]] = {}
    for image in images:
        by_type.setdefault(image.image_type, []).append(image)
    # Image types with identical variants share one description.
    types_by_parts: dict[tuple[str, ...], list[str]] = {}
    for image_type in sorted(by_type):
        parts = tuple(_variant_parts(by_type[image_type]))
        types_by_parts.setdefault(parts, []).append(image_type)
    return "; ".join(
        f"{', '.join(types)} {part}"
        for parts, types in types_by_parts.items()
        for part in parts
    )


def exit_code(
    scans: Sequence[ImageScan],
    failed_steps: Sequence[str],
    issues: Sequence[str] = (),
) -> int:
    if failed_steps or issues or not scans or any(not s.scanned for s in scans):
        return EXIT_INCOMPLETE
    if any(g.fixable for g in group_findings(scans)):
        return EXIT_FINDINGS
    return EXIT_CLEAN


def _cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


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


def _suppressed_table(
    groups: list[Group], everything: list[Image], max_rows: Optional[int]
) -> list[str]:
    rows = [
        f"| {_cve(g.finding)} | {g.finding.severity[0]} | `{_cell(g.finding.pkg_name)}` "
        f"| {_cell(g.finding.statement)} | {describe_images(g.images, everything)} |"
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
    for findings in scan.gated().values():
        (fixable if _any_fixable(findings) else no_fix).append(findings[0])
    return fixable, no_fix


def _per_image_table(scans: Sequence[ImageScan], max_rows: Optional[int]) -> list[str]:
    rows = []
    for scan in scans:
        if not scan.scanned:
            rows.append(f"| `{scan.image.name}` | | | not scanned | | |")
            continue
        fixable, no_fix = _split_by_fixable(scan)
        suppressed = len(scan.gated(suppressed=True))
        rows.append(
            f"| `{scan.image.name}` | `{scan.image.digest[:19]}` | {scan.packages} "
            f"| {_critical_high(fixable)} | {_critical_high(no_fix)} | {suppressed} |"
        )
    header = [
        "| Image | Digest | Packages | Fixable C/H | No fix C/H | Suppressed |",
        "|---|---|---|---|---|---|",
    ]
    return header + _limited(rows, max_rows)


def _scanner_text(scanner: Mapping[str, Any]) -> str:
    trivy = scanner.get("trivy") or {}
    db = scanner.get("db") or {}
    text = ""
    if trivy.get("version"):
        text += f" · Trivy {trivy['version']}"
    # trivy_db records the DB artifact's creation time; older metadata has
    # the DB's own UpdatedAt.
    db_date = db.get("updated_at") or db.get("created")
    if db_date:
        text += f" · DB {db_date[:10]}"
        if db.get("blob"):
            text += f" (`{db['blob'][:19]}`)"
    return text


def render_markdown(
    scans: Sequence[ImageScan],
    failed_steps: Sequence[str],
    scanner: Mapping[str, Any],
    max_rows: Optional[int] = None,
    per_image: bool = True,
    issues: Sequence[str] = (),
    files_note: str = FILES_NOTE,
) -> str:
    """Render the report. ``max_rows`` caps each table, for the annotation."""
    everything = [s.image for s in scans if s.scanned]
    groups = group_findings(scans)
    fixable = [g for g in groups if g.fixable]
    no_fix = [g for g in groups if not g.fixable]
    suppressed = group_findings(scans, suppressed=True)

    lines = [
        "## Trivy image scan: CRITICAL/HIGH",
        "",
        f"**{len(fixable)} fixable CRITICAL/HIGH** · {len(no_fix)} with no fix yet · "
        f"{len(suppressed)} suppressed · {len(everything)}/{len(scans)} images scanned"
        + _scanner_text(scanner),
        "",
    ]

    not_scanned = [s for s in scans if not s.scanned]
    if not_scanned or failed_steps or issues or not scans:
        lines += ["### Incomplete", ""]
        if not scans:
            lines.append("- no scan status records found in the results directory")
        lines += [f"- `{s.image.name}`: {_cell(s.error)}" for s in not_scanned]
        lines += [f"- scan step `{step}` did not pass" for step in failed_steps]
        lines += [f"- {_cell(issue)}" for issue in issues]
        lines.append("")

    lines += [f"### Fixable ({len(fixable)})", ""]
    lines += _fixable_table(fixable, everything, max_rows) if fixable else ["None."]
    lines.append("")
    if no_fix:
        lines += [f"<details><summary>No fix yet ({len(no_fix)})</summary>", ""]
        lines += _no_fix_table(no_fix, everything, max_rows) + ["", "</details>", ""]
    if suppressed:
        lines += [f"<details><summary>Suppressed ({len(suppressed)})</summary>", ""]
        lines += _suppressed_table(suppressed, everything, max_rows) + [
            "",
            "</details>",
            "",
        ]
    if per_image:
        lines += [f"<details><summary>Per image ({len(scans)})</summary>", ""]
        lines += _per_image_table(scans, max_rows) + ["", "</details>", ""]
    else:
        lines += ["_Per-image table omitted here (truncated); see `report.md`._", ""]
    lines.append(files_note)
    return "\n".join(lines) + "\n"


def render_annotation(
    scans: Sequence[ImageScan],
    failed_steps: Sequence[str],
    scanner: Mapping[str, Any],
    limit: int = ANNOTATION_LIMIT_BYTES,
    issues: Sequence[str] = (),
    files_note: str = FILES_NOTE,
) -> str:
    """The longest rendering of the report that fits in ``limit`` bytes."""
    for max_rows, per_image in (
        (None, True),
        (200, True),
        (200, False),
        (50, False),
        (10, False),
        (0, False),
    ):
        body = render_markdown(
            scans, failed_steps, scanner, max_rows, per_image, issues, files_note
        )
        if len(body.encode()) <= limit:
            return body
    return body.encode()[:limit].decode(errors="ignore")


def annotation_style(scans: Sequence[ImageScan], code: int) -> str:
    if any(g.fixable for g in group_findings(scans)):
        return "error"
    return "warning" if code == EXIT_INCOMPLETE else "success"


def annotate(body: str, style: str) -> None:
    subprocess.run(
        [
            "buildkite-agent",
            "annotate",
            "--context",
            ANNOTATION_CONTEXT,
            "--style",
            style,
        ],
        input=body.encode(),
        check=True,
    )


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
    failed_steps: Sequence[str],
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
        "failed_steps": list(failed_steps),
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
    failed_steps: Sequence[str] = (),
    issues: Sequence[str] = (),
    files_note: str = FILES_NOTE,
) -> int:
    """Write report.md, vulns.csv and summary.json; return the exit code."""
    output_dir.mkdir(parents=True, exist_ok=True)
    report = render_markdown(
        scans, failed_steps, scanner, issues=issues, files_note=files_note
    )
    (output_dir / "report.md").write_text(report)
    write_csv(output_dir / "vulns.csv", scans, build)
    summary = build_summary(scans, failed_steps, scanner, build, issues)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return exit_code(scans, failed_steps, issues)


def resolve_path(path: Path) -> Path:
    """Resolve relative paths against the workspace under ``bazel run``."""
    if path.is_absolute():
        return path
    return Path(os.environ.get("BUILD_WORKSPACE_DIRECTORY", "")) / path


def load_metadata(path: Path) -> tuple[dict[str, Any], str]:
    """Read the scanner metadata, or say why it can't be used.

    Without it, results can't be tied to a Trivy binary or DB snapshot, so a
    missing file is an issue, not something to skip silently.
    """
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError) as e:
        return {}, f"scanner metadata unreadable ({path}): {e}"
    if not isinstance(data, dict):
        return {}, f"scanner metadata in {path} is not a JSON object"
    return data, ""


@click.command()
@click.argument("results_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option(
    "--output-dir",
    required=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="Where to write report.md, vulns.csv and summary.json.",
)
@click.option(
    "--metadata",
    "metadata_path",
    type=click.Path(dir_okay=False, path_type=Path),
    help="metadata.json with the Trivy version and DB pin of this build.",
)
@click.option(
    "--failed-step",
    "failed_steps",
    multiple=True,
    help="Key of a scan step that did not pass. Repeatable.",
)
@click.option(
    "--annotate",
    "post_annotation",
    is_flag=True,
    help="Also post the report as a Buildkite annotation.",
)
def main(
    results_dir: Path,
    output_dir: Path,
    metadata_path: Optional[Path],
    failed_steps: tuple[str, ...],
    post_annotation: bool,
) -> None:
    results_dir = resolve_path(results_dir)
    output_dir = resolve_path(output_dir)
    scans = load_scans(results_dir) if results_dir.is_dir() else []
    scanner: dict[str, Any] = {}
    issues: list[str] = []
    if metadata_path:
        scanner, issue = load_metadata(resolve_path(metadata_path))
        if issue:
            issues.append(issue)
    code = write_report(
        output_dir, scans, scanner, build_info(os.environ), failed_steps, issues
    )
    if post_annotation:
        try:
            annotate(
                render_annotation(scans, failed_steps, scanner, issues=issues),
                annotation_style(scans, code),
            )
        except (subprocess.CalledProcessError, OSError) as e:
            # The report files are written; keep the exit code meaningful.
            click.echo(f"warning: could not post the annotation: {e}", err=True)
    sys.exit(code)


if __name__ == "__main__":
    main()
