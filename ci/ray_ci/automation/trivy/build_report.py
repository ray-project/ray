"""Report a build's Trivy image scans from its S3 folder.

Runs once every scan job has finished, passed or not. It reads what trivy_db
and the scan jobs uploaded to the build's folder (see ``scan_store``), and
checks it against the images ``.buildkite/image-scan.rayci.yml`` asks for, so
an image whose job never ran or never uploaded still shows up as not scanned.
It writes:

- ``report.md``, ``vulns.csv`` and ``summary.json`` to ``--output-dir``, and
  to the folder's ``report/`` (``vulns.csv`` gzipped);
- the annotation as ``<annotation-dir>/trivy-scan.<style>.md``, which
  ``.buildkite/hooks/post-command`` posts: postmerge job containers have no
  buildkite-agent.

Exit codes are trivy_report's; a failed report upload turns a clean 0 into 1.

Example:
    bazel run //ci/ray_ci/automation/trivy:build_report -- \\
        --output-dir /artifact-mount/trivy/report \\
        --annotation-dir /artifact-mount/annotations
"""

import dataclasses
import gzip
import logging
import os
import re
import shlex
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

import click
import runfiles
import yaml

from ci.ray_ci.automation.trivy import scan_images, scan_store, trivy_report
from ci.ray_ci.automation.trivy.scan_store import (
    DB_FOLDER,
    REPORT_FOLDER,
    RESULTS_FOLDER,
    ScanStore,
    StoreError,
)
from ci.ray_ci.automation.trivy.trivy_db import DB_METADATA
from ci.ray_ci.automation.trivy.trivy_report import Image, ImageScan
from ci.ray_ci.ray_image import RayImage, RayImageError

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

PIPELINE_RUNFILE = "io_ray/.buildkite/image-scan.rayci.yml"
SCAN_TARGET = "//ci/ray_ci/automation/trivy:scan_images"
RESULT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*\.(status\.json|json\.gz)$")
DOWNLOAD_THREADS = 16


def _commands(step: Mapping[str, Any]) -> list[str]:
    """Buildkite takes ``command`` or ``commands``, each a string or a list."""
    found: list[str] = []
    for key in ("command", "commands"):
        value = step.get(key)
        if isinstance(value, str):
            found.append(value)
        elif isinstance(value, list):
            found += [str(c) for c in value]
    return found


def _step_images(step: Mapping[str, Any]) -> list[Image]:
    """The images one step scans, or none if it doesn't run scan_images.
    A scan step this can't read exactly is an error: guessing would make
    the coverage check quietly wrong."""
    key = step.get("key") or step.get("label") or "?"
    text = " ".join(_commands(step))
    runs = text.count(SCAN_TARGET)
    if not runs:
        return []
    if runs > 1:
        raise ValueError(f"step {key} runs scan_images {runs} times")
    images = []
    for platform in (step.get("array") or {}).get("platform") or [""]:
        expanded = text.replace("{{array.platform}}", platform)
        leftover = re.search(r"\{\{[^}]*\}\}", expanded)
        if leftover:
            raise ValueError(f"step {key} leaves {leftover.group()} unexpanded")
        args = shlex.split(expanded)
        if "--" not in args:
            raise ValueError(f"step {key} has no `--` before scan_images' arguments")
        params = scan_images.main.make_context(
            "scan_images", args[args.index("--") + 1 :], resilient_parsing=True
        ).params
        for option in ("architecture", "platform"):
            if not params.get(option):
                raise ValueError(f"step {key} gives no --{option}")
        if not params.get("images"):
            raise ValueError(f"step {key} gives no --image")
        for spec in params["images"]:
            try:
                image_type, python_version = scan_images.parse_image(spec)
                ray_image = RayImage(
                    image_type,
                    python_version,
                    params["platform"],
                    params["architecture"],
                )
                ray_image.validate()
            except (click.BadParameter, RayImageError) as e:
                raise ValueError(f"step {key}: {e}") from e
            images.append(
                Image(
                    name=ray_image.wanda_image_name,
                    image_type=image_type,
                    python_version=python_version,
                    platform=ray_image.platform,
                    architecture=ray_image.architecture,
                )
            )
    return images


def expected_images(pipeline: Mapping[str, Any]) -> list[Image]:
    """Every image the pipeline's scan steps are asked to scan, read with
    scan_images' own option parser."""
    images = [
        image for step in pipeline.get("steps") or [] for image in _step_images(step)
    ]
    names = [image.name for image in images]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ValueError(f"more than one scan step scans {duplicates}")
    return images


RESULT_FILES = {".status.json": "status record", ".json.gz": "result"}


def download_results(store: ScanStore, dest: Path) -> tuple[list[str], dict[str, str]]:
    """Download every status record and result. Returns the build-wide
    issues, and why each image whose files failed to download lacks them."""
    dest.mkdir(parents=True)
    try:
        names = store.names(RESULTS_FOLDER)
    except StoreError as e:
        return [f"could not list the scan results: {e}"], {}
    for name in names:
        if not RESULT_NAME.match(name):
            logger.warning(f"Ignoring unexpected file {name} in results/")

    def fetch(name: str) -> str:
        try:
            store.download(RESULTS_FOLDER, name, dest / name)
        except StoreError as e:
            return str(e)
        return ""

    wanted = [n for n in names if RESULT_NAME.match(n)]
    # names() made the client; boto3 clients are thread-safe once made.
    with ThreadPoolExecutor(max_workers=DOWNLOAD_THREADS) as pool:
        errors = list(pool.map(fetch, wanted))
    failed = {}
    for name, error in zip(wanted, errors):
        if error:
            suffix = next(s for s in RESULT_FILES if name.endswith(s))
            image = name[: -len(suffix)]
            failed[image] = f"could not download its {RESULT_FILES[suffix]}: {error}"
    return [], failed


def explain_failed_downloads(
    scans: Sequence[ImageScan], failed: Mapping[str, str]
) -> list[ImageScan]:
    """A scan whose result failed to download says so, not "no result file"."""
    return [
        dataclasses.replace(s, error=failed[s.image.name])
        if s.image.name in failed and not s.scanned
        else s
        for s in scans
    ]


def load_scanner(store: ScanStore, workdir: Path) -> tuple[dict[str, Any], str]:
    try:
        path = store.download(DB_FOLDER, DB_METADATA, workdir / DB_METADATA)
    except StoreError as e:
        return {}, f"scanner metadata unavailable: {e}"
    return trivy_report.load_metadata(path)


def files_note(store: ScanStore, uploaded: bool = True) -> str:
    raw = f"each image's raw Trivy result in `{store.url(RESULTS_FOLDER)}`."
    if not uploaded:
        return f"The report is only in this job's `trivy/report/` artifacts; {raw}"
    return (
        f"Kept for 30 days: the report in `{store.url(REPORT_FOLDER)}` "
        "(`report.md`, `summary.json`, `vulns.csv.gz`) and in this job's "
        f"`trivy/report/` artifacts; {raw}"
    )


def publish_report(store: ScanStore, output_dir: Path, workdir: Path) -> str:
    """Upload the report, summary.json last; returns the error, if any."""
    csv_gz = workdir / "vulns.csv.gz"
    with open(output_dir / "vulns.csv", "rb") as src, gzip.open(csv_gz, "wb") as out:
        shutil.copyfileobj(src, out)
    try:
        for path in (csv_gz, output_dir / "report.md", output_dir / "summary.json"):
            store.upload(path, REPORT_FOLDER)
    except StoreError as e:
        return str(e)
    return ""


def write_annotation(annotation_dir: Path, body: str, style: str) -> Path:
    annotation_dir.mkdir(parents=True, exist_ok=True)
    path = annotation_dir / f"{trivy_report.ANNOTATION_CONTEXT}.{style}.md"
    path.write_text(body)
    return path


def failure_annotation(error: BaseException) -> str:
    detail = re.sub(r"\s+", " ", f"{type(error).__name__}: {error}")
    detail = detail.replace("`", "'")[:1000]
    return (
        "## Trivy image scan: report failed\n\n"
        f"`{detail}`\n\n"
        "The scans may have finished; this job's log has the traceback.\n"
    )


def report_build(
    output_dir: Path,
    annotation_dir: Path,
    pipeline_file: Optional[Path],
    work_dir: Optional[Path],
    store: ScanStore,
) -> int:
    """Write the report and the annotation; returns the exit code."""
    if not pipeline_file:
        pipeline_file = Path(runfiles.Create().Rlocation(PIPELINE_RUNFILE))
    expected = expected_images(yaml.safe_load(pipeline_file.read_text()))
    note = files_note(store)

    with tempfile.TemporaryDirectory(dir=work_dir) as tmp:
        workdir = Path(tmp)
        issues, failed = download_results(store, workdir / "results")
        scanner, issue = load_scanner(store, workdir)
        if issue:
            issues.append(issue)
        scans = explain_failed_downloads(
            trivy_report.load_scans(workdir / "results"), failed
        )
        scans = trivy_report.add_missing(scans, expected, failed)
        expected_names = {image.name for image in expected}
        issues += [
            f"`{name}`: {error}"
            for name, error in sorted(failed.items())
            if name not in expected_names
        ]
        code = trivy_report.write_report(
            output_dir,
            scans,
            scanner,
            trivy_report.build_info(os.environ),
            issues=issues,
            files_note=note,
        )
        upload_error = publish_report(store, output_dir, workdir)

    style = trivy_report.annotation_style(scans, code)
    if upload_error:
        issues = [*issues, f"the report could not be uploaded to S3: {upload_error}"]
        note = files_note(store, uploaded=False)
        style = "warning" if style == "success" else style
    body = trivy_report.render_annotation(
        scans, [], scanner, issues=issues, files_note=note
    )
    annotation = write_annotation(annotation_dir, body, style)
    logger.info(f"Wrote {output_dir} and {annotation}")
    if upload_error:
        logger.error(f"Could not upload the report: {upload_error}")
        return code or 1
    logger.info(f"Uploaded the report to {store.url(REPORT_FOLDER)}")
    return code


@click.command()
@click.option(
    "--output-dir",
    required=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="Where to write report.md, vulns.csv and summary.json.",
)
@click.option(
    "--annotation-dir",
    required=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="Where to leave the annotation for the post-command hook.",
)
@click.option(
    "--pipeline-file",
    type=click.Path(dir_okay=False, path_type=Path),
    help="The scan steps (default: the image-scan.rayci.yml built in).",
)
@click.option("--work-dir", type=click.Path(file_okay=False, path_type=Path))
@scan_store.build_options
def main(
    output_dir: Path,
    annotation_dir: Path,
    pipeline_file: Optional[Path],
    work_dir: Optional[Path],
    store: ScanStore,
) -> None:
    output_dir = trivy_report.resolve_path(output_dir)
    annotation_dir = trivy_report.resolve_path(annotation_dir)
    if pipeline_file:
        pipeline_file = trivy_report.resolve_path(pipeline_file)
    try:
        code = report_build(output_dir, annotation_dir, pipeline_file, work_dir, store)
    except Exception as e:
        # Without an annotation, a soft-failed report job goes unnoticed.
        logger.exception("The Trivy report failed")
        write_annotation(annotation_dir, failure_annotation(e), "warning")
        code = 1
    sys.exit(code)


if __name__ == "__main__":
    main()
