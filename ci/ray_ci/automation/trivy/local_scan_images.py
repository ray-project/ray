"""Scan any images, by name, on your own machine.

The CI scan covers the images a build made, and its jobs hand files to each
other through the build's S3 folder. This scans any images given by name
and tag or digest, e.g. a published rayproject/ray tag, with no build or S3.
It writes the CI report and the annotation markdown to --output-dir.

It pulls each image from its registry, so an image built locally must be
pushed to a registry first; one run on localhost will do (see README.md).

It reuses the CI scan's pieces: trivy_db stages the DBs, pinned by digest,
in a local folder instead of S3. scan_images checks each digest, pulls the
images into an OCI layout and runs the pinned Trivy in a network-less
container. trivy_report renders the results. Like CI's scan jobs, it needs
Linux x86_64, Docker, and access to the registries and the DBs' upstream.

Each run downloads the DBs, over 1 GB, into a temporary directory.

crane uses docker's credentials. For a private ECR registry,
local_scan_images logs in itself with your AWS credentials, but only to registries in us-west-2.

Exit codes are trivy_report's: 0 clean, 10 fixable CRITICAL/HIGH findings,
11 incomplete coverage. A failed DB download exits 1, bad options 2.

Example:
    bazel run //ci/ray_ci/automation/trivy:local_scan_images -- \\
        --image rayproject/ray:2.60.0-py312-cpu --image ubuntu:22.04 \\
        --output-dir /tmp/trivy-report
"""

import contextlib
import dataclasses
import logging
import os
import re
import signal
import sys
import tempfile
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional, Sequence

import click

from ci.ray_ci.automation.trivy import registry, scan_images, trivy_db, trivy_report
from ci.ray_ci.automation.trivy.scan_images import (
    BASE_IMAGE,
    OCI_ARCH,
    Target,
)
from ci.ray_ci.automation.trivy.scan_store import DB_FOLDER, LocalStore, StoreError
from ci.ray_ci.automation.trivy.trivy_db import DB_METADATA
from ci.ray_ci.automation.trivy.trivy_report import Image, ImageScan
from ci.ray_ci.utils import ecr_docker_login

logger = logging.getLogger(__name__)

IMAGE_TYPE = "image"
# Only the characters an image's name, tag or digest uses: nothing a shell, the
# markdown or crane (a leading "-" reads as a flag) would act on.
IMAGE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/:@-]*")
# A local run may go on for hours, so only containers and volumes older than
# this are taken for another run's leftovers (CI's jobs are shorter).
LOCAL_STALE_AFTER = timedelta(days=1)
IGNORE_SUFFIXES = (".yaml", ".yml")


@dataclass
class LocalTarget(Target):
    """An image given by name: no Ray image type, Python version or
    platform, and nothing published."""

    architecture: str = "x86_64"
    may_be_index = True

    @property
    def name(self) -> str:
        """The image's name as a file name, for results/<name>.json.gz."""
        return re.sub(r"[^A-Za-z0-9._-]+", "_", self.ref)

    @property
    def image_type(self) -> str:
        return IMAGE_TYPE

    @property
    def repo(self) -> str:
        """The image's name without its tag or digest; a registry's port isn't
        a tag."""
        name = self.ref.split("@", 1)[0]
        if ":" in name.rsplit("/", 1)[-1]:
            name = name.rsplit(":", 1)[0]
        return name

    def image(self) -> Image:
        return Image(
            name=self.name,
            image_type=IMAGE_TYPE,
            python_version="",
            platform="",
            architecture=self.architecture,
            ref=self.ref,
            digest=self.digest,
        )

    def status_record(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "image_type": IMAGE_TYPE,
            "python_version": "",
            "platform": "",
            "architecture": self.architecture,
            "ref": self.ref,
            "digest": self.digest,
            "published_tags": [],
            "status": "scanned" if self.scanned and not self.error else "error",
            "error": self.error,
            "trivy_db": self.db,
        }


def read_images(images: Sequence[str], images_file: Optional[Path]) -> list[str]:
    """The --image values, then the file's lines; "#" starts a comment."""
    names = list(images)
    if images_file:
        lines = images_file.read_text(encoding="utf-8").splitlines()
        names += [line.split("#", 1)[0].strip() for line in lines]
    return [name for name in names if name]


def local_targets(images: Sequence[str], architecture: str) -> list[LocalTarget]:
    targets: list[LocalTarget] = []
    for image in images:
        if not IMAGE_NAME.fullmatch(image):
            raise click.BadParameter(f"{image!r} is not an image name")
        # No Ray image context: the target knows its image from its name.
        target = LocalTarget(ctx=None, ref=image, architecture=architecture)
        clash = next((t for t in targets if t.name == target.name), None)
        if clash and clash.ref == image:
            raise click.BadParameter(f"{image} is given twice")
        if clash:
            raise click.BadParameter(
                f"{clash.ref} and {image} would share the result file {target.name}"
            )
        targets.append(target)
    if not targets:
        raise click.UsageError("Give at least one --image or --images-file.")
    return targets


def download_dbs(store: LocalStore, workdir: Path) -> None:
    """Download a DB snapshot into ``store``."""
    logger.info(
        "Downloading the Trivy DBs (over 1 GB) into a temporary directory "
        f"under {tempfile.gettempdir()}, which also holds the pulled images."
    )
    try:
        trivy_db.stage_snapshot(store, workdir)
    except (trivy_db.TrivyDbError, registry.RegistryError, StoreError, OSError) as e:
        raise click.ClickException(f"Could not download the Trivy DBs: {e}") from e


def login(targets: Sequence[LocalTarget]) -> None:
    """Log in to each ECR registry among the images; crane finds other
    credentials in docker's config. A failed login only warns: that
    registry's images then fail on their own, with crane's error."""
    for host in sorted({t.repo.split("/")[0] for t in targets}):
        try:
            registry.login_if_ecr(host, ecr_docker_login)
        except Exception as e:
            logger.warning(f"Could not log in to {host}: {type(e).__name__}: {e}")


def local_scans(
    results_dir: Path, targets: Sequence[LocalTarget], notes: list[str]
) -> list[ImageScan]:
    """Each target's scan. An image with no distro, such as one built FROM
    scratch, keeps its other packages' findings, with a note."""
    scans: list[ImageScan] = []
    for target in targets:
        image, record = target.image(), target.status_record()
        scan = trivy_report.load_scan(results_dir, image, record)
        if scan.error.startswith(trivy_report.NO_OS_PACKAGES):
            logger.warning(f"{target.ref}: {scan.error}; checked its other packages")
            notes.append(
                f"`{target.ref}`: {scan.error}; its other packages were checked."
            )
            scan = trivy_report.load_scan(
                results_dir, image, record, require_os_packages=False
            )
        # The report names each image by the name it was given.
        image = dataclasses.replace(scan.image, name=target.ref)
        scans.append(dataclasses.replace(scan, image=image))
    return scans


def write_outputs(
    output_dir: Path,
    results_dir: Path,
    targets: Sequence[LocalTarget],
    scanner: dict[str, Any],
    issues: Sequence[str],
    architecture: str,
    notes: Sequence[str] = (),
) -> int:
    """Write the report, summary, CSV and annotation; returns the exit code."""
    notes = list(notes)
    scans = local_scans(results_dir, targets, notes)
    note = " ".join(
        [
            f"Scanned each image for linux/{OCI_ARCH[architecture]}.",
            *notes,
            f"Each image's raw Trivy result is in `{results_dir}`.",
        ]
    )
    code = trivy_report.write_report(
        output_dir, scans, scanner, trivy_report.build_info({}), issues, files_note=note
    )
    annotation = output_dir / "annotation.md"
    annotation.write_text(
        trivy_report.render_annotation(scans, scanner, issues=issues, files_note=note),
        encoding="utf-8",
    )
    scanned = sum(s.scanned for s in scans)
    logger.info(
        f"Scanned {scanned}/{len(scans)} images; wrote {output_dir / 'report.md'} "
        f"and {annotation}"
    )
    return code


def _user_path(path: Optional[Path]) -> Optional[Path]:
    """Relative to where `bazel run` was started, not to its runfiles."""
    if path is None:
        return None
    path = path.expanduser()
    if path.is_absolute():
        return path
    return Path(os.environ.get("BUILD_WORKING_DIRECTORY", os.getcwd())) / path


@click.command()
@click.option(
    "--image",
    "images",
    multiple=True,
    help="An image to scan, by name and tag or digest, e.g. "
    "rayproject/ray:2.60.0-py312-cpu. Repeatable. Images are pulled from a "
    "registry, so push a locally built one to a registry first, e.g. one on "
    "localhost. Private ECR images work only in us-west-2.",
)
@click.option(
    "--images-file",
    type=click.Path(dir_okay=False, path_type=Path),
    help="A file of images to scan, one per line; # starts a comment.",
)
@click.option(
    "--architecture",
    default="x86_64",
    show_default=True,
    type=click.Choice(sorted(OCI_ARCH)),
    help="The architecture to scan of a multi-platform image.",
)
@click.option(
    "--output-dir",
    required=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="Where to write report.md, annotation.md, summary.json, vulns.csv "
    "and results/.",
)
@click.option(
    "--ignorefile",
    type=click.Path(dir_okay=False, path_type=Path),
    help="A Trivy YAML ignore file (.yaml or .yml) for every image: your own, "
    "or e.g. ci/ray_ci/automation/trivy/ignore/ray.trivyignore.yaml. "
    "Default: none.",
)
def main(
    images: tuple[str, ...],
    images_file: Optional[Path],
    architecture: str,
    output_dir: Path,
    ignorefile: Optional[Path],
) -> None:
    output_dir = _user_path(output_dir)
    images_file = _user_path(images_file)
    ignorefile = _user_path(ignorefile)
    for path, option in ((images_file, "--images-file"), (ignorefile, "--ignorefile")):
        if path and not path.is_file():
            raise click.BadParameter(f"{path} is not a file", param_hint=option)
    if ignorefile and ignorefile.suffix not in IGNORE_SUFFIXES:
        # The file is staged as .yaml, and Trivy picks its parser by suffix.
        raise click.BadParameter(
            f"{ignorefile} isn't a Trivy YAML ignore file (.yaml or .yml); "
            "the plain .trivyignore format isn't supported",
            param_hint="--ignorefile",
        )
    targets = local_targets(read_images(images, images_file), architecture)
    results_dir = output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    # So no result of an earlier run sits beside this run's record.
    for target in targets:
        for suffix in (".status.json", ".json.gz"):
            (results_dir / f"{target.name}{suffix}").unlink(missing_ok=True)

    # Resolve the images before the DB download, so a mistyped one costs none.
    login(targets)
    for target in targets:
        scan_images.check_target(target, architecture)
    if all(t.error for t in targets):
        for target in targets:
            scan_images.write_status(results_dir, target)
        logger.error("No image could be resolved, so nothing was downloaded.")
        sys.exit(write_outputs(output_dir, results_dir, targets, {}, [], architecture))

    issues: list[str] = []
    with contextlib.ExitStack() as stack:
        root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        # Layouts and unpacked DBs take several GB; keep them beside the DBs.
        scratch = Path(stack.enter_context(tempfile.TemporaryDirectory(dir=root)))
        store = LocalStore(root)
        (scratch / "db").mkdir()
        download_dbs(store, scratch / "db")
        scanner, issue = trivy_report.load_metadata(
            store.path(DB_FOLDER, DB_METADATA), store.url(DB_FOLDER) + DB_METADATA
        )
        issues += [issue] if issue else []
        if not ignorefile:
            ignorefile = scratch / "none.trivyignore.yaml"
            ignorefile.write_text("# No ignores; pass --ignorefile for some.\n")
        (scratch / "scan").mkdir()
        scan_images.scan_group(
            targets,
            architecture,
            store,
            results_dir,
            scratch / "scan",
            BASE_IMAGE,
            # Logged in above already.
            login=lambda: None,
            ignore_files={IMAGE_TYPE: ignorefile},
            stale_after=LOCAL_STALE_AFTER,
        )
    sys.exit(
        write_outputs(output_dir, results_dir, targets, scanner, issues, architecture)
    )


if __name__ == "__main__":
    # Only as a program: importers keep their own logging and signal handling.
    # force, as importing ci.ray_ci.utils already gave the root logger a handler.
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s: %(message)s", force=True
    )
    signal.signal(signal.SIGTERM, scan_images._exit_on_sigterm)
    main()
