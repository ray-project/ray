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

The DBs are over 1 GB. --work-dir keeps them, so later runs with the same
--work-dir reuse them instead of downloading them again; runs may share it.
A reused DB grows stale; --refresh-db downloads fresh ones.

crane uses docker's credentials. For a private ECR registry,
local_scan_images logs in itself with your AWS credentials, but only to registries in us-west-2.

Exit codes are trivy_report's: 0 clean, 10 fixable CRITICAL/HIGH findings,
11 incomplete coverage. A failed DB download exits 1, bad options 2.

Example:
    bazel run //ci/ray_ci/automation/trivy:local_scan_images -- \\
        --image rayproject/ray:2.60.0-py312-cpu --image ubuntu:22.04 \\
        --work-dir ~/.cache/ray-trivy --output-dir /tmp/trivy-report
"""

import contextlib
import dataclasses
import fcntl
import json
import logging
import os
import re
import shutil
import signal
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional, Sequence

import click

from ci.ray_ci.automation.trivy import registry, scan_images, trivy_db, trivy_report
from ci.ray_ci.automation.trivy.scan_images import (
    BASE_IMAGE,
    GROUP_FAILED,
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
STALE_DB_NOTE = (
    "A reused DB grows stale, so pass --refresh-db when you want fresh ones."
)
# A reused DB older than this gets a warning: it lacks newer advisories.
STALE_DB_DAYS = 7
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


def db_age_days(store: LocalStore) -> Optional[int]:
    """How many days ago the kept DB was built, or None if unknown."""
    try:
        metadata = json.loads(
            store.path(DB_FOLDER, DB_METADATA).read_text(encoding="utf-8")
        )
        created = date.fromisoformat(metadata["db"]["created"][:10])
    except (OSError, ValueError, KeyError, TypeError):
        return None
    return (date.today() - created).days


def _log_reuse(store: LocalStore) -> None:
    days = db_age_days(store)
    if days is None:
        age = "DB date unknown"
    else:
        age = f"DB created {date.today() - timedelta(days=days)}, "
        age += f"{days} day{'' if days == 1 else 's'} ago"
    if days is not None and days > STALE_DB_DAYS:
        logger.warning(
            f"Reusing the Trivy DBs in {store.root} ({age}): CVEs published "
            "since then are missing. Pass --refresh-db to download fresh ones."
        )
    else:
        logger.info(
            f"Reusing the Trivy DBs in {store.root} ({age}); "
            "pass --refresh-db to download fresh ones."
        )


class WorkDirLock:
    """Lets runs share a --work-dir. A run downloads DBs holding the lock
    exclusively, and scans holding it shared, so a download waits for the
    runs reading the kept DBs, and they wait for it. (flock switches modes
    by dropping one lock, then taking the other.)"""

    def __init__(self, work_dir: Path) -> None:
        self.work_dir = work_dir
        self.file = open(work_dir / ".lock", "a")

    def exclusive(self) -> None:
        self._take(fcntl.LOCK_EX)

    def shared(self) -> None:
        self._take(fcntl.LOCK_SH)

    def _take(self, mode: int) -> None:
        try:
            fcntl.flock(self.file, mode | fcntl.LOCK_NB)
        except BlockingIOError:
            logger.info(
                f"Waiting for another local_scan_images run using {self.work_dir}"
            )
            fcntl.flock(self.file, mode)

    def close(self) -> None:
        self.file.close()


def _swap_in(new: Path, dest: Path) -> None:
    """Put a complete download in place of the kept DBs, so a failed one
    never leaves them half replaced."""
    old = dest.with_name(f"{dest.name}.old")
    shutil.rmtree(old, ignore_errors=True)
    if dest.exists():
        dest.rename(old)
    new.rename(dest)
    shutil.rmtree(old, ignore_errors=True)


def prepare_dbs(
    store: LocalStore,
    workdir: Path,
    kept: bool,
    refresh: bool,
    lock: Optional[WorkDirLock] = None,
) -> bool:
    """Reuse the DB snapshot a kept --work-dir holds, or download one,
    warning about the download unless it was asked for. Returns whether it
    reused kept DBs. With ``lock``, it returns holding it shared."""

    def reusable() -> bool:
        return kept and not refresh and store.exists(DB_FOLDER, DB_METADATA)

    if lock:
        lock.shared()
        if not reusable():
            lock.exclusive()
    # Under the exclusive lock, another run may have just downloaded them.
    if reusable():
        _log_reuse(store)
        if lock:
            lock.shared()
        return True
    if refresh:
        logger.info(
            f"Downloading fresh Trivy DBs into {store.root} (--refresh-db); "
            "the kept ones stay until the new ones are complete."
        )
    elif kept:
        logger.warning(
            f"{store.root} has no Trivy DBs yet, so this run downloads them "
            f"(over 1 GB). Later runs with --work-dir {store.root} reuse them, "
            f"saving bandwidth and disk. {STALE_DB_NOTE}"
        )
    else:
        logger.warning(
            "Downloading the Trivy DBs (over 1 GB) for this run only, into a "
            f"temporary directory under {tempfile.gettempdir()} that also holds "
            "the pulled images. Pass --work-dir DIR to keep them: later runs "
            "with the same --work-dir reuse them, saving bandwidth and disk. "
            f"{STALE_DB_NOTE}"
        )
    fresh = LocalStore(workdir / "fresh")
    try:
        trivy_db.stage_snapshot(fresh, workdir)
    except (trivy_db.TrivyDbError, registry.RegistryError, StoreError, OSError) as e:
        raise click.ClickException(f"Could not download the Trivy DBs: {e}") from e
    _swap_in(fresh.root / DB_FOLDER, store.root / DB_FOLDER)
    if lock:
        lock.shared()
    return False


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
    "--work-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Keeps the Trivy DBs for later runs to reuse, and holds the scan's "
    "temporary files. Without it, each run downloads the DBs (over 1 GB) into "
    "the system temp directory ($TMPDIR), which also holds the pulled images.",
)
@click.option(
    "--refresh-db",
    is_flag=True,
    help="Download fresh DBs into --work-dir, replacing the ones it keeps.",
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
    work_dir: Optional[Path],
    refresh_db: bool,
    ignorefile: Optional[Path],
) -> None:
    output_dir = _user_path(output_dir)
    work_dir = _user_path(work_dir)
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
    if refresh_db and not work_dir:
        raise click.UsageError(
            "--refresh-db needs --work-dir: without it, every run downloads "
            "fresh DBs."
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
    notes: list[str] = []
    with contextlib.ExitStack() as stack:
        lock = None
        if work_dir:
            work_dir.mkdir(parents=True, exist_ok=True)
            root = work_dir
            lock = WorkDirLock(work_dir)
            stack.callback(lock.close)
        else:
            root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        # Layouts and unpacked DBs take several GB; keep them beside the DBs.
        scratch = Path(stack.enter_context(tempfile.TemporaryDirectory(dir=root)))
        store = LocalStore(root)
        (scratch / "db").mkdir()
        reused = prepare_dbs(
            store, scratch / "db", work_dir is not None, refresh_db, lock
        )
        scanner, issue = trivy_report.load_metadata(
            store.path(DB_FOLDER, DB_METADATA), store.url(DB_FOLDER) + DB_METADATA
        )
        issues += [issue] if issue else []
        days = db_age_days(store) if reused else None
        if days is not None and days > STALE_DB_DAYS:
            notes.append(
                f"The reused DB was {days} days old, so CVEs published since "
                "then are missing; --refresh-db downloads fresh ones."
            )
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
    if reused and all(t.error.startswith(GROUP_FAILED) for t in targets):
        hint = (
            f"every image failed before scanning while reusing the DBs in "
            f"{root}; if the error is about the DBs, pass --refresh-db to "
            "download fresh ones"
        )
        logger.warning(hint)
        issues.append(hint)
    sys.exit(
        write_outputs(
            output_dir, results_dir, targets, scanner, issues, architecture, notes
        )
    )


if __name__ == "__main__":
    # Only as a program: importers keep their own logging and signal handling.
    # force, as importing ci.ray_ci.utils already gave the root logger a handler.
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s: %(message)s", force=True
    )
    signal.signal(signal.SIGTERM, scan_images._exit_on_sigterm)
    main()
