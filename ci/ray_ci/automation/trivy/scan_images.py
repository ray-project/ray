"""Scan one (architecture, platform) group of Ray images with Trivy.

For each image the build pushed to its wanda cache, this:
1. checks the ref is a single-platform image for the expected architecture;
2. downloads this build's vulnerability DBs from its S3 folder (staged by
   ``trivy_db``), and checks each against the sha256 pinned in metadata.json
   and each pin against the upstream manifest it names;
3. pulls every image of the group into one shared OCI layout with crane, so
   layers shared across Python versions and image types are stored once;
4. runs the pinned Trivy on each image, by digest, in a throwaway container
   with no network, no credentials, no inherited environment, a non-root
   user, a read-only root and no capabilities;
5. writes ``<name>.status.json`` and ``<name>.json.gz``, the input contract
   of ``trivy_report``, and uploads each image's pair to the build's S3
   ``results/`` folder as soon as it is final, for the report job.

The job talks to the host's docker daemon, so bind mounts would resolve host
paths. Files reach the Trivy containers through docker volumes filled with
``docker cp`` instead.

A finding never fails this tool; an image that couldn't be scanned does,
with exit 11 (incomplete), which rayci doesn't retry: a retry would rescan
the whole group. A failed check or scan fails only its image, after the
others are scanned; a failed DB check or group pull fails every image.

Example:
    bazel run //ci/ray_ci/automation/trivy:scan_images -- \\
        --architecture x86_64 --platform cu12.8.1-cudnn \\
        --image ray:3.11 --image ray:3.12 --image ray-extra:3.12 \\
        --results-dir trivy-results
"""

import gzip
import json
import logging
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

import click

from ci.ray_ci.automation.push_ray_image import RayImagePushContext
from ci.ray_ci.automation.trivy import registry, scan_store
from ci.ray_ci.automation.trivy.scan_store import (
    DB_FOLDER,
    RESULTS_FOLDER,
    ScanStore,
    StoreError,
)
from ci.ray_ci.automation.trivy.trivy_db import (
    DB_METADATA,
    DB_SOURCES,
    DbSource,
    db_layer,
    manifest_created,
)
from ci.ray_ci.automation.trivy.trivy_report import EXIT_INCOMPLETE, resolve_path
from ci.ray_ci.docker_container import RayType
from ci.ray_ci.ray_image import RayImageError
from ci.ray_ci.utils import ecr_docker_login

logger = logging.getLogger(__name__)

# Trivy is a static binary; this image only provides an empty, non-root root.
# It pins a multi-arch index, but the Trivy binary staged into it is linux
# x86_64, so scan jobs must run on x86_64 hosts.
BASE_IMAGE = (
    "gcr.io/distroless/static-debian12"
    "@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab"
)
TRIVY_RUNFILE = "trivy_linux_x86_64/trivy"
IGNORE_RUNFILE = "io_ray/ci/ray_ci/automation/trivy/ignore/{}.trivyignore.yaml"
SCAN_USER = "65534:65534"
# Everything the scan creates on the docker host carries this label.
LABEL = "ray-trivy-scan=1"
# Leftovers older than this come from jobs that died without cleaning up:
# every scan step times out after 60 minutes (image-scan.rayci.yml), so
# younger ones may belong to a job still running on the same host.
STALE_AFTER = timedelta(hours=2)
SCAN_TIMEOUT = "30m"
# Seconds a docker call may take: a scan gets a margin over Trivy's own
# SCAN_TIMEOUT, copying the layout and DBs into the volumes can take a
# while, and everything else is quick.
DOCKER_RUN_TIMEOUT = 35 * 60
DOCKER_CP_TIMEOUT = 30 * 60
DOCKER_TIMEOUT = 2 * 60
# Single-platform manifests; an index would let Trivy silently pick one.
IMAGE_MANIFEST_TYPES = frozenset(
    {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }
)
OCI_ARCH = {"x86_64": "amd64", "aarch64": "arm64"}
# Nightly tags embed the publish date (nightly.<yymmdd>.<sha>), which a scan
# can't know for sure: it may run after midnight. The digest identifies the
# image; these tags are left out of status records.
DATED_TAG = re.compile(r"^nightly\.\d{6}\.")
DB_FILES = {
    "db": {"trivy.db", "metadata.json"},
    "java_db": {"trivy-java.db", "metadata.json"},
}


class ScanError(Exception):
    """An image, or the whole group, could not be scanned."""


@dataclass
class Target:
    """One image to scan, and what is known about it so far."""

    ctx: RayImagePushContext
    ref: str
    # Whether this build's publish step pushes the image to Docker Hub.
    published: bool = False
    digest: str = ""
    error: str = ""
    db: dict[str, Any] = field(default_factory=dict)
    # Set only once the result file is written.
    scanned: bool = False
    # Set once the final status is written and handed to the reporter.
    reported: bool = False

    @property
    def name(self) -> str:
        return self.ctx.wanda_image_name()

    @property
    def image_type(self) -> str:
        return self.ctx.ray_type.value

    @property
    def repo(self) -> str:
        return self.ref.rsplit(":", 1)[0]

    @property
    def pinned_ref(self) -> str:
        return f"{self.repo}@{self.digest}"

    def status_record(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "image_type": self.image_type,
            "python_version": self.ctx.python_version,
            "platform": self.ctx.platform,
            "architecture": self.ctx.architecture,
            "ref": self.ref,
            "digest": self.digest,
            # The tags the publish step pushed this image under, none when it
            # only did a dry run. Nightly tags of the two architectures are
            # later joined into multi-arch indexes, so such a tag can name an
            # index holding this image rather than the image itself.
            "published_tags": [
                f"{self.ctx.docker_hub_repo}:{tag}"
                for tag in self.ctx.destination_tags()
                if self.published and not DATED_TAG.match(tag)
            ],
            "status": "scanned" if self.scanned and not self.error else "error",
            "error": self.error,
            "trivy_db": self.db,
        }


def parse_image(spec: str) -> tuple[str, str]:
    image_type, sep, python_version = spec.partition(":")
    if not sep or not image_type or not python_version:
        raise click.BadParameter(f"{spec!r} is not TYPE:PYTHON, e.g. ray:3.12")
    return image_type, python_version


def publishes(branch: str, rayci_schedule: str) -> bool:
    """Whether the build's publish step pushes to Docker Hub, as
    push_ray_image._should_upload decides. Its other condition, a postmerge
    pipeline, holds for every scan job."""
    return branch.startswith("releases/") or (
        branch == "master" and rayci_schedule == "nightly"
    )


def build_targets(
    images: Sequence[tuple[str, str]],
    platform: str,
    architecture: str,
    work_repo: str,
    build_id: str,
    branch: str,
    commit: str,
    rayci_schedule: str,
    pull_request: str,
) -> list[Target]:
    targets: list[Target] = []
    for image_type, python_version in images:
        try:
            ctx = RayImagePushContext(
                ray_type=RayType(image_type),
                python_version=python_version,
                platform=platform,
                architecture=architecture,
                branch=branch,
                commit=commit,
                rayci_schedule=rayci_schedule,
                rayci_build_id=build_id,
                pull_request=pull_request,
            )
            ctx.ray_image.validate()
        except (ValueError, RayImageError) as e:
            # ValueError: an image type RayType doesn't know.
            raise click.BadParameter(str(e)) from e
        target = Target(
            ctx=ctx,
            ref=f"{work_repo}:{ctx.wanda_tag}",
            published=publishes(branch, rayci_schedule),
        )
        if any(t.name == target.name for t in targets):
            raise click.BadParameter(f"{image_type}:{python_version} is given twice")
        targets.append(target)
    return targets


def _describe(e: BaseException) -> str:
    """Expected failures read as their message; anything else names its type."""
    if isinstance(e, (ScanError, StoreError, registry.RegistryError)):
        return str(e)
    return f"{type(e).__name__}: {e}"


def check_target(target: Target, architecture: str) -> None:
    """Record the digest, or why this ref must not be scanned. The checks
    run on the digest itself, since the tag can be re-pushed meanwhile."""
    try:
        digest = registry.digest(target.ref)
        pinned = f"{target.repo}@{digest}"
        manifest = registry.manifest(pinned)
        if manifest.get("mediaType") not in IMAGE_MANIFEST_TYPES:
            target.error = (
                f"{pinned} is {manifest.get('mediaType')}, not a "
                "single-platform image manifest"
            )
            return
        found = registry.config(pinned).get("architecture")
        if found != OCI_ARCH[architecture]:
            target.error = f"{pinned} is {found}, expected {OCI_ARCH[architecture]}"
            return
        target.digest = digest
    except Exception as e:
        target.error = _describe(e)


def check_layout(layout: Path, digests: set[str]) -> None:
    """Every digest to scan must be in the layout crane wrote."""
    index = json.loads((layout / "index.json").read_text())
    found = {m.get("digest") for m in index.get("manifests") or []}
    missing = digests - found
    if missing:
        raise ScanError(
            f"layout lacks {sorted(missing)}; it has {sorted(found, key=str)}"
        )


def _safe_extract(tarball: Path, dest: Path, allowed: set[str]) -> None:
    """Extract only the expected top-level DB files, in one streaming pass:
    listing the members first would re-inflate the gzip stream."""
    dest.mkdir(parents=True)
    with tarfile.open(tarball, "r|gz") as tar:
        for member in tar:
            if not member.isfile() or member.name not in allowed:
                raise ScanError(f"unexpected entry {member.name!r} in {tarball.name}")
            with tar.extractfile(member) as src, open(dest / member.name, "wb") as out:
                shutil.copyfileobj(src, out)
    missing = allowed - {p.name for p in dest.iterdir()}
    if missing:
        raise ScanError(f"{tarball.name} lacks {sorted(missing)}")


def load_db_pins(store: ScanStore, workdir: Path) -> dict[str, Any]:
    """The DB pins trivy_db recorded for this build."""
    path = store.download(DB_FOLDER, DB_METADATA, workdir / DB_METADATA)
    try:
        pins = json.loads(path.read_text())
    except ValueError as e:
        raise ScanError(f"{store.url(DB_FOLDER)}{DB_METADATA} is not JSON: {e}")
    if not isinstance(pins, dict):
        raise ScanError(f"{store.url(DB_FOLDER)}{DB_METADATA} is not a JSON object")
    return pins


def fetch_db(
    source: DbSource, store: ScanStore, pins: dict[str, Any], workdir: Path
) -> tuple[Path, dict[str, Any]]:
    """Download this build's copy of a DB and verify it against its pin, and
    the pin against the upstream manifest it names: anyone who can write the
    bucket could replace both the tarball and the pin."""
    pin = pins.get(source.name)
    pin = pin if isinstance(pin, dict) else {}
    blob, manifest_digest = pin.get("blob"), pin.get("manifest")
    if not all(
        isinstance(d, str) and registry.SHA256_DIGEST.fullmatch(d)
        for d in (blob, manifest_digest)
    ):
        raise ScanError(f"{DB_METADATA} has no valid {source.name} pin: {pin!r}")
    # crane refuses a manifest whose digest differs from the one requested.
    pinned = f"{source.upstream_repo}@{manifest_digest}"
    manifest = registry.manifest(pinned)
    upstream = db_layer(manifest, source.media_type)["digest"]
    if upstream != blob:
        raise ScanError(
            f"{DB_METADATA} pins {source.name} blob {blob}, but {pinned} has {upstream}"
        )
    tarball = store.download(DB_FOLDER, source.file_name, workdir / source.file_name)
    actual = registry.sha256_file(tarball)
    if actual != blob:
        raise ScanError(f"{source.file_name} has digest {actual}, pinned {blob}")
    dest = workdir / source.name
    _safe_extract(tarball, dest, DB_FILES[source.name])
    tarball.unlink()
    # Recorded from what was verified, never from the pin.
    return dest, {
        "source": source.upstream,
        "manifest": manifest_digest,
        "blob": blob,
        "created": manifest_created(manifest),
    }


def _run(args: Sequence[str], check: bool = True, timeout: int = DOCKER_TIMEOUT) -> str:
    """Run a docker command. A timeout, or a docker that can't run, is a
    ScanError even with ``check=False``."""
    what = " ".join(args[:3])
    try:
        result = subprocess.run(
            list(args),
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise ScanError(f"{what} timed out after {timeout}s") from None
    except OSError as e:
        raise ScanError(f"{what} could not run: {e}") from e
    if check and result.returncode != 0:
        raise ScanError(f"{what} failed: {result.stderr.strip()[-2000:]}")
    return result.stdout


def trivy_args(image_type: str, digest: str, name: str) -> list[str]:
    """Trivy's arguments. Every setting is a flag: Trivy rejects an unknown
    flag but silently ignores an unknown config key."""
    return [
        "/data/trivy",
        "image",
        "--config",
        "/data/trivy.yaml",
        "--input",
        f"/data/layout@{digest}",
        "--cache-dir",
        "/work/cache",
        "--skip-db-update",
        "--skip-java-db-update",
        "--offline-scan",
        "--skip-version-check",
        "--disable-telemetry",
        "--scanners",
        "vuln",
        "--pkg-types",
        "os,library",
        "--list-all-pkgs",
        "--ignorefile",
        f"/data/ignore/{image_type}.trivyignore.yaml",
        "--show-suppressed",
        "--exit-code",
        "0",
        "--no-progress",
        "--quiet",
        "--timeout",
        SCAN_TIMEOUT,
        "--format",
        "json",
        "--output",
        f"/work/out/{name}.json",
    ]


class ContainedTrivy:
    """Runs Trivy in throwaway containers that can't reach the network.

    Volume ``data`` (mounted read-only) holds the Trivy binary, the OCI
    layout, the ignore files and the DBs. Volume ``work`` holds Trivy's layer
    cache, its temp dir and the results. Nothing else is visible to Trivy.
    """

    def __init__(self, base_image: str) -> None:
        self.base_image = base_image
        self.prefix = f"trivy-scan-{uuid.uuid4().hex[:12]}"
        self.data = f"{self.prefix}-data"
        self.work = f"{self.prefix}-work"
        self.loader = f"{self.prefix}-loader"
        self.scans: list[str] = []

    def __enter__(self) -> "ContainedTrivy":
        _prune_stale()
        try:
            _run(["docker", "volume", "create", "--label", LABEL, self.data])
            _run(["docker", "volume", "create", "--label", LABEL, self.work])
            _run(
                [
                    "docker",
                    "create",
                    "--name",
                    self.loader,
                    "--label",
                    LABEL,
                    "-v",
                    f"{self.data}:/data",
                    "-v",
                    f"{self.work}:/work",
                    self.base_image,
                    # Never started; distroless has no default command to fall back on.
                    "/data/trivy",
                ]
            )
        except BaseException:
            # __exit__ doesn't run when __enter__ raises.
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *exc: object) -> None:
        # rm -f also stops a scan container left running by an interrupted
        # `docker run`; killing the CLI doesn't stop the container.
        try:
            _run(["docker", "rm", "-f", self.loader, *self.scans], check=False)
        finally:
            # Even if that failed, or the job is being stopped.
            _run(["docker", "volume", "rm", "-f", self.data, self.work], check=False)

    def load(self, staging: Path) -> None:
        """Copy ``staging/data`` and ``staging/work`` into the volumes."""
        for volume in ("data", "work"):
            _run(
                ["docker", "cp", f"{staging / volume}/.", f"{self.loader}:/{volume}"],
                timeout=DOCKER_CP_TIMEOUT,
            )

    def docker_run_args(self, target: Target, name: str) -> list[str]:
        return [
            "docker",
            "run",
            "--rm",
            "--name",
            name,
            "--label",
            LABEL,
            "--network",
            "none",
            "--user",
            SCAN_USER,
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--workdir",
            "/work/tmp",
            "-e",
            "HOME=/work/home",
            "-e",
            "XDG_CACHE_HOME=/work/home/.cache",
            "-e",
            "XDG_DATA_HOME=/work/home/.local",
            "-e",
            "TMPDIR=/work/tmp",
            "-v",
            f"{self.data}:/data:ro",
            "-v",
            f"{self.work}:/work",
            self.base_image,
            *trivy_args(target.image_type, target.digest, target.name),
        ]

    def scan(self, target: Target, dest: Path) -> None:
        """Scan one image and copy its JSON result to ``dest``."""
        logger.info(f"Scanning {target.name} ({target.digest})")
        name = f"{self.prefix}-scan-{len(self.scans)}"
        self.scans.append(name)
        _run(self.docker_run_args(target, name), timeout=DOCKER_RUN_TIMEOUT)
        _run(["docker", "cp", f"{self.loader}:/work/out/{target.name}.json", str(dest)])


def _created(kind: str, name: str) -> Optional[datetime]:
    """When a container or volume was created, as naive UTC."""
    field_name = "{{.CreatedAt}}" if kind == "volume" else "{{.Created}}"
    out = _run(["docker", kind, "inspect", "--format", field_name, name], check=False)
    # Containers report UTC ("...Z", nanoseconds); volumes report local time
    # with an offset. Drop the fraction, which fromisoformat may not parse.
    match = re.match(
        r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.\d+)?(Z|[+-]\d\d:\d\d)?",
        out.strip(),
    )
    if not match:
        return None
    offset = match.group(2) if match.group(2) not in (None, "Z") else "+00:00"
    created = datetime.fromisoformat(match.group(1) + offset)
    return created.astimezone(timezone.utc).replace(tzinfo=None)


def _prune_stale(now: Optional[datetime] = None) -> None:
    """Remove containers and volumes that earlier jobs left behind, e.g.
    after being killed. Containers go first, so their volumes are free."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    for kind, listing, remove in (
        ("container", ["docker", "ps", "-a"], ["docker", "rm", "-f"]),
        ("volume", ["docker", "volume", "ls"], ["docker", "volume", "rm", "-f"]),
    ):
        names = _run(
            [
                *listing,
                "--filter",
                f"label={LABEL}",
                "--format",
                "{{.Name}}" if kind == "volume" else "{{.Names}}",
            ],
            check=False,
        ).split()
        for name in names:
            created = _created(kind, name)
            if created is None:
                logger.warning(f"Can't tell when {kind} {name} was created; left")
            elif now - created > STALE_AFTER:
                logger.info(f"Removing stale {kind} {name} (created {created})")
                _run([*remove, name], check=False)


def _exit_on_sigterm(signum: int, frame: object) -> None:
    """Cancelled and timed-out jobs get SIGTERM. Exiting normally lets the
    cleanup and status writing run, which the default handler would skip."""
    sys.exit(128 + signum)


def _make_world_readable(root: Path) -> None:
    for path in [root, *root.rglob("*")]:
        if path.is_symlink():
            continue
        if path.is_dir() or path.stat().st_mode & 0o100:
            path.chmod(0o755)
        else:
            path.chmod(0o644)


def stage(
    workdir: Path, layout: Path, dbs: dict[str, Path], image_types: set[str]
) -> Path:
    """Lay out the files the volumes start with. Runfiles are symlinks, so
    everything is copied for real; docker cp would copy the links."""
    staging = workdir / "staging"
    data = staging / "data"
    (data / "ignore").mkdir(parents=True)
    shutil.copy(registry.runfile(TRIVY_RUNFILE), data / "trivy")
    (data / "trivy").chmod(0o755)
    for image_type in sorted(image_types):
        src = registry.runfile(IGNORE_RUNFILE.format(image_type))
        if not src.is_file():
            raise ScanError(f"no ignore file for {image_type}: {src}")
        shutil.copy(src, data / "ignore" / src.name)
    # An empty config file stops Trivy looking for trivy.yaml elsewhere.
    (data / "trivy.yaml").write_text("")
    shutil.move(str(layout), data / "layout")
    for name, path in dbs.items():
        shutil.move(str(path), data / name.replace("_", "-"))

    # crane writes blobs 0600, and docker cp keeps the job's owner, which isn't
    # the scan user, so the scan user can only read what is world-readable.
    _make_world_readable(data)

    work = staging / "work"
    for sub in ("cache", "out", "tmp", "home"):
        (work / sub).mkdir(parents=True)
        # docker cp keeps the job's owner; the scan runs as nobody.
        (work / sub).chmod(0o1777)
    # Trivy reads its DBs from the cache dir; point it at the read-only copies.
    for name in dbs:
        db_dir = name.replace("_", "-")
        (work / "cache" / db_dir).symlink_to(f"/data/{db_dir}")
    return staging


def write_status(results_dir: Path, target: Target) -> None:
    path = results_dir / f"{target.name}.status.json"
    path.write_text(json.dumps(target.status_record(), indent=2) + "\n")


def _finish(
    results_dir: Path, target: Target, report: Callable[[Target], None]
) -> None:
    write_status(results_dir, target)
    report(target)
    target.reported = True


def scan_group(
    targets: Sequence[Target],
    architecture: str,
    store: ScanStore,
    results_dir: Path,
    workdir: Path,
    base_image: str,
    login: Callable[[], None] = lambda: None,
    report: Callable[[Target], None] = lambda target: None,
) -> None:
    """Scan every target. Each one ends with a status record either way, and
    any failure, expected or not, becomes the affected images' error.

    ``report`` gets each target as soon as its status is final, so a job
    killed midway has already handed over the images it finished."""
    try:
        login()
        for target in targets:
            check_target(target, architecture)
            if target.error:
                _finish(results_dir, target, report)
        ready = [t for t in targets if not t.error]
        if ready:
            _scan_ready(
                ready, architecture, store, results_dir, workdir, base_image, report
            )
    except Exception as e:
        # Logged even when every image is already final, e.g. a failed cleanup.
        logger.error(f"Scanning the group failed: {_describe(e)}")
        for target in targets:
            if not target.error and not target.scanned:
                target.error = f"group failed before scanning: {_describe(e)}"
    finally:
        _finish_unreported(targets, results_dir, report)


def _finish_unreported(
    targets: Sequence[Target], results_dir: Path, report: Callable[[Target], None]
) -> None:
    """Give every target not yet reported its final status."""
    for target in targets:
        if target.reported:
            continue
        if not target.error and not target.scanned:
            target.error = "scan did not complete"
        try:
            _finish(results_dir, target, report)
        except Exception as e:
            # The others' statuses still matter.
            target.error = target.error or "status not recorded"
            logger.error(f"Could not record {target.name}: {_describe(e)}")


def _scan_ready(
    ready: Sequence[Target],
    architecture: str,
    store: ScanStore,
    results_dir: Path,
    workdir: Path,
    base_image: str,
    report: Callable[[Target], None],
) -> None:
    dbs: dict[str, Path] = {}
    pins: dict[str, Any] = {}
    db_pins = load_db_pins(store, workdir)
    for source in DB_SOURCES:
        dbs[source.name], pins[source.name] = fetch_db(source, store, db_pins, workdir)
    layout = workdir / "layout"
    platform = f"linux/{OCI_ARCH[architecture]}"
    registry.pull(sorted({t.pinned_ref for t in ready}), layout, platform)
    check_layout(layout, {t.digest for t in ready})
    staging = stage(workdir, layout, dbs, {t.image_type for t in ready})
    with ContainedTrivy(base_image) as trivy:
        trivy.load(staging)
        shutil.rmtree(staging)
        try:
            for target in ready:
                _scan_one(trivy, target, pins, results_dir, workdir)
                try:
                    _finish(results_dir, target, report)
                except Exception as e:
                    # Retried, and logged as an error if it fails again, at
                    # the end; the other images still get scanned.
                    logger.warning(f"Could not record {target.name}: {_describe(e)}")
        except BaseException:
            # A cancelled job may be killed before the cleanup below ends,
            # so hand over the unfinished images' statuses first.
            _finish_unreported(ready, results_dir, report)
            raise


def _scan_one(
    trivy: ContainedTrivy,
    target: Target,
    pins: dict[str, Any],
    results_dir: Path,
    workdir: Path,
) -> None:
    """Scan one image into ``results_dir``, or record why it failed."""
    target.db = pins
    raw = workdir / f"{target.name}.json"
    result = results_dir / f"{target.name}.json.gz"
    try:
        trivy.scan(target, raw)
        with open(raw, "rb") as src, gzip.open(result, "wb") as out:
            shutil.copyfileobj(src, out)
        raw.unlink()
        target.scanned = True
    except Exception as e:
        target.error = _describe(e)
        # A partial result must not be uploaded beside the error.
        result.unlink(missing_ok=True)


def upload_result(store: ScanStore, results_dir: Path, target: Target) -> str:
    """Upload the image's result, then its status record, so a status found
    in S3 always has its result beside it. Returns the failure, if any."""
    for name in (f"{target.name}.json.gz", f"{target.name}.status.json"):
        path = results_dir / name
        if not path.is_file():
            continue
        try:
            store.upload(path, RESULTS_FOLDER)
        except Exception as e:
            return f"{target.name}: {_describe(e)}"
    return ""


@click.command()
@click.option("--architecture", required=True, type=click.Choice(sorted(OCI_ARCH)))
@click.option("--platform", required=True, help="e.g. cpu, tpu, cu12.8.1-cudnn")
@click.option(
    "--image",
    "images",
    required=True,
    multiple=True,
    help="TYPE:PYTHON to scan, e.g. ray:3.12. Repeatable.",
)
@click.option(
    "--results-dir",
    default="/artifact-mount/trivy/results",
    type=click.Path(file_okay=False, path_type=Path),
)
@click.option("--rayci-work-repo", required=True, envvar="RAYCI_WORK_REPO")
@click.option("--rayci-build-id", required=True, envvar="RAYCI_BUILD_ID")
@click.option("--rayci-schedule", default="", envvar="RAYCI_SCHEDULE")
@click.option("--pull-request", default="false", envvar="BUILDKITE_PULL_REQUEST")
@scan_store.build_options
def main(
    architecture: str,
    platform: str,
    images: tuple[str, ...],
    results_dir: Path,
    rayci_work_repo: str,
    rayci_build_id: str,
    rayci_schedule: str,
    pull_request: str,
    store: ScanStore,
) -> None:
    results_dir = resolve_path(results_dir)
    targets = build_targets(
        [parse_image(spec) for spec in images],
        platform,
        architecture,
        rayci_work_repo,
        rayci_build_id,
        store.branch,
        store.commit,
        rayci_schedule,
        pull_request,
    )

    upload_failures: list[str] = []

    def report(target: Target) -> None:
        failure = upload_result(store, results_dir, target)
        if failure:
            upload_failures.append(failure)

    results_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        scan_group(
            targets,
            architecture,
            store,
            results_dir,
            Path(tmp),
            BASE_IMAGE,
            login=lambda: registry.login_if_ecr(rayci_work_repo, ecr_docker_login),
            report=report,
        )
    failed = [t for t in targets if t.error]
    for target in failed:
        logger.error(f"{target.name}: {target.error}")
    for failure in upload_failures:
        logger.error(f"Upload failed: {failure}")
    logger.info(
        f"Scanned {len(targets) - len(failed)}/{len(targets)} images; "
        f"results in {store.url(RESULTS_FOLDER)}"
    )
    sys.exit(EXIT_INCOMPLETE if failed or upload_failures else 0)


if __name__ == "__main__":
    # Only as a program: importers keep their own logging and signal handling.
    # force, as importing ci.ray_ci.utils already gave the root logger a handler.
    logging.basicConfig(
        level=logging.INFO, format="%(message)s", stream=sys.stdout, force=True
    )
    signal.signal(signal.SIGTERM, _exit_on_sigterm)
    main()
