"""Name and check the Ray images one Trivy scan job scans, and run Trivy.

For one (architecture, platform) group, ``build_targets`` names each image
the build pushed to its wanda cache, with the tags the publish step gives it.
``check_target`` resolves each ref to a digest and checks, on the digest
itself since a tag can be re-pushed meanwhile, that it is a single-platform
image for the expected architecture.

``ContainedTrivy`` runs the pinned Trivy on an OCI layout in a throwaway
container with no network, no credentials, no inherited environment, a
non-root user, a read-only root and no capabilities. The job talks to the
host's docker daemon, so bind mounts would resolve host paths. Files reach
the Trivy containers through docker volumes filled with ``docker cp``
instead.
"""

import logging
import re
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

import click
import runfiles

from ci.ray_ci.automation.push_ray_image import RayImagePushContext
from ci.ray_ci.automation.trivy import registry
from ci.ray_ci.automation.trivy.scan_store import StoreError
from ci.ray_ci.docker_container import RayType
from ci.ray_ci.ray_image import RayImageError

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)

# Trivy is a static binary; this image only provides an empty, non-root root.
BASE_IMAGE = (
    "gcr.io/distroless/static-debian12"
    "@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab"
)
TRIVY_RUNFILE = "trivy_linux_x86_64/trivy"
IGNORE_RUNFILE = "io_ray/ci/ray_ci/automation/trivy/ignore/{}.trivyignore.yaml"
SCAN_USER = "65534:65534"
# Everything the scan creates on the docker host carries this label.
LABEL = "ray-trivy-scan=1"
# Leftovers older than this come from jobs that died without cleaning up;
# younger ones may belong to a job still running on the same host.
STALE_AFTER = timedelta(hours=6)
SCAN_TIMEOUT = "30m"
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


class ScanError(Exception):
    """An image, or the whole group, could not be scanned."""


@dataclass
class Target:
    """One image to scan, and what is known about it so far."""

    ctx: RayImagePushContext
    ref: str
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
            "published_tags": [
                f"{self.ctx.docker_hub_repo}:{tag}"
                for tag in self.ctx.destination_tags()
                if not DATED_TAG.match(tag)
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
    targets = []
    for image_type, python_version in images:
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
        try:
            ctx.ray_image.validate()
        except RayImageError as e:
            raise click.BadParameter(str(e)) from e
        targets.append(Target(ctx=ctx, ref=f"{work_repo}:{ctx.wanda_tag}"))
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


def _run(args: Sequence[str], check: bool = True) -> str:
    result = subprocess.run(list(args), capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        raise ScanError(f"{' '.join(args[:3])} failed: {result.stderr.strip()[-2000:]}")
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
        _run(["docker", "rm", "-f", self.loader, *self.scans], check=False)
        _run(["docker", "volume", "rm", "-f", self.data, self.work], check=False)

    def load(self, staging: Path) -> None:
        """Copy ``staging/data`` and ``staging/work`` into the volumes."""
        _run(["docker", "cp", f"{staging / 'data'}/.", f"{self.loader}:/data"])
        _run(["docker", "cp", f"{staging / 'work'}/.", f"{self.loader}:/work"])

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
        _run(self.docker_run_args(target, name))
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
            if created and now - created > STALE_AFTER:
                logger.info(f"Removing stale {kind} {name} (created {created})")
                _run([*remove, name], check=False)


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
    r = runfiles.Create()
    shutil.copy(r.Rlocation(TRIVY_RUNFILE), data / "trivy")
    (data / "trivy").chmod(0o755)
    for image_type in sorted(image_types):
        src = Path(r.Rlocation(IGNORE_RUNFILE.format(image_type)))
        if not src.is_file():
            raise ScanError(f"no ignore file for {image_type}: {src}")
        shutil.copy(src, data / "ignore" / src.name)
    # An empty config file stops Trivy looking for trivy.yaml elsewhere.
    (data / "trivy.yaml").write_text("")
    shutil.move(str(layout), data / "layout")
    for name, path in dbs.items():
        shutil.move(str(path), data / name.replace("_", "-"))

    # crane writes blobs 0600 and docker cp makes root the owner, so the
    # non-root scan user can only read what is world-readable.
    _make_world_readable(data)

    work = staging / "work"
    for sub in ("cache", "out", "tmp", "home"):
        (work / sub).mkdir(parents=True)
        # docker cp makes root the owner; the scan runs as nobody.
        (work / sub).chmod(0o1777)
    # Trivy reads its DBs from the cache dir; point it at the read-only copies.
    (work / "cache" / "db").symlink_to("/data/db")
    (work / "cache" / "java-db").symlink_to("/data/java-db")
    return staging
