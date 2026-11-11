"""Scan one (architecture, platform) group of Ray images with Trivy.

For each image the build pushed to its wanda cache, this:
1. checks the ref is a single-platform image for the expected architecture;
2. pulls every image of the group into one shared OCI layout with crane, so
   layers shared across Python versions and image types are stored once;
3. runs the pinned Trivy on each image, by digest, in a throwaway container
   with no network, no credentials, no inherited environment, a non-root
   user, a read-only root and no capabilities;
4. writes ``<name>.status.json`` and ``<name>.json.gz``, the input contract
   of ``trivy_report``.

The job talks to the host's docker daemon, so bind mounts would resolve host
paths. Files reach the Trivy containers through docker volumes filled with
``docker cp`` instead.

A finding never fails this tool; an image that couldn't be scanned does
(exit 1), after every other image in the group has been scanned.

Example:
    bazel run //ci/ray_ci/automation/trivy:scan_images -- \\
        --architecture x86_64 --platform cu12.8.1-cudnn \\
        --image ray:3.11 --image ray:3.12 --image ray-extra:3.12
"""

import gzip
import json
import logging
import shutil
import subprocess
import sys
import tarfile
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

import click
import runfiles

from ci.ray_ci.automation.push_ray_image import RayImagePushContext
from ci.ray_ci.automation.trivy import registry
from ci.ray_ci.automation.trivy.trivy_db import (
    DB_SOURCES,
    DbSource,
    db_layer,
    mirror_ref,
)
from ci.ray_ci.docker_container import RayType
from ci.ray_ci.ray_image import RayImageError
from ci.ray_ci.utils import ecr_docker_login

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
SCAN_TIMEOUT = "30m"
# Single-platform manifests; an index would let Trivy silently pick one.
IMAGE_MANIFEST_TYPES = frozenset(
    {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }
)
OCI_ARCH = {"x86_64": "amd64", "aarch64": "arm64"}
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
    digest: str = ""
    error: str = ""
    db: dict[str, Any] = field(default_factory=dict)

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
            ],
            "status": "error" if self.error else "scanned",
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


def check_target(target: Target, architecture: str) -> None:
    """Record the digest, or why this ref must not be scanned."""
    try:
        manifest = registry.manifest(target.ref)
        if manifest.get("mediaType") not in IMAGE_MANIFEST_TYPES:
            target.error = (
                f"{target.ref} is {manifest.get('mediaType')}, not a "
                "single-platform image manifest"
            )
            return
        found = registry.config(target.ref).get("architecture")
        if found != OCI_ARCH[architecture]:
            target.error = f"{target.ref} is {found}, expected {OCI_ARCH[architecture]}"
            return
        target.digest = registry.digest(target.ref)
    except registry.RegistryError as e:
        target.error = str(e)


def check_layout(layout: Path, digests: set[str]) -> None:
    index = json.loads((layout / "index.json").read_text())
    found = [m.get("digest") for m in index.get("manifests") or []]
    missing = digests - set(found)
    if missing or len(found) != len(set(found)):
        raise ScanError(f"layout has manifests {found}, expected {sorted(digests)}")


def _safe_extract(tarball: Path, dest: Path, allowed: set[str]) -> None:
    """Extract only the expected top-level DB files."""
    dest.mkdir(parents=True)
    with tarfile.open(tarball) as tar:
        for member in tar.getmembers():
            if not member.isfile() or member.name not in allowed:
                raise ScanError(f"unexpected entry {member.name!r} in {tarball.name}")
            with tar.extractfile(member) as src, open(dest / member.name, "wb") as out:
                shutil.copyfileobj(src, out)
    missing = allowed - {p.name for p in dest.iterdir()}
    if missing:
        raise ScanError(f"{tarball.name} lacks {sorted(missing)}")


def fetch_db(
    source: DbSource, db_repo: str, build_id: str, workdir: Path
) -> tuple[Path, dict[str, Any]]:
    """Fetch this build's DB mirror by digest and verify it."""
    ref = mirror_ref(db_repo, build_id, source)
    manifest_digest = registry.digest(ref)
    layer = db_layer(registry.manifest(ref), source.media_type)
    tarball = workdir / f"{source.name}.tar.gz"
    registry.fetch_blob(ref.rsplit(":", 1)[0], layer["digest"], tarball)
    dest = workdir / source.name
    _safe_extract(tarball, dest, DB_FILES[source.name])
    tarball.unlink()
    return dest, {"ref": ref, "manifest": manifest_digest, "blob": layer["digest"]}


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
        prefix = f"trivy-scan-{uuid.uuid4().hex[:12]}"
        self.data = f"{prefix}-data"
        self.work = f"{prefix}-work"
        self.loader = f"{prefix}-loader"

    def __enter__(self) -> "ContainedTrivy":
        _run(["docker", "volume", "create", self.data])
        _run(["docker", "volume", "create", self.work])
        _run(
            [
                "docker",
                "create",
                "--name",
                self.loader,
                "-v",
                f"{self.data}:/data",
                "-v",
                f"{self.work}:/work",
                self.base_image,
                # Never started; distroless has no default command to fall back on.
                "/data/trivy",
            ]
        )
        return self

    def __exit__(self, *exc: object) -> None:
        _run(["docker", "rm", "-f", self.loader], check=False)
        _run(["docker", "volume", "rm", "-f", self.data, self.work], check=False)

    def load(self, staging: Path) -> None:
        """Copy ``staging/data`` and ``staging/work`` into the volumes."""
        _run(["docker", "cp", f"{staging / 'data'}/.", f"{self.loader}:/data"])
        _run(["docker", "cp", f"{staging / 'work'}/.", f"{self.loader}:/work"])

    def docker_run_args(self, target: Target) -> list[str]:
        return [
            "docker",
            "run",
            "--rm",
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
        _run(self.docker_run_args(target))
        _run(["docker", "cp", f"{self.loader}:/work/out/{target.name}.json", str(dest)])


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


def write_status(results_dir: Path, target: Target) -> None:
    path = results_dir / f"{target.name}.status.json"
    path.write_text(json.dumps(target.status_record(), indent=2) + "\n")


def scan_group(
    targets: Sequence[Target],
    architecture: str,
    db_repo: str,
    build_id: str,
    results_dir: Path,
    workdir: Path,
    base_image: str,
) -> None:
    """Scan every target; each one ends with a status record either way."""
    for target in targets:
        check_target(target, architecture)
    ready = [t for t in targets if not t.error]
    try:
        if ready:
            dbs: dict[str, Path] = {}
            pins: dict[str, Any] = {}
            for source in DB_SOURCES:
                dbs[source.name], pins[source.name] = fetch_db(
                    source, db_repo, build_id, workdir
                )
            layout = workdir / "layout"
            platform = f"linux/{OCI_ARCH[architecture]}"
            registry.pull(sorted({t.pinned_ref for t in ready}), layout, platform)
            check_layout(layout, {t.digest for t in ready})
            staging = stage(workdir, layout, dbs, {t.image_type for t in ready})
            with ContainedTrivy(base_image) as trivy:
                trivy.load(staging)
                shutil.rmtree(staging)
                for target in ready:
                    target.db = pins
                    raw = workdir / f"{target.name}.json"
                    try:
                        trivy.scan(target, raw)
                        with open(raw, "rb") as src, gzip.open(
                            results_dir / f"{target.name}.json.gz", "wb"
                        ) as out:
                            shutil.copyfileobj(src, out)
                        raw.unlink()
                    except (ScanError, OSError) as e:
                        target.error = str(e)
    except (ScanError, registry.RegistryError, OSError) as e:
        for target in ready:
            target.error = target.error or f"group failed before scanning: {e}"
    finally:
        for target in targets:
            write_status(results_dir, target)


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
@click.option("--work-dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--rayci-work-repo", required=True, envvar="RAYCI_WORK_REPO")
@click.option("--rayci-build-id", required=True, envvar="RAYCI_BUILD_ID")
@click.option(
    "--db-repo",
    help="Repo holding this build's DB mirror (default: the work repo).",
)
@click.option("--branch", required=True, envvar="BUILDKITE_BRANCH")
@click.option("--commit", required=True, envvar="BUILDKITE_COMMIT")
@click.option("--rayci-schedule", default="", envvar="RAYCI_SCHEDULE")
@click.option("--pull-request", default="false", envvar="BUILDKITE_PULL_REQUEST")
@click.option("--base-image", default=BASE_IMAGE, show_default=True)
def main(
    architecture: str,
    platform: str,
    images: tuple[str, ...],
    results_dir: Path,
    work_dir: Optional[Path],
    rayci_work_repo: str,
    rayci_build_id: str,
    db_repo: Optional[str],
    branch: str,
    commit: str,
    rayci_schedule: str,
    pull_request: str,
    base_image: str,
) -> None:
    targets = build_targets(
        [parse_image(spec) for spec in images],
        platform,
        architecture,
        rayci_work_repo,
        rayci_build_id,
        branch,
        commit,
        rayci_schedule,
        pull_request,
    )
    db_repo = db_repo or rayci_work_repo
    for repo in {rayci_work_repo, db_repo}:
        registry.login_if_ecr(repo, ecr_docker_login)
    results_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=work_dir) as tmp:
        scan_group(
            targets,
            architecture,
            db_repo,
            rayci_build_id,
            results_dir,
            Path(tmp),
            base_image,
        )
    failed = [t for t in targets if t.error]
    for target in failed:
        logger.error(f"{target.name}: {target.error}")
    logger.info(f"Scanned {len(targets) - len(failed)}/{len(targets)} images")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
