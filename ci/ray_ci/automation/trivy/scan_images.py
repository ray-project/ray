"""Name and check the Ray images one Trivy scan job scans.

For one (architecture, platform) group, ``build_targets`` names each image
the build pushed to its wanda cache, with the tags the publish step gives it.
``check_target`` resolves each ref to a digest and checks, on the digest
itself since a tag can be re-pushed meanwhile, that it is a single-platform
image for the expected architecture.
"""

import logging
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Sequence

import click

from ci.ray_ci.automation.push_ray_image import RayImagePushContext
from ci.ray_ci.automation.trivy import registry
from ci.ray_ci.automation.trivy.scan_store import StoreError
from ci.ray_ci.docker_container import RayType
from ci.ray_ci.ray_image import RayImageError

logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
logger = logging.getLogger(__name__)
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
