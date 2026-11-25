"""Scan targets for images given by name, outside CI.

scan_images scans the images a CI build made, named by Ray image type and
Python version. A LocalTarget is any image given by name and tag or digest,
e.g. a published rayproject/ray tag, for scan_images.scan_group to scan the
same way. The local_scan_images command builds on these to scan such images
on your own machine.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

import click

from ci.ray_ci.automation.trivy.scan_images import Target
from ci.ray_ci.automation.trivy.trivy_report import Image

IMAGE_TYPE = "image"
# Only the characters an image's name, tag or digest uses: nothing a shell, the
# markdown or crane (a leading "-" reads as a flag) would act on.
IMAGE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/:@-]*")


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
