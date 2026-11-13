"""Work out which images a build's Trivy scan steps should cover.

``expected_images`` reads ``.buildkite/image-scan.rayci.yml`` with
scan_images' own option parser. A report can then list an image whose scan
job never ran or never uploaded, instead of silently leaving it out.
"""

import re
import shlex
from typing import Any, Mapping

import click

from ci.ray_ci.automation.trivy import scan_images
from ci.ray_ci.automation.trivy.trivy_report import Image
from ci.ray_ci.ray_image import RayImage, RayImageError

PIPELINE_RUNFILE = "io_ray/.buildkite/image-scan.rayci.yml"
SCAN_TARGET = "//ci/ray_ci/automation/trivy:scan_images"


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


def _scan_args(command: str, key: str) -> list[str]:
    """The arguments after the `--` that follows the scan_images target."""
    args = shlex.split(command)
    if SCAN_TARGET not in args:
        raise ValueError(f"step {key} names scan_images in a form this can't read")
    try:
        separator = args.index("--", args.index(SCAN_TARGET) + 1)
    except ValueError:
        raise ValueError(
            f"step {key} has no `--` before scan_images' arguments"
        ) from None
    return args[separator + 1 :]


def _parse_scan_args(args: list[str], key: str) -> dict[str, Any]:
    """Parse with scan_images' own options, strictly: an unknown option or a
    stray argument (e.g. `&& echo`) is an error, not something to skip."""
    parser = scan_images.main.make_parser(click.Context(scan_images.main))
    try:
        params, extra, _ = parser.parse_args(args=list(args))
    except click.UsageError as e:
        raise ValueError(f"step {key}: {e.format_message()}") from e
    if extra:
        raise ValueError(f"step {key} passes scan_images unexpected arguments {extra}")
    return params


def _step_images(step: Mapping[str, Any]) -> list[Image]:
    """The images one step scans, or none if it doesn't run scan_images.
    A scan step this can't read exactly is an error: guessing would make
    the coverage check quietly wrong."""
    key = step.get("key") or step.get("label") or "?"
    scans = [c for c in _commands(step) if SCAN_TARGET in c]
    runs = sum(c.count(SCAN_TARGET) for c in scans)
    if not runs:
        return []
    if runs > 1:
        raise ValueError(f"step {key} runs scan_images {runs} times")
    images = []
    for platform in (step.get("array") or {}).get("platform") or [""]:
        expanded = scans[0].replace("{{array.platform}}", platform)
        leftover = re.search(r"\{\{[^}]*\}\}", expanded)
        if leftover:
            raise ValueError(f"step {key} leaves {leftover.group()} unexpanded")
        params = _parse_scan_args(_scan_args(expanded, key), key)
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
