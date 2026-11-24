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
    try:
        args = shlex.split(command)
    except ValueError as e:
        raise ValueError(f"step {key}'s command can't be split: {e}") from None
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
    command = scan_images.main
    ctx = click.Context(command)
    try:
        params, extra, _ = command.make_parser(ctx).parse_args(args=list(args))
    except click.UsageError as e:
        raise ValueError(f"step {key}: {e.format_message()}") from e
    if extra:
        raise ValueError(f"step {key} passes scan_images unexpected arguments {extra}")
    if "help" in params:
        raise ValueError(f"step {key} passes --help, so scan_images would scan nothing")
    # The raw parser skips type checks, e.g. --architecture's choices.
    options = {param.name: param for param in command.params}
    try:
        return {
            name: options[name].type_cast_value(ctx, value)
            for name, value in params.items()
        }
    except click.BadParameter as e:
        raise ValueError(f"step {key}: {e.format_message()}") from e


def _step_key(step: Mapping[str, Any]) -> str:
    return str(step.get("key") or step.get("label") or "?")


def _step_images(step: Mapping[str, Any]) -> list[Image]:
    """The images one step scans, or none if it doesn't run scan_images.
    A scan step this can't read exactly is an error: guessing would make
    the coverage check quietly wrong."""
    key = _step_key(step)
    # Any mention, so that an unfamiliar spelling of the target is an error
    # in _scan_args rather than a step quietly left out.
    scans = [c for c in _commands(step) if "scan_images" in c]
    mentions = sum(c.count("scan_images") for c in scans)
    if not mentions:
        return []
    if mentions > 1:
        raise ValueError(
            f"step {key} mentions scan_images {mentions} times; expected one run"
        )
    for field in ("matrix", "parallelism"):
        if field in step:
            raise ValueError(f"step {key} uses {field}, which this can't expand")
    array = step.get("array") or {}
    if not isinstance(array, Mapping):
        raise ValueError(f"step {key} has an array that isn't a mapping")
    # rayci also takes other dimensions and `adjustments`, which add
    # combinations: images this would miss, or jobs that repeat a scan.
    unknown = sorted(set(array) - {"platform"})
    if unknown:
        raise ValueError(f"step {key} uses array keys this can't expand: {unknown}")
    platforms = array.get("platform") or [""]
    if not isinstance(platforms, list) or not all(
        isinstance(p, str) for p in platforms
    ):
        raise ValueError(f"step {key} has an array.platform that isn't text")
    images = []
    for platform in platforms:
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
    images = []
    scanned_by: dict[str, str] = {}
    for step in pipeline.get("steps") or []:
        # A step that isn't a mapping, such as "wait", runs no command.
        if not isinstance(step, Mapping):
            continue
        key = _step_key(step)
        for image in _step_images(step):
            if image.name in scanned_by:
                first = scanned_by[image.name]
                where = f"step {key}" if first == key else f"steps {first} and {key}"
                raise ValueError(f"{image.name} is scanned twice, by {where}")
            scanned_by[image.name] = key
            images.append(image)
    return images
