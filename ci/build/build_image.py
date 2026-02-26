#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = ["pyyaml"]
# ///
"""
Build Ray Docker images locally using raymake.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

from ci.build.build_common import (
    BuildError,
    detect_host_arch,
    find_ray_root,
    get_git_commit,
    log,
    parse_file,
)

DEFAULT_ARCHITECTURE = "x86_64"

# Maps base image type string to its YAML config key.
_BASE_TYPE_MAP: dict[str, str] = {
    "ray": "ray",
    "ray-extra": "ray",
    "ray-llm": "ray-llm",
    "ray-llm-extra": "ray-llm",
}

# Maps image type to its Docker Hub repo name.
_REPO_MAP: dict[str, str] = {
    "ray": "ray",
    "ray-extra": "ray",
    "ray-llm": "ray-llm",
    "ray-llm-extra": "ray-llm",
}


def _load_ray_images_yaml() -> dict:
    """Load ray-images.yaml from the repository root (relative to this file)."""
    path = Path(__file__).resolve().parent.parent.parent / "ray-images.yaml"
    if not path.exists():
        raise BuildError(f"Missing {path}")
    with open(path) as f:
        return yaml.safe_load(f)


def _build_image_type_config() -> dict[str, dict]:
    """Build IMAGE_TYPE_CONFIG from ray-images.yaml."""
    raw = _load_ray_images_yaml()
    config: dict[str, dict] = {}
    for image_type, yaml_key in _BASE_TYPE_MAP.items():
        yaml_cfg = raw[yaml_key]
        defaults = yaml_cfg["defaults"]
        platforms = yaml_cfg["platforms"]
        config[image_type] = {
            "python_versions": yaml_cfg["python"],
            "platforms": platforms,
            "architectures": yaml_cfg["architectures"],
            "default_python": defaults["python"],
            "default_platform": "cpu"
            if "cpu" in platforms
            else defaults["gpu_platform"],
        }
    return config


IMAGE_TYPE_CONFIG: dict[str, dict] = _build_image_type_config()


class RayImageError(Exception):
    """Raised when a RayImage field combination is invalid."""


@dataclass(frozen=True)
class RayImage:
    """Immutable identity of a Ray Docker image variant."""

    image_type: str
    python_version: str
    platform: str
    architecture: str = DEFAULT_ARCHITECTURE

    @property
    def wanda_image_name(self) -> str:
        """Wanda output image name (without registry prefix)."""
        if self.platform == "cpu":
            return f"{self.image_type}-py{self.python_version}-cpu{self.arch_suffix}"
        return f"{self.image_type}-py{self.python_version}-{self.platform}{self.arch_suffix}"

    @property
    def arch_suffix(self) -> str:
        if self.architecture == DEFAULT_ARCHITECTURE:
            return ""
        return f"-{self.architecture}"

    @property
    def repo(self) -> str:
        return _REPO_MAP[self.image_type]

    @property
    def variation_suffix(self) -> str:
        if self.image_type.endswith("-extra"):
            return "-extra"
        return ""

    def validate(self) -> None:
        if self.image_type not in IMAGE_TYPE_CONFIG:
            valid = ", ".join(IMAGE_TYPE_CONFIG)
            raise RayImageError(
                f"Unknown image type {self.image_type!r}. Valid types: {valid}"
            )
        cfg = IMAGE_TYPE_CONFIG[self.image_type]
        if self.python_version not in cfg["python_versions"]:
            raise RayImageError(
                f"Invalid python version {self.python_version} "
                f"for {self.image_type}. "
                f"Valid versions: {', '.join(cfg['python_versions'])}"
            )
        if self.platform not in cfg["platforms"]:
            raise RayImageError(
                f"Invalid platform {self.platform} "
                f"for {self.image_type}. "
                f"Valid platforms: {', '.join(cfg['platforms'])}"
            )
        if self.architecture not in cfg["architectures"]:
            raise RayImageError(
                f"Invalid architecture {self.architecture} "
                f"for {self.image_type}. "
                f"Valid architectures: {', '.join(cfg['architectures'])}"
            )


# (image_type, platform_class) where platform_class is "cpu" or "cuda".
WandaSpecKey = tuple[str, str]

WANDA_SPEC_PATHS: dict[WandaSpecKey, str] = {
    ("ray", "cpu"): "ci/docker/ray-image-cpu.wanda.yaml",
    ("ray", "cuda"): "ci/docker/ray-image-cuda.wanda.yaml",
    ("ray-extra", "cpu"): "ci/docker/ray-extra-image-cpu.wanda.yaml",
    ("ray-extra", "cuda"): "ci/docker/ray-extra-image-cuda.wanda.yaml",
    ("ray-llm", "cuda"): "ci/docker/ray-llm-image-cuda.wanda.yaml",
    ("ray-llm-extra", "cuda"): "ci/docker/ray-llm-extra-image-cuda.wanda.yaml",
}

SUPPORTED_IMAGE_TYPES: list[str] = list(dict.fromkeys(t for t, _ in WANDA_SPEC_PATHS))
REGISTRY_PREFIX = "cr.ray.io/rayproject/"


@dataclass(frozen=True)
class ImageBuildConfig:
    ray_image: RayImage
    ray_root: Path

    @property
    def raymake_version(self) -> str:
        return (self.ray_root / ".rayciversion").read_text().strip()

    @property
    def manylinux_version(self) -> str:
        return parse_file(
            self.ray_root / "rayci.env", r'MANYLINUX_VERSION=["\']?([^"\'\s]+)'
        )

    @property
    def ray_version(self) -> str:
        return parse_file(self.ray_root / "rayci.env", r'RAY_VERSION=["\']?([^"\'\s]+)')

    @property
    def commit(self) -> str:
        return get_git_commit(self.ray_root)

    @property
    def wanda_spec_path(self) -> str:
        key: WandaSpecKey = (
            self.ray_image.image_type,
            "cpu" if self.ray_image.platform == "cpu" else "cuda",
        )
        if key not in WANDA_SPEC_PATHS:
            raise BuildError(
                f"No wanda spec for image_type={self.ray_image.image_type!r}, "
                f"platform={self.ray_image.platform!r}"
            )
        return WANDA_SPEC_PATHS[key]

    @property
    def wanda_image_tag(self) -> str:
        return f"{REGISTRY_PREFIX}{self.ray_image.wanda_image_name}"

    @property
    def nightly_alias(self) -> str | None:
        cfg = IMAGE_TYPE_CONFIG[self.ray_image.image_type]
        if (
            self.ray_image.python_version != cfg["default_python"]
            or self.ray_image.platform != cfg["default_platform"]
            or self.ray_image.architecture != DEFAULT_ARCHITECTURE
        ):
            return None
        return f"{REGISTRY_PREFIX}{self.ray_image.repo}:nightly{self.ray_image.variation_suffix}"

    @property
    def build_env(self) -> dict[str, str]:
        env = {
            "PYTHON_VERSION": self.ray_image.python_version,
            "MANYLINUX_VERSION": self.manylinux_version,
            "HOSTTYPE": self.ray_image.architecture,
            "ARCH_SUFFIX": self.ray_image.arch_suffix,
            "BUILDKITE_COMMIT": self.commit,
            "RAY_VERSION": self.ray_version,
            "IS_LOCAL_BUILD": "true",
            "IMAGE_TYPE": self.ray_image.repo,
        }
        if self.ray_image.platform != "cpu":
            env["CUDA_VERSION"] = self.ray_image.platform.removeprefix("cu")
        return env

    @classmethod
    def from_args(
        cls,
        image_type: str,
        python_version: str,
        image_platform: str,
    ) -> ImageBuildConfig:
        if image_type not in SUPPORTED_IMAGE_TYPES:
            raise BuildError(
                f"Unknown image type {image_type!r}. "
                f"Valid types: {', '.join(SUPPORTED_IMAGE_TYPES)}"
            )

        root = find_ray_root()
        ray_image = RayImage(
            image_type=image_type,
            python_version=python_version,
            platform=image_platform,
            architecture=cls._detect_host_arch(),
        )
        try:
            ray_image.validate()
        except RayImageError as e:
            raise BuildError(str(e)) from e
        return cls(ray_image=ray_image, ray_root=root)

    @staticmethod
    def _detect_host_arch() -> str:
        return detect_host_arch()


class ImageBuilder:
    def __init__(self, config: ImageBuildConfig):
        self.config = config

    def build(self) -> str:
        """Build the image and return the primary image tag."""
        if not shutil.which("raymake"):
            raise BuildError("raymake not found. Run via ./build-image.sh")

        log.info("Build configuration:")
        summary = {
            "Image Type": self.config.ray_image.image_type,
            "Python": self.config.ray_image.python_version,
            "Platform": self.config.ray_image.platform,
            "Arch": self.config.ray_image.architecture,
            "Commit": self.config.commit,
            "Raymake": self.config.raymake_version,
            "Ray Version": self.config.ray_version,
            "Wanda Spec": self.config.wanda_spec_path,
        }
        print("-" * 50)
        for k, v in summary.items():
            print(f"{k:<12}: {v}")
        print("-" * 50)

        cmd = [
            "raymake",
            str(self.config.wanda_spec_path),
        ]

        log.info(f"Running raymake: {self.config.wanda_spec_path}")
        log.info(f"Build environment: {self.config.build_env}")
        proc = subprocess.Popen(
            cmd,
            cwd=self.config.ray_root,
            env={**os.environ, **self.config.build_env},
        )
        try:
            if proc.wait() != 0:
                raise BuildError("raymake failed")
        except KeyboardInterrupt:
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
            raise

        image_tag = self.config.wanda_image_tag
        log.info(f"Built image: {image_tag}")

        alias = self.config.nightly_alias
        if alias:
            self._docker_tag(image_tag, alias)
            log.info(f"Tagged alias: {alias}")

        return image_tag

    @staticmethod
    def _docker_tag(source: str, dest: str) -> None:
        result = subprocess.run(
            ["docker", "tag", source, dest],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise BuildError(f"docker tag failed: {result.stderr.strip()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _image_type_table() -> str:
    """Build an ASCII table of all image types from IMAGE_TYPE_CONFIG."""

    def _tag_default(items, default):
        return [f"{v} (default)" if v == default else v for v in items]

    def _wrap(items, width):
        """Join items with ', ' and wrap into lines that fit in *width*."""
        lines: list[str] = []
        cur = ""
        for item in items:
            entry = item if not cur else f", {item}"
            if cur and len(cur) + len(entry) > width:
                lines.append(cur)
                cur = item
            else:
                cur += entry
        if cur:
            lines.append(cur)
        return lines

    headers = ("IMAGE TYPE", "PYTHON VERSIONS (-p)", "PLATFORMS (--platform)")
    plat_width = 52  # wrap platforms to this width

    def _build_rows(type_list):
        rows: list[tuple[str, str, list[str]]] = []
        for name in type_list:
            cfg = IMAGE_TYPE_CONFIG[name]
            py_cell = ", ".join(
                _tag_default(cfg["python_versions"], cfg["default_python"])
            )
            plat_lines = _wrap(
                _tag_default(cfg["platforms"], cfg["default_platform"]),
                plat_width,
            )
            rows.append((name, py_cell, plat_lines))
        return rows

    rows = _build_rows(SUPPORTED_IMAGE_TYPES)

    # Compute column widths.
    widths = [len(h) for h in headers]
    for label, py_cell, plat_lines in rows:
        widths[0] = max(widths[0], len(label))
        widths[1] = max(widths[1], len(py_cell))
        widths[2] = max(widths[2], *(len(l) for l in plat_lines))

    sep = "  "

    def fmt(cells):
        return sep.join(c.ljust(widths[i]) for i, c in enumerate(cells))

    lines = [fmt(headers), sep.join("-" * w for w in widths)]
    for label, py_cell, plat_lines in rows:
        lines.append(fmt((label, py_cell, plat_lines[0])))
        for cont in plat_lines[1:]:
            lines.append(fmt(("", "", cont)))

    return "\n".join(lines)


def _nightly_table() -> str:
    """Build a table mapping build commands to nightly docker run commands."""
    headers = ("BUILD", "RUN")
    rows = []
    for name in SUPPORTED_IMAGE_TYPES:
        cfg = IMAGE_TYPE_CONFIG[name]
        img = RayImage(name, cfg["default_python"], cfg["default_platform"])
        tag = f"{REGISTRY_PREFIX}{img.repo}:nightly{img.variation_suffix}"
        rows.append((f"./build-image.sh {name}", f"docker run -it {tag}"))

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    sep = "  "

    def fmt(cells):
        return sep.join(c.ljust(widths[i]) for i, c in enumerate(cells))

    lines = [fmt(headers), sep.join("-" * w for w in widths)]
    lines.extend(fmt(row) for row in rows)
    return "\n".join(lines)


def _build_examples() -> str:
    """Generate non-default example commands from IMAGE_TYPE_CONFIG."""
    ray_cfg = IMAGE_TYPE_CONFIG["ray"]
    ray_py = ray_cfg["default_python"]
    alt_py = next((v for v in ray_cfg["python_versions"] if v != ray_py), None)
    alt_plat = next((p for p in ray_cfg["platforms"] if p != "cpu"), None)

    examples: list[tuple[str, str]] = []
    if alt_py:
        examples.append(
            (f"ray -p {alt_py}", f"ray cpu py{alt_py}"),
        )
    if alt_plat:
        examples.append(
            (f"ray --platform {alt_plat}", f"ray {alt_plat} py{ray_py}"),
        )
    if alt_py and alt_plat:
        examples.append(
            (f"ray -p {alt_py} --platform {alt_plat}", f"ray {alt_plat} py{alt_py}"),
        )

    if not examples:
        return ""

    cmd_prefix = "  ./build-image.sh "
    cmds = [cmd_prefix + args for args, _ in examples]
    max_cmd = max(len(c) for c in cmds)
    lines = []
    for cmd, (_, comment) in zip(cmds, examples):
        lines.append(f"{cmd:<{max_cmd}}  # {comment}")
    return "\n".join(lines)


def main():
    epilog_parts = [
        "Supported image types:",
        _image_type_table(),
        "",
        "Nightly images (built with defaults):",
        _nightly_table(),
        "",
        "Examples:",
        _build_examples(),
    ]

    parser = argparse.ArgumentParser(
        prog="./build-image.sh",
        description="Build Ray Docker images locally using raymake.",
        epilog="\n".join(epilog_parts),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "image_type",
        choices=SUPPORTED_IMAGE_TYPES,
        metavar="IMAGE_TYPE",
        help=f"Image type: {', '.join(SUPPORTED_IMAGE_TYPES)}",
    )
    parser.add_argument(
        "-p",
        "--python-version",
        default=None,
        metavar="VERSION",
        help="Python version (default depends on image type; see table below)",
    )
    parser.add_argument(
        "--platform",
        default=None,
        metavar="PLATFORM",
        help="Target platform (default depends on image type; see table below)",
    )
    parser.add_argument(
        "--list-platforms",
        action="store_true",
        help="List valid platforms for the given image type and exit",
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    cfg = IMAGE_TYPE_CONFIG[args.image_type]

    if args.list_platforms:
        print(f"Valid platforms for {args.image_type}:")
        for p in cfg["platforms"]:
            marker = " (default)" if p == cfg["default_platform"] else ""
            print(f"  {p}{marker}")
        sys.exit(0)

    python_version = args.python_version or cfg["default_python"]
    platform_choice = args.platform or cfg["default_platform"]

    try:
        config = ImageBuildConfig.from_args(
            args.image_type, python_version, platform_choice
        )
        builder = ImageBuilder(config)
        image_tag = builder.build()

        print()
        log.info("Success!")
        print(f"  docker run -it {image_tag}")
        alias = config.nightly_alias
        if alias:
            print(f"  docker run -it {alias}")
    except BuildError as e:
        log.error(e)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
