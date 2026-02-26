#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = ["pyyaml"]
# ///
"""
Build Ray Docker images locally using raymake.
"""
from __future__ import annotations

from dataclasses import dataclass

import yaml

from ci.build.build_common import BuildError, find_ray_root

DEFAULT_ARCHITECTURE = "x86_64"

# Maps image type to its base name (YAML config key and Docker Hub repo).
_BASE_TYPE_MAP: dict[str, str] = {
    "ray": "ray",
    "ray-extra": "ray",
    "ray-llm": "ray-llm",
    "ray-llm-extra": "ray-llm",
}


def _load_ray_images_yaml() -> dict:
    """Load ray-images.yaml from the repository root."""
    path = find_ray_root() / "ray-images.yaml"
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
        return _BASE_TYPE_MAP[self.image_type]

    @property
    def variation_suffix(self) -> str:
        if self.image_type.endswith("-extra"):
            return "-extra"
        return ""

    @property
    def platform_class(self) -> str:
        """Returns 'cpu' or 'cuda'."""
        if self.platform == "cpu":
            return "cpu"
        if self.platform.startswith("cu"):
            return "cuda"
        raise ValueError(f"Could not determine platform class for {self.platform!r}")

    def get_wanda_spec_path(self) -> str:
        """Returns the path to the wanda spec file for this image."""
        key: WandaSpecKey = (self.image_type, self.platform_class)
        try:
            return WANDA_SPEC_PATHS[key]
        except KeyError:
            raise RayImageError(
                f"No wanda spec for image_type={self.image_type!r}, "
                f"platform={self.platform!r}"
            ) from None

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
