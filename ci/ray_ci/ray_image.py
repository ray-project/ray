"""
Shared RayImage dataclass for image identity logic.

Centralizes wanda image naming, validation, repo lookup, arch suffix,
and variation suffix computation used by both ci/build/build_image.py
and ci/ray_ci/automation/push_ray_image.py.
"""
from __future__ import annotations

from dataclasses import dataclass

from ci.ray_ci.configs import DEFAULT_ARCHITECTURE, DEFAULT_PYTHON_TAG_VERSION
from ci.ray_ci.docker_container import (
    ARCHITECTURES_RAY,
    ARCHITECTURES_RAY_LLM,
    PLATFORMS_RAY,
    PLATFORMS_RAY_LLM,
    PYTHON_VERSIONS_RAY,
    PYTHON_VERSIONS_RAY_LLM,
    RAY_REPO_MAP,
    RayType,
)


class RayImageError(Exception):
    """Raised when a RayImage field combination is invalid."""


# Per-type configuration: valid python versions, platforms, architectures,
# and the default python/platform (used for nightly aliases and CLI defaults).
IMAGE_TYPE_CONFIG: dict[str, dict] = {
    RayType.RAY: {
        "python_versions": PYTHON_VERSIONS_RAY,
        "platforms": PLATFORMS_RAY,
        "architectures": ARCHITECTURES_RAY,
        "default_python": DEFAULT_PYTHON_TAG_VERSION,
        "default_platform": "cpu",
    },
    RayType.RAY_EXTRA: {
        "python_versions": PYTHON_VERSIONS_RAY,
        "platforms": PLATFORMS_RAY,
        "architectures": ARCHITECTURES_RAY,
        "default_python": DEFAULT_PYTHON_TAG_VERSION,
        "default_platform": "cpu",
    },
    RayType.RAY_LLM: {
        "python_versions": PYTHON_VERSIONS_RAY_LLM,
        "platforms": PLATFORMS_RAY_LLM,
        "architectures": ARCHITECTURES_RAY_LLM,
        "default_python": "3.11",
        "default_platform": "cu12.8.1-cudnn",
    },
    RayType.RAY_LLM_EXTRA: {
        "python_versions": PYTHON_VERSIONS_RAY_LLM,
        "platforms": PLATFORMS_RAY_LLM,
        "architectures": ARCHITECTURES_RAY_LLM,
        "default_python": "3.11",
        "default_platform": "cu12.8.1-cudnn",
    },
}


@dataclass(frozen=True)
class RayImage:
    """Immutable identity of a Ray Docker image variant."""

    image_type: str
    python_version: str
    platform: str
    architecture: str = DEFAULT_ARCHITECTURE

    def __post_init__(self):
        # Normalize RayType enum values to plain strings so f-strings
        # produce "ray" instead of "RayType.RAY" (Python 3.12+ changed
        # str(Enum) formatting).
        if isinstance(self.image_type, RayType):
            object.__setattr__(self, "image_type", self.image_type.value)

    @property
    def wanda_image_name(self) -> str:
        """Wanda output image name (without registry prefix)."""
        if self.platform == "cpu":
            return f"{self.image_type}-py{self.python_version}-cpu{self.arch_suffix}"
        return f"{self.image_type}-py{self.python_version}-{self.platform}{self.arch_suffix}"

    @property
    def arch_suffix(self) -> str:
        """Architecture suffix for image names (empty for default arch)."""
        if self.architecture == DEFAULT_ARCHITECTURE:
            return ""
        return f"-{self.architecture}"

    @property
    def repo(self) -> str:
        """Docker Hub repository name (e.g. 'ray', 'ray-ml', 'ray-llm')."""
        return RAY_REPO_MAP[self.image_type]

    @property
    def variation_suffix(self) -> str:
        """Variation suffix: '-extra' for extra types, '' otherwise."""
        if self.image_type in (
            RayType.RAY_EXTRA,
            RayType.RAY_ML_EXTRA,
            RayType.RAY_LLM_EXTRA,
        ):
            return "-extra"
        return ""

    def validate(self) -> None:
        """
        Validate that image_type, python_version, platform, and architecture
        are a valid combination. Raises RayImageError on invalid input.
        """
        if self.image_type not in IMAGE_TYPE_CONFIG:
            valid = ", ".join(IMAGE_TYPE_CONFIG.keys())
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
