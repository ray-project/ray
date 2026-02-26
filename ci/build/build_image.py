#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = ["pyyaml"]
# ///
"""
Build Ray Docker images locally using raymake.
"""
from __future__ import annotations

import os
import shutil
import subprocess
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
        """Returns 'cpu', 'cuda', 'tpu', etc."""
        if self.platform == "cpu":
            return "cpu"
        if self.platform == "tpu":
            return "tpu"
        if self.platform.startswith("cu"):
            return "cuda"
        raise RayImageError(f"Could not determine platform class for {self.platform!r}")

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
    ("ray", "tpu"): "ci/docker/ray-image-tpu.wanda.yaml",
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
    raymake_version: str
    manylinux_version: str
    ray_version: str
    commit: str

    @property
    def wanda_spec_path(self) -> str:
        return self.ray_image.get_wanda_spec_path()

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
        if self.ray_image.platform.startswith("cu"):
            env["CUDA_VERSION"] = self.ray_image.platform.removeprefix("cu")
        return env

    @classmethod
    def from_args(
        cls,
        image_type: str,
        python_version: str,
        image_platform: str,
    ) -> ImageBuildConfig:
        # Validate inputs before expensive operations (filesystem, platform).
        try:
            probe = RayImage(image_type, python_version, image_platform)
            probe.validate()
            probe.get_wanda_spec_path()
        except RayImageError as e:
            raise BuildError(str(e)) from e

        ray_image = RayImage(
            image_type=image_type,
            python_version=python_version,
            platform=image_platform,
            architecture=cls._detect_host_arch(),
        )
        # Re-validate with real architecture (catches arch-restricted types).
        try:
            ray_image.validate()
        except RayImageError as e:
            raise BuildError(str(e)) from e

        root = find_ray_root()
        return cls(
            ray_image=ray_image,
            ray_root=root,
            raymake_version=(root / ".rayciversion").read_text().strip(),
            manylinux_version=parse_file(
                root / "rayci.env", r'MANYLINUX_VERSION=["\']?([^"\'\s]+)'
            ),
            ray_version=parse_file(
                root / "rayci.env", r'RAY_VERSION=["\']?([^"\'\s]+)'
            ),
            commit=get_git_commit(root),
        )

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
