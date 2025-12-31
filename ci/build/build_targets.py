"""
Build targets for Ray Wanda builds for local development.
"""

import os
import platform
import shutil
import subprocess
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional

IMAGE_PREFIX = "cr.ray.io/rayproject"
DEFAULT_MANYLINUX_VERSION = "251216.3835fc5"


@dataclass
class BuildContext:
    """Shared context for all build steps."""

    wanda_bin: Optional[Path] = None
    repo_root: Optional[Path] = None
    dry_run: bool = False
    env: Dict[str, str] = field(default_factory=dict, repr=False)
    _run_cmd: Optional[Callable] = field(default=None, repr=False)
    _built: set = field(default_factory=set, repr=False)  # Track built targets

    @property
    def python_version(self) -> str:
        return self.env.get("PYTHON_VERSION", "3.10")

    @property
    def arch_suffix(self) -> str:
        return self.env.get("ARCH_SUFFIX", "")

    def run(self, cmd: List[str], **kwargs) -> subprocess.CompletedProcess:
        """Run a command from repo root, respecting dry_run mode."""
        kwargs.setdefault("cwd", self.repo_root)
        kwargs.setdefault("env", self.env)
        if self._run_cmd:
            return self._run_cmd(cmd, **kwargs)
        if self.dry_run:
            print(f"[dry-run] {' '.join(cmd)}")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        return subprocess.run(cmd, check=True, **kwargs)

    def wanda(self, spec_file: str) -> None:
        """Run wanda with a spec file."""
        self.run(
            [str(self.wanda_bin), spec_file],
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

    def docker_tag(self, src: str, dst: str) -> None:
        """Tag a docker image."""
        self.run(["docker", "tag", src, dst], stdout=sys.stdout, stderr=sys.stderr)


class BuildTarget(ABC):
    """Base class for build targets."""

    SPEC: str = ""  # Wanda spec file path
    DEPS: List[type] = []  # Dependency target classes

    def __init__(self, ctx: BuildContext):
        self.ctx = ctx

    def build(self) -> None:
        """Build dependencies then this target (skips if already built)."""
        target_name = self.__class__.__name__
        if target_name in self.ctx._built:
            return
        for dep_cls in self.DEPS:
            dep_cls(self.ctx).build()
        self._build()
        self.ctx._built.add(target_name)

    @abstractmethod
    def _build(self) -> None:
        """Execute the actual build (override in subclasses)."""
        pass

    def header(self, msg: str) -> None:
        """Print a blue header."""
        print(f"\n\033[34;1m===> {msg}\033[0m", file=sys.stderr)


class RayCore(BuildTarget):
    """Builds ray-core (C++ components via bazel)."""

    SPEC = "ci/docker/ray-core.wanda.yaml"

    def _build(self) -> None:
        self.header("Building ray-core...")
        self.ctx.wanda(self.SPEC)
        self.ctx.docker_tag(self.image_name(), self.local_name())

    def image_name(self) -> str:
        return f"{IMAGE_PREFIX}/ray-core-py{self.ctx.python_version}{self.ctx.arch_suffix}:latest"

    def local_name(self) -> str:
        return f"ray-core-py{self.ctx.python_version}{self.ctx.arch_suffix}:latest"


class RayDashboard(BuildTarget):
    """Builds ray-dashboard (npm build)."""

    SPEC = "ci/docker/ray-dashboard.wanda.yaml"

    def _build(self) -> None:
        self.header("Building ray-dashboard...")
        self.ctx.wanda(self.SPEC)
        self.ctx.docker_tag(self.image_name(), self.local_name())

    def image_name(self) -> str:
        return f"{IMAGE_PREFIX}/ray-dashboard:latest"

    def local_name(self) -> str:
        return "ray-dashboard:latest"


class RayJava(BuildTarget):
    """Builds ray-java (Java JARs)."""

    SPEC = "ci/docker/ray-java.wanda.yaml"

    def _build(self) -> None:
        self.header("Building ray-java...")
        self.ctx.wanda(self.SPEC)
        self.ctx.docker_tag(self.image_name(), self.local_name())

    def image_name(self) -> str:
        return f"{IMAGE_PREFIX}/ray-java-build{self.ctx.arch_suffix}:latest"

    def local_name(self) -> str:
        return f"ray-java-build{self.ctx.arch_suffix}:latest"


class RayWheel(BuildTarget):
    """Builds the ray wheel."""

    SPEC = "ci/docker/ray-wheel.wanda.yaml"
    DEPS = [RayCore, RayDashboard, RayJava]

    def _build(self) -> None:
        self.header("Building ray-wheel...")
        self.ctx.wanda(self.SPEC)
        self.ctx.docker_tag(self.image_name(), self.local_name())

    def image_name(self) -> str:
        return f"{IMAGE_PREFIX}/ray-wheel-py{self.ctx.python_version}{self.ctx.arch_suffix}:latest"

    def local_name(self) -> str:
        return f"ray-wheel-py{self.ctx.python_version}{self.ctx.arch_suffix}:latest"


class RayCppCore(BuildTarget):
    """Builds ray-cpp-core (C++ headers and libs)."""

    SPEC = "ci/docker/ray-cpp-core.wanda.yaml"
    DEPS = [RayCore, RayDashboard, RayJava]

    def _build(self) -> None:
        self.header("Building ray-cpp-core...")
        self.ctx.wanda(self.SPEC)
        self.ctx.docker_tag(self.image_name(), self.local_name())

    def image_name(self) -> str:
        return f"{IMAGE_PREFIX}/ray-cpp-core-py{self.ctx.python_version}{self.ctx.arch_suffix}:latest"

    def local_name(self) -> str:
        return f"ray-cpp-core-py{self.ctx.python_version}{self.ctx.arch_suffix}:latest"


class RayCppWheel(BuildTarget):
    """Builds the ray-cpp wheel (includes cpp-core)."""

    SPEC = "ci/docker/ray-cpp-wheel.wanda.yaml"
    DEPS = [RayCppCore]

    def _build(self) -> None:
        self.header("Building ray-cpp-wheel...")
        self.ctx.wanda(self.SPEC)
        self.ctx.docker_tag(self.image_name(), self.local_name())

    def image_name(self) -> str:
        return f"{IMAGE_PREFIX}/ray-cpp-wheel-py{self.ctx.python_version}{self.ctx.arch_suffix}:latest"

    def local_name(self) -> str:
        return f"ray-cpp-wheel-py{self.ctx.python_version}{self.ctx.arch_suffix}:latest"


# ---------------------------------------------------------------------------
# Path / Binary Discovery
# ---------------------------------------------------------------------------


def get_repo_root() -> Path:
    """Get git repository root."""
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(result.stdout.strip())


def find_wanda() -> Optional[Path]:
    """Find wanda binary (checks WANDA_BIN env, PATH, then default location)."""
    if wanda := os.environ.get("WANDA_BIN"):
        return Path(wanda) if Path(wanda).exists() else None
    if wanda := shutil.which("wanda"):
        return Path(wanda)
    default = Path.home() / "rayci" / "bin" / "wanda"
    return default if default.exists() else None


# ---------------------------------------------------------------------------
# Platform Detection
# ---------------------------------------------------------------------------


def normalize_arch(arch: str = "") -> str:
    """Normalize architecture name (arm64 -> aarch64)."""
    if not arch:
        arch = platform.machine()
    return "aarch64" if arch == "arm64" else arch


def detect_platform() -> tuple[str, Optional[str]]:
    """Detect platform. Returns (host_os, target_platform for Docker/Wanda)."""
    host_os = platform.system().lower()
    if host_os != "darwin":
        return host_os, None
    arch = platform.machine()
    target = "linux/arm64" if arch == "arm64" else "linux/amd64"
    return host_os, os.environ.get("WANDA_PLATFORM", target)


# ---------------------------------------------------------------------------
# Environment Setup
# ---------------------------------------------------------------------------


def get_git_commit() -> str:
    """Get current git commit hash, or 'unknown' if not in a git repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def build_env(
    python_version: str = "3.10",
    manylinux_version: str = "",
    arch_suffix: str = "",
    hosttype: str = "",
) -> Dict[str, str]:
    """
    Create environment dict for wanda/docker builds.

    Returns a copy of os.environ with build variables set:
      - PYTHON_VERSION: Python version to build for (e.g., "3.11")
      - MANYLINUX_VERSION: Manylinux base image version
      - HOSTTYPE: Architecture (e.g., "x86_64", "aarch64")
      - ARCH_SUFFIX: Optional architecture suffix for image names
      - BUILDKITE_COMMIT: Git commit hash for wheel versioning
      - DOCKER_DEFAULT_PLATFORM: Docker platform (set on macOS)
      - WANDA_PLATFORM: Wanda target platform (set on macOS)
    """
    env = os.environ.copy()

    # Normalize hosttype (arm64 -> aarch64)
    hosttype = normalize_arch(hosttype)

    # Build variables
    env["PYTHON_VERSION"] = python_version
    env["MANYLINUX_VERSION"] = manylinux_version or DEFAULT_MANYLINUX_VERSION
    env["HOSTTYPE"] = hosttype
    env["ARCH_SUFFIX"] = arch_suffix

    # Git commit for wheel versioning (use existing or detect)
    if not env.get("BUILDKITE_COMMIT"):
        env["BUILDKITE_COMMIT"] = get_git_commit()

    # Platform variables (for cross-platform builds on macOS)
    _, target_platform = detect_platform()
    if target_platform:
        env.setdefault("DOCKER_DEFAULT_PLATFORM", target_platform)
        env.setdefault("WANDA_PLATFORM", target_platform)

    return env


# ---------------------------------------------------------------------------
# Context Creation
# ---------------------------------------------------------------------------


def create_context(
    python_version: str = "3.10",
    manylinux_version: str = "",
    arch_suffix: str = "",
    hosttype: str = "",
    dry_run: bool = False,
    repo_root: Optional[Path] = None,
    wanda_bin: Optional[Path] = None,
) -> BuildContext:
    """
    Create a configured BuildContext with isolated environment.

    This is the main entry point for setting up a build. It:
      1. Resolves paths (repo root, wanda binary)
      2. Creates an isolated env dict (copy of os.environ + build vars)
      3. Returns a BuildContext for use with build targets
    """
    if repo_root is None:
        repo_root = get_repo_root()

    return BuildContext(
        wanda_bin=wanda_bin or find_wanda(),
        repo_root=repo_root,
        dry_run=dry_run,
        env=build_env(
            python_version=python_version,
            manylinux_version=manylinux_version,
            arch_suffix=arch_suffix,
            hosttype=hosttype,
        ),
    )
