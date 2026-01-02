"""
Build targets for Wanda. Primarily used for managing dependency
images in local development.
"""

from __future__ import annotations

import os
import platform
import re
import shutil
import subprocess
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, Optional, Sequence, Set, Type

IMAGE_PREFIX = "cr.ray.io/rayproject"
DEFAULT_MANYLINUX_VERSION = "251216.3835fc5"
ENV_VAR_PATTERN = r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)"
DEFAULT_PYTHON_VERSION = "3.10"


# ---------------------------------------------------------------------------
# Wanda Spec Parsing
# ---------------------------------------------------------------------------


@lru_cache(maxsize=32)
def parse_wanda_name(spec_path: Path) -> str:
    """
    Extract the 'name' field from a wanda YAML spec file.
    Uses simple string parsing to avoid requiring PyYAML dependency.
    Handles: `name: value`, `name: "value"`, `name: 'value'`
    Raises:
        ValueError: If no 'name:' field is found in the spec file.
    """
    NAME_FIELD = "name:"
    with open(spec_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith(NAME_FIELD):
                value = line[len(NAME_FIELD) :].strip().rsplit(" #", 1)[0].strip()
                # Remove surrounding quotes if present
                if (value.startswith('"') and value.endswith('"')) or (
                    value.startswith("'") and value.endswith("'")
                ):
                    value = value[1:-1]
                return value
    raise ValueError(f"No 'name:' field found in {spec_path}")


def extract_env_var_names(template: str) -> Set[str]:
    """
    Extract environment variable names from a template string.
    Returns set of variable names referenced via $VAR or ${VAR} syntax.
    """
    return {
        name
        for match in re.findall(ENV_VAR_PATTERN, template)
        for name in match
        if name
    }


def expand_env_vars(template: str, env: Dict[str, str]) -> str:
    """
    Expand environment variables in a template string.
    Supports both $VAR and ${VAR} syntax.
    """
    # Use a regex-based approach to handle $VAR syntax (without braces)
    def replace(match: re.Match) -> str:
        var_name = match.group(1) or match.group(2)
        return env.get(var_name, "")

    # Match ${VAR} or $VAR (where VAR is alphanumeric + underscore)
    return re.sub(ENV_VAR_PATTERN, replace, template)


def get_wanda_image_name(spec_path: Path, env: Dict[str, str]) -> str:
    """
    Get the expanded image name from a wanda spec file.
    Parses the spec, extracts the 'name' field, and expands env vars.
    """
    name_template = parse_wanda_name(spec_path)
    return expand_env_vars(name_template, env)


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


@dataclass
class BuildContext:
    """Shared context for all build steps."""

    wanda_bin: Path
    repo_root: Path
    env: Dict[str, str] = field(default_factory=dict, repr=False)
    _run_cmd: Optional[Callable[..., subprocess.CompletedProcess[str]]] = field(
        default=None, repr=False
    )
    _built_targets: Set[str] = field(default_factory=set, repr=False)

    @property
    def python_version(self) -> str:
        return self.env.get("PYTHON_VERSION", DEFAULT_PYTHON_VERSION)

    @property
    def arch_suffix(self) -> str:
        return self.env.get("ARCH_SUFFIX", "")

    def run(
        self, cmd: Sequence[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        """Run a command from repo root."""
        kwargs.setdefault("cwd", self.repo_root)
        kwargs.setdefault("env", self.env)
        kwargs.setdefault("text", True)

        cmd_list = list(cmd)

        if self._run_cmd:
            return self._run_cmd(cmd_list, **kwargs)

        return subprocess.run(cmd_list, check=True, **kwargs)

    def wanda(self, spec_file: str) -> None:
        """Run wanda with a spec file."""
        self.run([str(self.wanda_bin), spec_file], stdout=sys.stdout, stderr=sys.stderr)

    def docker_tag(self, src: str, dst: str) -> None:
        """Tag a docker image."""
        self.run(["docker", "tag", src, dst], stdout=sys.stdout, stderr=sys.stderr)


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------


class BuildTarget(ABC):
    """Base class for build targets."""

    SPEC: ClassVar[str]
    DEPS: ClassVar[Sequence[Type["BuildTarget"]]] = ()

    def __init__(self, ctx: BuildContext):
        self.ctx = ctx

    def build(self, _stack: Optional[Set[str]] = None) -> None:
        """Build dependencies then this target (skips if already built)."""
        _stack = _stack or set()
        name = type(self).__name__

        if name in self.ctx._built_targets:
            return
        if name in _stack:
            # Keep the message helpful without relying on set ordering.
            raise RuntimeError(f"Dependency cycle detected at {name}")

        _stack.add(name)
        for dep_cls in self.DEPS:
            dep_cls(self.ctx).build(_stack)
        self._build()
        self.ctx._built_targets.add(name)
        _stack.remove(name)

    @abstractmethod
    def _build(self) -> None:
        """Execute the actual build (override in subclasses)."""
        raise NotImplementedError

    def header(self, msg: str) -> None:
        """Print a blue header."""
        print(f"\n\033[34;1m===> {msg}\033[0m", file=sys.stderr)


class WandaDockerTarget(BuildTarget):
    """Common pattern: wanda spec build + docker tag remote -> local."""

    @property
    def _image_name(self) -> str:
        """Get the expanded image name from the wanda spec."""
        spec_path = self.ctx.repo_root / self.SPEC
        return get_wanda_image_name(spec_path, self.ctx.env)

    @property
    def remote_image(self) -> str:
        """Full remote image name with registry prefix."""
        return f"{IMAGE_PREFIX}/{self._image_name}:latest"

    @property
    def local_image(self) -> str:
        """Local image name (no registry prefix)."""
        return f"{self._image_name}:latest"

    def _build(self) -> None:
        self.header(f"Building {type(self).__name__}...")
        self.ctx.wanda(self.SPEC)
        self.ctx.docker_tag(self.remote_image, self.local_image)


class RayCore(WandaDockerTarget):
    SPEC = "ci/docker/ray-core.wanda.yaml"


class RayDashboard(WandaDockerTarget):
    SPEC = "ci/docker/ray-dashboard.wanda.yaml"


class RayJava(WandaDockerTarget):
    SPEC = "ci/docker/ray-java.wanda.yaml"


class RayWheel(WandaDockerTarget):
    SPEC = "ci/docker/ray-wheel.wanda.yaml"
    DEPS = (RayCore, RayDashboard, RayJava)


class RayCppCore(WandaDockerTarget):
    SPEC = "ci/docker/ray-cpp-core.wanda.yaml"


class RayCppWheel(WandaDockerTarget):
    SPEC = "ci/docker/ray-cpp-wheel.wanda.yaml"
    DEPS = (RayCore, RayCppCore, RayJava, RayDashboard)


TARGETS: Dict[str, Type[BuildTarget]] = {
    "ray-core": RayCore,
    "ray-dashboard": RayDashboard,
    "ray-java": RayJava,
    "ray-wheel": RayWheel,
    "ray-cpp-core": RayCppCore,
    "ray-cpp-wheel": RayCppWheel,
}


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
    """Find wanda binary (checks WANDA_BIN env var, then PATH)."""
    if wanda := os.environ.get("WANDA_BIN"):
        p = Path(wanda)
        return p if p.exists() else None
    if wanda_path := shutil.which("wanda"):
        return Path(wanda_path)
    return None


WANDA_VERSION = "v0.22.0"
WANDA_RELEASE_URL = (
    f"https://github.com/ray-project/rayci/releases/download/{WANDA_VERSION}"
)


def get_wanda_download_url() -> str:
    """Get the wanda download URL for the current platform."""
    system = platform.system().lower()
    arch = platform.machine().lower()

    if system == "darwin":
        if arch != "arm64":
            raise RuntimeError(
                f"Unsupported platform: darwin/{arch} (only arm64 is supported)"
            )
        suffix = "darwin-arm64"
    elif system == "windows":
        suffix = "windows-amd64"
    else:
        # Linux
        suffix = "linux-arm64" if arch in ("arm64", "aarch64") else "linux-amd64"

    return f"{WANDA_RELEASE_URL}/wanda-{suffix}"


# ---------------------------------------------------------------------------
# Platform Detection
# ---------------------------------------------------------------------------


def normalize_arch(arch: str = "") -> str:
    """Normalize architecture name (arm64 -> aarch64, amd64 synonyms)."""
    if not arch:
        arch = platform.machine()

    a = arch.lower()
    if a in {"arm64"}:
        return "aarch64"
    if a in {"x64", "amd64"}:
        return "x86_64"
    return a


def detect_target_platform() -> Optional[str]:
    """Detect Docker/Wanda target platform (only needed on macOS)."""
    if platform.system().lower() != "darwin":
        return None

    arch = platform.machine().lower()
    default_target = "linux/arm64" if arch == "arm64" else "linux/amd64"
    return os.environ.get("WANDA_PLATFORM", default_target)


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
    python_version: str = DEFAULT_PYTHON_VERSION,
    manylinux_version: str = "",
    arch_suffix: str = "",
    hosttype: str = "",
) -> Dict[str, str]:
    """
    Create environment dict for wanda/docker builds.
    Returns a copy of os.environ with build variables set:
      - PYTHON_VERSION
      - MANYLINUX_VERSION
      - HOSTTYPE
      - ARCH_SUFFIX
      - BUILDKITE_COMMIT (if missing)
      - DOCKER_DEFAULT_PLATFORM / WANDA_PLATFORM (on macOS, if not already set)
    """
    env = os.environ.copy()

    hosttype_norm = normalize_arch(hosttype)

    env["PYTHON_VERSION"] = python_version
    env["MANYLINUX_VERSION"] = manylinux_version or DEFAULT_MANYLINUX_VERSION
    env["HOSTTYPE"] = hosttype_norm
    env["ARCH_SUFFIX"] = arch_suffix

    if not env.get("BUILDKITE_COMMIT"):
        env["BUILDKITE_COMMIT"] = get_git_commit()

    # Use read-only cache for local builds
    env.setdefault("BUILDKITE_CACHE_READONLY", "true")

    # Platform variables (for cross-platform builds on macOS)
    target_platform = detect_target_platform()
    if target_platform:
        env.setdefault("DOCKER_DEFAULT_PLATFORM", target_platform)
        env.setdefault("WANDA_PLATFORM", target_platform)

    return env


# ---------------------------------------------------------------------------
# Context Creation (fail fast)
# ---------------------------------------------------------------------------


def create_context(
    python_version: str = DEFAULT_PYTHON_VERSION,
    manylinux_version: str = "",
    arch_suffix: str = "",
    hosttype: str = "",
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
    if not repo_root.exists():
        raise FileNotFoundError(f"repo_root does not exist: {repo_root}")

    wanda = wanda_bin or find_wanda()
    if wanda is None:
        url = get_wanda_download_url()
        raise FileNotFoundError(
            "Could not find 'wanda'. Set WANDA_BIN or ensure 'wanda' is on PATH.\n"
            f"Download from https://github.com/ray-project/rayci/releases ({WANDA_VERSION})\n"
            f"\n"
            f"Example:\n"
            f"  mkdir -p ~/.local/bin\n"
            f"  curl -fsSL -o ~/.local/bin/wanda {url}\n"
            f"  chmod +x ~/.local/bin/wanda\n"
            f"  wanda --version"
        )
    if not wanda.exists():
        raise FileNotFoundError(f"wanda binary does not exist: {wanda}")

    env = build_env(
        python_version=python_version,
        manylinux_version=manylinux_version,
        arch_suffix=arch_suffix,
        hosttype=hosttype,
    )

    return BuildContext(
        wanda_bin=wanda,
        repo_root=repo_root,
        env=env,
    )
