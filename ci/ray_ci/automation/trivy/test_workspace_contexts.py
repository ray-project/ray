"""WORKSPACE may only load from packages that every partial build context copies.

The ray-core, ray-cpp-core and ray-java image builds copy WORKSPACE without
the rest of the repo. If WORKSPACE loads a .bzl from a package such a context
lacks, Bazel fails before building anything ("... is not a package"), and
every wheel and image build fails with it.
"""

import re
import sys
from pathlib import Path

import pytest
import runfiles
import yaml

# Build contexts that copy WORKSPACE. A new one must be added here too.
WANDA_CONTEXTS = (
    "ci/docker/ray-core.wanda.yaml",
    "ci/docker/ray-cpp-core.wanda.yaml",
    "ci/docker/ray-java.wanda.yaml",
)


def repo_file(path: str) -> Path:
    return Path(runfiles.Create().Rlocation(f"io_ray/{path}"))


def workspace_loads() -> list[tuple[str, str]]:
    """(package, file) for every load from this repo in WORKSPACE."""
    text = repo_file("WORKSPACE").read_text()
    return re.findall(r'^load\("//([^:"]*):([^"]+)"', text, re.MULTILINE)


def covered(path: str, srcs: list[str]) -> bool:
    return any(path == s or (s.endswith("/") and path.startswith(s)) for s in srcs)


def test_finds_the_trivy_pin_load():
    assert ("ci/ray_ci/automation/trivy/pin", "trivy_versions.bzl") in workspace_loads()


@pytest.mark.parametrize("context", WANDA_CONTEXTS)
def test_context_has_every_package_workspace_loads(context: str):
    srcs = yaml.safe_load(repo_file(context).read_text())["srcs"]
    assert "WORKSPACE" in srcs
    missing = []
    for package, name in workspace_loads():
        prefix = f"{package}/" if package else ""
        build_files = [f"{prefix}BUILD.bazel", f"{prefix}BUILD"]
        if not covered(f"{prefix}{name}", srcs) or not any(
            covered(b, srcs) for b in build_files
        ):
            missing.append(f"//{package}:{name}")
    assert not missing, f"{context} copies WORKSPACE but not {missing}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
