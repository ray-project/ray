"""WORKSPACE may only load from packages that every partial build context copies.

The ray-core, ray-cpp-core and ray-java image builds copy WORKSPACE without
the rest of the repo. If WORKSPACE, or the Trivy pin it loads, loads a .bzl
from a package such a context lacks, Bazel fails before building anything
("... is not a package"), and every wheel and image build fails with it.
"""

import re
import sys
from pathlib import Path

import pytest
import runfiles
import yaml

PIN = "ci/ray_ci/automation/trivy/pin/trivy_versions.bzl"
# The build contexts that copy WORKSPACE today. Any other wanda spec in
# ci/docker/ that copies it is checked too.
KNOWN_CONTEXTS = {
    "ci/docker/ray-core.wanda.yaml",
    "ci/docker/ray-cpp-core.wanda.yaml",
    "ci/docker/ray-java.wanda.yaml",
}
# A load from this repo, written "//pkg:file", "@//pkg:file" or
# "@io_ray//pkg:file", on one line or several.
LOAD = re.compile(r'\bload\(\s*"(?:@io_ray|@)?//([^:"]*):([^"]+)"')


def repo_file(path: str) -> Path:
    return Path(runfiles.Create().Rlocation(f"io_ray/{path}"))


def repo_loads(path: str) -> list[tuple[str, str]]:
    """(package, file) for every load from this repo in ``path``."""
    return LOAD.findall(repo_file(path).read_text())


def workspace_loads() -> list[tuple[str, str]]:
    # The pin's own loads too: a change only to the pin runs no image builds
    # in premerge, so a load it gained would first fail on postmerge.
    return repo_loads("WORKSPACE") + repo_loads(PIN)


def srcs(spec: str) -> list[str]:
    return (yaml.safe_load(repo_file(spec).read_text()) or {}).get("srcs") or []


def contexts_with_workspace() -> list[str]:
    """Every wanda spec in ci/docker/ that copies WORKSPACE."""
    # The //:wanda_specs files share this directory in the runfiles.
    docker = repo_file("ci/docker/ray-core.wanda.yaml").parent
    specs = sorted(f"ci/docker/{p.name}" for p in docker.glob("*.wanda.yaml"))
    return [spec for spec in specs if "WORKSPACE" in srcs(spec)]


def missing(context_srcs: list[str], loads: list[tuple[str, str]]) -> list[str]:
    """The loads whose file, or its package's BUILD file, the context lacks."""

    def covered(path: str) -> bool:
        return any(
            path == s or (s.endswith("/") and path.startswith(s)) for s in context_srcs
        )

    gaps = []
    for package, name in loads:
        prefix = f"{package}/" if package else ""
        has_build = covered(f"{prefix}BUILD.bazel") or covered(f"{prefix}BUILD")
        if not covered(f"{prefix}{name}") or not has_build:
            gaps.append(f"//{package}:{name}")
    return gaps


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ('load("//a/b:c.bzl", "x")', [("a/b", "c.bzl")]),
        ('load("@//a/b:c.bzl", "x")', [("a/b", "c.bzl")]),
        ('load("@io_ray//a/b:c.bzl", "x")', [("a/b", "c.bzl")]),
        ('load(\n    "//a/b:c.bzl",\n    "x",\n)', [("a/b", "c.bzl")]),
        ('load("@bazel_tools//tools:http.bzl", "x")', []),
    ],
)
def test_load_pattern(text: str, expected: list):
    assert LOAD.findall(text) == expected


CONTEXT_SRCS = [
    "WORKSPACE",
    "ci/ray_ci/automation/trivy/pin/",
    "java/BUILD.bazel",
    "java/dependencies.bzl",
    "tools/defs.bzl",
]


@pytest.mark.parametrize(
    ("load", "expected"),
    [
        (("ci/ray_ci/automation/trivy/pin", "trivy_versions.bzl"), []),
        (("java", "dependencies.bzl"), []),
        (("java", "other.bzl"), ["//java:other.bzl"]),
        # The file is copied but its package's BUILD file isn't.
        (("tools", "defs.bzl"), ["//tools:defs.bzl"]),
    ],
)
def test_missing(load: tuple, expected: list):
    assert missing(CONTEXT_SRCS, [load]) == expected


def test_finds_the_trivy_pin_and_every_known_context():
    assert ("ci/ray_ci/automation/trivy/pin", "trivy_versions.bzl") in workspace_loads()
    assert KNOWN_CONTEXTS <= set(contexts_with_workspace())


@pytest.mark.parametrize("context", contexts_with_workspace())
def test_context_has_every_package_workspace_loads(context: str):
    gaps = missing(srcs(context), workspace_loads())
    assert not gaps, f"{context} copies WORKSPACE but not {gaps}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
