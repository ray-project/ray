import platform
import subprocess
from pathlib import Path
from typing import List, Optional

import click
import runfiles
from networkx import DiGraph, topological_sort

from ci.raydepsets.workspace import Depset, Workspace

DEFAULT_UV_FLAGS = """
    --generate-hashes
    --strip-extras
    --no-strip-markers
    --emit-index-url
    --emit-find-links
    --unsafe-package ray
    --unsafe-package grpcio-tools
    --unsafe-package setuptools
    --index-url https://pypi.org/simple
    --extra-index-url https://download.pytorch.org/whl/cpu
    --index-strategy unsafe-best-match
    --quiet
""".split()


@click.group(name="raydepsets")
def cli():
    """Manage Python dependency sets."""


@cli.command()
@click.argument("config_path", default="ci/raydepsets/ray.depsets.yaml")
@click.option("--workspace-dir", default=None)
@click.option("--name", default=None)
@click.option("--uv-cache-dir", default=None)
def load(
    config_path: str,
    workspace_dir: Optional[str],
    name: Optional[str],
    uv_cache_dir: Optional[str],
):
    """Load a dependency sets from a config file."""
    manager = DependencySetManager(
        config_path=config_path,
        workspace_dir=workspace_dir,
        uv_cache_dir=uv_cache_dir,
    )
    if name:
        manager.execute_single(manager.get_depset(name))
    else:
        manager.execute()


class DependencySetManager:
    def __init__(
        self,
        config_path: str = None,
        workspace_dir: Optional[str] = None,
        uv_cache_dir: Optional[str] = None,
    ):
        self.workspace = Workspace(workspace_dir)
        self.config = self.workspace.load_config(config_path)
        self.build_graph = DiGraph()
        self._build()
        self._uv_binary = _uv_binary()
        self._uv_cache_dir = uv_cache_dir

    def _build(self):
        for depset in self.config.depsets:
            if depset.operation == "compile":
                self.build_graph.add_node(
                    depset.name, operation="compile", depset=depset
                )
            elif depset.operation == "subset":
                self.build_graph.add_node(
                    depset.name, operation="subset", depset=depset
                )
                self.build_graph.add_edge(depset.source_depset, depset.name)
            elif depset.operation == "expand":
                self.build_graph.add_node(
                    depset.name, operation="expand", depset=depset
                )
                for depset_name in depset.depsets:
                    self.build_graph.add_edge(depset_name, depset.name)
            else:
                raise ValueError(f"Invalid operation: {depset.operation}")

    def execute(self):
        for node in topological_sort(self.build_graph):
            depset = self.build_graph.nodes[node]["depset"]
            self.execute_single(depset)

    def get_depset(self, name: str) -> Depset:
        for depset in self.config.depsets:
            if depset.name == name:
                return depset
        raise KeyError(f"Dependency set {name} not found")

    def exec_uv_cmd(self, cmd: str, args: List[str]) -> str:
        cmd = [self._uv_binary, "pip", cmd, *args]
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, cwd=self.workspace.dir)
        if status.returncode != 0:
            raise RuntimeError(f"Failed to execute command: {cmd}")
        return status.stdout

    def execute_single(self, depset: Depset):
        if depset.operation == "compile":
            self.compile(
                constraints=depset.constraints,
                requirements=depset.requirements,
                name=depset.name,
                output=depset.output,
                append_flags=depset.append_flags,
                override_flags=depset.override_flags,
            )
        elif depset.operation == "subset":
            self.subset(
                source_depset=depset.source_depset,
                requirements=depset.requirements,
                append_flags=depset.append_flags,
                override_flags=depset.override_flags,
                name=depset.name,
                output=depset.output,
            )
        elif depset.operation == "expand":
            self.expand(
                depsets=depset.depsets,
                requirements=depset.requirements,
                constraints=depset.constraints,
                append_flags=depset.append_flags,
                override_flags=depset.override_flags,
                name=depset.name,
                output=depset.output,
            )
        click.echo(f"Dependency set {depset.name} compiled successfully")

    def compile(
        self,
        constraints: List[str],
        requirements: List[str],
        name: str,
        output: str,
        append_flags: Optional[List[str]] = None,
        override_flags: Optional[List[str]] = None,
    ):
        """Compile a dependency set."""
        args = DEFAULT_UV_FLAGS.copy()
        if self._uv_cache_dir:
            args.extend(["--cache-dir", self._uv_cache_dir])
        if override_flags:
            args = _override_uv_flags(override_flags, args)
        if append_flags:
            args = _append_uv_flags(append_flags, args)
        if constraints:
            for constraint in constraints:
                args.extend(["-c", self.get_path(constraint)])
        if requirements:
            for requirement in requirements:
                args.extend([self.get_path(requirement)])
        if output:
            args.extend(["-o", self.get_path(output)])
        self.exec_uv_cmd("compile", args)

    def subset(
        self,
        source_depset: str,
        requirements: List[str],
        name: str,
        output: str = None,
        append_flags: Optional[List[str]] = None,
        override_flags: Optional[List[str]] = None,
    ):
        """Subset a dependency set."""
        source_depset = self.get_depset(source_depset)
        self.check_subset_exists(source_depset, requirements)
        self.compile(
            constraints=[source_depset.output],
            requirements=requirements,
            name=name,
            output=output,
            append_flags=append_flags,
            override_flags=override_flags,
        )

    def expand(
        self,
        depsets: List[str],
        requirements: List[str],
        constraints: List[str],
        name: str,
        output: str = None,
        append_flags: Optional[List[str]] = None,
        override_flags: Optional[List[str]] = None,
    ):
        """Expand a dependency set."""
        # handle both depsets and requirements
        depset_req_list = []
        for depset_name in depsets:
            depset = self.get_depset(depset_name)
            depset_req_list.extend(depset.requirements)
        if requirements:
            depset_req_list.extend(requirements)
        self.compile(
            constraints=constraints,
            requirements=depset_req_list,
            name=name,
            output=output,
            append_flags=append_flags,
            override_flags=override_flags,
        )

    def get_path(self, path: str) -> str:
        return (Path(self.workspace.dir) / path).as_posix()

    def check_subset_exists(self, source_depset: Depset, requirements: List[str]):
        for req in requirements:
            if req not in source_depset.requirements:
                raise RuntimeError(
                    f"Requirement {req} is not a subset of {source_depset.name}"
                )


def _flatten_flags(flags: List[str]) -> List[str]:
    """
    Flatten a list of flags into a list of strings.
    For example, ["--find-links https://pypi.org/simple"] will be flattened to
    ["--find-links", "https://pypi.org/simple"].
    """
    flattened_flags = []
    for flag in flags:
        flattened_flags.extend(flag.split())
    return flattened_flags


def _override_uv_flags(flags: List[str], args: List[str]) -> List[str]:
    flag_names = {f.split()[0] for f in flags if f.startswith("--")}
    new_args = []
    skip_next = False
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg in flag_names:
            skip_next = True
            continue
        new_args.append(arg)

    return new_args + _flatten_flags(flags)


def _append_uv_flags(flags: List[str], args: List[str]) -> List[str]:
    args.extend(flags)
    return args


def _uv_binary():
    r = runfiles.Create()
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise RuntimeError(
            f"Unsupported platform/processor: {system}/{platform.processor()}"
        )
    return r.Rlocation("uv_x86_64/uv-x86_64-unknown-linux-gnu/uv")
