import platform
import subprocess
from pathlib import Path
from typing import List, Optional

import click
import runfiles
from networkx import DiGraph, topological_sort
import tempfile
import difflib
import shutil
import sys

from ci.raydepsets.workspace import Depset, Workspace

DEFAULT_UV_FLAGS = """
    --generate-hashes
    --strip-extras
    --unsafe-package ray
    --unsafe-package setuptools
    --index-url https://pypi.org/simple
    --extra-index-url https://download.pytorch.org/whl/cpu
    --index-strategy unsafe-best-match
    --no-strip-markers
    --emit-index-url
    --emit-find-links
    --quiet
""".split()


@click.group(name="raydepsets")
def cli():
    """Manage Python dependency sets."""


@cli.command()
@click.argument("config_path", default="ci/raydepsets/ray.depsets.yaml")
@click.option(
    "--workspace-dir",
    default=None,
    help="The path to the workspace directory. If not specified, $BUILD_WORKSPACE_DIRECTORY will be used.",
)
@click.option(
    "--name",
    default=None,
    help="The name of the dependency set to load. If not specified, all dependency sets will be loaded.",
)
@click.option(
    "--uv-cache-dir", default=None, help="The directory to cache uv dependencies"
)
@click.option(
    "--verify",
    is_flag=True,
    help="Verify the the compiled dependencies are valid. Only compatible with generating all dependency sets.",
)
def build(
    config_path: str,
    workspace_dir: Optional[str],
    name: Optional[str],
    uv_cache_dir: Optional[str],
    verify: bool = False,
):
    """
    Build dependency sets from a config file.
    Args:
        config_path: The path to the config file. If not specified, ci/raydepsets/ray.depsets.yaml will be used.
    """
    manager = DependencySetManager(
        config_path=config_path,
        workspace_dir=workspace_dir,
        uv_cache_dir=uv_cache_dir,
    )
    if verify:
        copy_lock_files_to_temp_dir(manager)
    if name:
        manager.execute_single(_get_depset(manager.config.depsets, name))
    else:
        manager.execute()
    if verify:
        diff_lock_files(manager)


def copy_lock_files_to_temp_dir(manager: "DependencySetManager"):
    """Copy the lock files to a temp directory."""
    # create temp directory
    manager.temp_dir = tempfile.mkdtemp()
    # copy existing lock files to temp directory
    for depset in manager.config.depsets:
        existing_lock_file_path = manager.get_path(depset.output)
        new_lock_file_path = Path(manager.temp_dir) / depset.output
        new_lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(existing_lock_file_path, new_lock_file_path)


def diff_lock_files(manager: "DependencySetManager"):
    """Diff the lock files."""
    for depset in manager.config.depsets:
        current_lock_file_path = depset.output
        if not Path(manager.get_path(current_lock_file_path)).exists():
            raise RuntimeError(f"Lock file {current_lock_file_path} does not exist")
        with open(manager.get_path(current_lock_file_path), "r") as f:
            current_lock_file_contents = f.read()
        new_lock_file_path = Path(manager.temp_dir) / depset.output
        if not new_lock_file_path.exists():
            raise RuntimeError(f"Lock file {new_lock_file_path} does not exist")
        with open(new_lock_file_path, "r") as f:
            new_lock_file_contents = f.read()
        diff = difflib.unified_diff(
            current_lock_file_contents.splitlines(),
            new_lock_file_contents.splitlines(),
        )
        diffs = list(diff)
        if len(diffs) > 0:
            click.echo(f"Lock file {current_lock_file_path} is not up to date.")
            click.echo("".join(diffs))
            sys.exit(1)
        else:
            click.echo(f"Lock file {current_lock_file_path} is the same.")

    click.echo("Verification complete.")
    shutil.rmtree(manager.temp_dir)


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
            args.extend(_flatten_flags(append_flags))
        if constraints:
            for constraint in constraints:
                args.extend(["-c", constraint])
        if requirements:
            for requirement in requirements:
                args.extend([requirement])
        if output:
            args.extend(["-o", output])
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
        source_depset = _get_depset(self.config.depsets, source_depset)
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
            depset = _get_depset(self.config.depsets, depset_name)
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


def _get_depset(depsets: List[Depset], name: str) -> Depset:
    for depset in depsets:
        if depset.name == name:
            return depset
    raise KeyError(f"Dependency set {name} not found")


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


def _uv_binary():
    r = runfiles.Create()
    system = platform.system()
    processor = platform.processor()

    if system == "Linux" and processor == "x86_64":
        return r.Rlocation("uv_x86_64-linux/uv-x86_64-unknown-linux-gnu/uv")
    elif system == "Darwin" and (processor == "arm" or processor == "aarch64"):
        return r.Rlocation("uv_aarch64-darwin/uv-aarch64-apple-darwin/uv")
    else:
        raise RuntimeError(f"Unsupported platform/processor: {system}/{processor}")
