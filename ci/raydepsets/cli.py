import difflib
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

import click
import runfiles
from networkx import DiGraph, ancestors as networkx_ancestors, topological_sort

from ci.raydepsets.workspace import Depset, Workspace

DEFAULT_UV_FLAGS = """
    --generate-hashes
    --strip-extras
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
    "--check",
    is_flag=True,
    help="Check the the compiled dependencies are valid. Only compatible with generating all dependency sets.",
)
def build(
    config_path: str,
    workspace_dir: Optional[str],
    name: Optional[str],
    uv_cache_dir: Optional[str],
    check: Optional[bool],
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
        check=check,
    )
    manager.execute(name)
    if check:
        try:
            manager.diff_lock_files()
        except RuntimeError as e:
            click.echo(e, err=True)
            sys.exit(1)
        finally:
            manager.cleanup()


class DependencySetManager:
    def __init__(
        self,
        config_path: str = None,
        workspace_dir: Optional[str] = None,
        uv_cache_dir: Optional[str] = None,
        check: Optional[bool] = False,
    ):
        self.workspace = Workspace(workspace_dir)
        self.config = self.workspace.load_config(config_path)
        if check:
            self.temp_dir = tempfile.mkdtemp()
            self.output_paths = self.get_output_paths()
            self.copy_to_temp_dir()
        self.build_graph = DiGraph()
        self._build()
        self._uv_binary = _uv_binary()
        self._uv_cache_dir = uv_cache_dir

    def get_output_paths(self) -> List[Path]:
        output_paths = []
        for depset in self.config.depsets:
            output_paths.append(Path(depset.output))
        return output_paths

    def copy_to_temp_dir(self):
        """Copy the lock files from source file paths to temp dir."""
        for output_path in self.output_paths:
            source_fp, target_fp = self.get_source_and_dest(output_path)
            target_fp.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(
                source_fp,
                target_fp,
            )

    def get_diffs(self) -> List[str]:
        diffs = []
        for output_path in self.output_paths:
            new_lock_file_fp, old_lock_file_fp = self.get_source_and_dest(output_path)
            old_lock_file_contents = self.read_lock_file(old_lock_file_fp)
            new_lock_file_contents = self.read_lock_file(new_lock_file_fp)
            for diff in difflib.unified_diff(
                old_lock_file_contents,
                new_lock_file_contents,
                fromfile=new_lock_file_fp.as_posix(),
                tofile=old_lock_file_fp.as_posix(),
                lineterm="",
            ):
                diffs.append(diff)
        return diffs

    def diff_lock_files(self):
        diffs = self.get_diffs()
        if len(diffs) > 0:
            raise RuntimeError(
                "Lock files are not up to date. Please update lock files and push the changes.\n"
                + "".join(diffs)
            )
        click.echo("Lock files are up to date.")

    def get_source_and_dest(self, output_path: str) -> tuple[Path, Path]:
        return (self.get_path(output_path), (Path(self.temp_dir) / output_path))

    def _build(self):
        for depset in self.config.depsets:
            if depset.operation == "compile":
                self.build_graph.add_node(
                    depset.name, operation="compile", depset=depset, node_type="depset"
                )
            elif depset.operation == "subset":
                self.build_graph.add_node(
                    depset.name, operation="subset", depset=depset, node_type="depset"
                )
                self.build_graph.add_edge(depset.source_depset, depset.name)
            elif depset.operation == "expand":
                self.build_graph.add_node(
                    depset.name, operation="expand", depset=depset, node_type="depset"
                )
                for depset_name in depset.depsets:
                    self.build_graph.add_edge(depset_name, depset.name)
            else:
                raise ValueError(f"Invalid operation: {depset.operation}")
            if depset.pre_hooks:
                for ind, hook in enumerate(depset.pre_hooks):
                    hook_name = f"{depset.name}_pre_hook_{ind+1}"
                    self.build_graph.add_node(
                        hook_name,
                        operation="pre_hook",
                        pre_hook=hook,
                        node_type="pre_hook",
                    )
                    self.build_graph.add_edge(hook_name, depset.name)

    def subgraph_dependency_nodes(self, depset_name: str):
        dependency_nodes = networkx_ancestors(self.build_graph, depset_name)
        nodes = dependency_nodes | {depset_name}
        self.build_graph = self.build_graph.subgraph(nodes).copy()

    def execute(self, single_depset_name: Optional[str] = None):
        if single_depset_name:
            # check if the depset exists
            _get_depset(self.config.depsets, single_depset_name)
            self.subgraph_dependency_nodes(single_depset_name)

        for node in topological_sort(self.build_graph):
            node_type = self.build_graph.nodes[node]["node_type"]
            if node_type == "pre_hook":
                pre_hook = self.build_graph.nodes[node]["pre_hook"]
                self.execute_pre_hook(pre_hook)
            elif node_type == "depset":
                depset = self.build_graph.nodes[node]["depset"]
                self.execute_depset(depset)

    def exec_uv_cmd(
        self, cmd: str, args: List[str], stdin: Optional[bytes] = None
    ) -> str:
        cmd = [self._uv_binary, "pip", cmd, *args]
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, cwd=self.workspace.dir, input=stdin)
        if status.returncode != 0:
            raise RuntimeError(f"Failed to execute command: {cmd}")
        return status.stdout

    def execute_pre_hook(self, pre_hook: str):
        status_code = subprocess.call(pre_hook, cwd=self.workspace.dir)
        if status_code != 0:
            raise RuntimeError(f"Failed to execute pre-hook: {pre_hook}")
        click.echo(f"Executed pre-hook: {pre_hook}")
        return status_code

    def execute_depset(self, depset: Depset):
        if depset.operation == "compile":
            self.compile(
                constraints=depset.constraints,
                requirements=depset.requirements,
                name=depset.name,
                output=depset.output,
                append_flags=depset.append_flags,
                override_flags=depset.override_flags,
                packages=depset.packages,
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
        name: str,
        output: str,
        append_flags: Optional[List[str]] = None,
        override_flags: Optional[List[str]] = None,
        packages: Optional[List[str]] = None,
        requirements: Optional[List[str]] = None,
    ):
        """Compile a dependency set."""
        args = DEFAULT_UV_FLAGS.copy()
        stdin = None
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
        if packages:
            # need to add a dash to process stdin
            args.append("-")
            stdin = _get_bytes(packages)
        if output:
            args.extend(["-o", output])
        self.exec_uv_cmd("compile", args, stdin)

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

    def read_lock_file(self, file_path: Path) -> List[str]:
        if not file_path.exists():
            raise RuntimeError(f"Lock file {file_path} does not exist")
        with open(file_path, "r") as f:
            return f.readlines()

    def get_path(self, path: str) -> Path:
        return Path(self.workspace.dir) / path

    def check_subset_exists(self, source_depset: Depset, requirements: List[str]):
        for req in requirements:
            if req not in source_depset.requirements:
                raise RuntimeError(
                    f"Requirement {req} is not a subset of {source_depset.name}"
                )

    def cleanup(self):
        if self.temp_dir:
            shutil.rmtree(self.temp_dir)


def _get_bytes(packages: List[str]) -> bytes:
    return ("\n".join(packages) + "\n").encode("utf-8")


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
