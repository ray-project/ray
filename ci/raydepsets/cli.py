import click
from pathlib import Path
from ci.raydepsets.workspace import Workspace, Depset
from typing import List
import subprocess
import platform
import runfiles
from networkx import DiGraph, topological_sort

DEFAULT_UV_FLAGS = [
    "--generate-hashes",
    "--strip-extras",
    "--no-strip-markers",
    "--emit-index-url",
    "--emit-find-links",
    "--unsafe-package ray",
    "--unsafe-package grpcio-tools",
    "--unsafe-package setuptools",
    "--index-url https://pypi.org/simple",
    "--extra-index-url https://download.pytorch.org/whl/cpu",
    "--index-strategy unsafe-best-match",
    "--quiet",
    "--python-version=3.11",
]


@click.group(name="raydepsets")
def cli():
    """Manage Python dependency sets."""


@cli.command()
@click.argument("config_path", default="ci/raydepsets/depset.config.yaml")
@click.option("--workspace-dir", default=None)
@click.option("--name", default=None)
def load(config_path: str, workspace_dir: str, name: str):
    """Load a dependency sets from a config file."""
    # install and pin python 3.11
    subprocess.run(["uv", "python", "install", "3.11"])
    subprocess.run(["uv", "python", "pin", "3.11"])
    manager = DependencySetManager(config_path=config_path, workspace_dir=workspace_dir)
    if name:
        manager.execute_single(manager.get_depset(name))
    else:
        manager.execute()


class DependencySetManager:
    def __init__(
        self,
        config_path: Path = Path(__file__).parent / "depset.config.yaml",
        workspace_dir: str = None,
    ):
        self.workspace = Workspace(workspace_dir)
        self.config = self.workspace.load_config(config_path)
        self.build_graph = DiGraph()
        self._build()

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
        cmd = f"{uv_binary()} pip {cmd} {' '.join(args)}"
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, shell=True, cwd=self.workspace.dir)
        if status.returncode != 0:
            raise RuntimeError(f"Failed to execute command: {cmd}")
        return status.stdout

    def execute_single(self, depset: Depset):
        if depset.operation == "compile":
            self.compile(
                constraints=depset.constraints,
                requirements=depset.requirements,
                args=DEFAULT_UV_FLAGS.copy(),
                name=depset.name,
                output=depset.output,
            )
        elif depset.operation == "subset":
            self.subset(
                source_depset=depset.source_depset,
                requirements=depset.requirements,
                name=depset.name,
                output=depset.output,
            )
        elif depset.operation == "expand":
            self.expand(
                depsets=depset.depsets,
                constraints=depset.constraints,
                name=depset.name,
                output=depset.output,
            )
        click.echo(f"Dependency set {depset.name} compiled successfully")

    def compile(
        self,
        constraints: List[str],
        requirements: List[str],
        args: List[str],
        name: str,
        output: str,
    ):
        """Compile a dependency set."""
        args = self.override_uv_flags(args)
        if constraints:
            for constraint in constraints:
                args.extend(["-c", constraint])
        if requirements:
            for requirement in requirements:
                args.extend([requirement])
        args.extend(["-o", output])
        self.exec_uv_cmd("compile", args)

    def subset(
        self,
        source_depset: str,
        requirements: List[str],
        name: str,
        output: str = None,
    ):
        """Subset a dependency set."""
        source_depset = self.get_depset(source_depset)
        self.compile(
            constraints=[source_depset.output],
            requirements=requirements,
            args=source_depset.extra_flags,
            name=name,
            output=output,
        )

    def expand(
        self,
        depsets: List[str],
        constraints: List[str],
        name: str,
        output: str = None,
    ):
        """Expand a dependency set."""
        depset_req_list = []
        for depset_name in depsets:
            depset = self.get_depset(depset_name)
            depset_req_list.extend(depset.requirements)
        self.compile(
            constraints=constraints,
            requirements=depset_req_list,
            args=depset.extra_flags,
            name=name,
            output=output,
        )

    def override_uv_flags(self, flags: List[str]):
        found = False
        new_flags = DEFAULT_UV_FLAGS.copy()
        for flag in flags:
            flag_name = flag.split(" ")[0]
            if len(flag_name) == 0:
                flag_name = flag.split("=")[0]
            for i, default_flag in enumerate(new_flags):
                if flag_name in default_flag:
                    new_flags[i] = flag
                    found = True
                    break
        if not found:
            new_flags.extend(flags)
        return new_flags


def uv_binary():
    r = runfiles.Create()
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise RuntimeError(
            f"Unsupported platform/processor: {system}/{platform.processor()}"
        )
    return r.Rlocation("uv_x86_64/uv-x86_64-unknown-linux-gnu/uv")
