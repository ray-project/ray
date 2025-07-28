import click
from pathlib import Path
from ci.raydepsets.workspace import Workspace, Depset
from typing import List
import subprocess
import platform
import runfiles

DEFAULT_UV_FLAGS = [
    "--generate-hashes",
    "--strip-extras",
    "--python-version=3.11",
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
    manager = DependencySetManager(config_path=config_path, workspace_dir=workspace_dir)
    if name:
        manager.execute_single(manager.get_depset(name))
    else:
        manager.execute_all()


class DependencySetManager:
    def __init__(
        self,
        config_path: Path = Path(__file__).parent / "depset.config.yaml",
        workspace_dir: str = None,
    ):
        self.workspace = Workspace(workspace_dir)
        self.config = self.workspace.load_config(config_path)

    def get_depset(self, name: str) -> Depset:
        for depset in self.config.depsets:
            if depset.name == name:
                return depset
        raise KeyError(f"Dependency set {name} not found")

    def exec_uv_cmd(self, cmd: str, args: List[str]) -> str:
        cmd = f"{uv_binary()} pip {cmd} {' '.join(args)}"
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, shell=True)
        if status.returncode != 0:
            raise RuntimeError(f"Failed to execute command: {cmd}")
        return status.stdout

    def execute_all(self):
        for depset in self.config.depsets:
            self.execute_single(depset)

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
                args=DEFAULT_UV_FLAGS.copy(),
                name=depset.name,
                output=depset.output,
            )
        elif depset.operation == "expand":
            self.expand(
                depsets=depset.depsets,
                constraints=depset.constraints,
                args=DEFAULT_UV_FLAGS.copy(),
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
        args: List[str],
        name: str,
        output: str = None,
    ):
        """Subset a dependency set."""
        source_depset = self.get_depset(source_depset)
        self.check_subset_exists(source_depset, requirements)
        self.compile(
            constraints=[source_depset.output],
            requirements=requirements,
            args=args,
            name=name,
            output=output,
        )

    def expand(
        self,
        depsets: List[str],
        constraints: List[str],
        args: List[str],
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
            args=args,
            name=name,
            output=output,
        )

    def get_path(self, path: str) -> str:
        return (Path(self.workspace.dir) / path).as_posix()

    def check_subset_exists(self, source_depset: Depset, requirements: List[str]):
        for req in requirements:
            if req not in source_depset.requirements:
                raise RuntimeError(
                    f"Requirement {req} is not a subset of {source_depset.name}"
                )


def uv_binary():
    r = runfiles.Create()
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise RuntimeError(
            f"Unsupported platform/processor: {system}/{platform.processor()}"
        )
    return r.Rlocation("uv_x86_64/uv-x86_64-unknown-linux-gnu/uv")
