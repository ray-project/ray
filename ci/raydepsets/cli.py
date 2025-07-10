import click
from pathlib import Path
from typing import Dict, List, Optional
import subprocess
from ci.raydepsets.workspace import Depset, Workspace

DEFAULT_UV_FLAGS = [
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
@click.option("--name", type=str, default="")
def load(config_path: str, name: str = ""):
    """Load a dependency sets from a config file."""
    manager = DependencySetManager(config_path=config_path)
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

    def exec_uv_cmd(self, cmd: str, args: List[str]) -> str:
        cmd = f"uv pip {cmd} {' '.join(args)}"
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, shell=True)
        if status.returncode != 0:
            raise Exception(f"Failed to execute command: {cmd}")
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
            click.echo(f"Dependency set {depset.name} compiled successfully")

    def compile(
        self,
        constraints: List[str],
        requirements: List[str],
        args: List[str],
        name: str,
        output: str = None,
    ):
        """Compile a dependency set."""
        if constraints:
            for constraint in constraints:
                args.extend(["-c", self.get_absolute_path(constraint)])
        if requirements:
            for requirement in requirements:
                args.append(self.get_absolute_path(requirement))
        args.extend(["-o", self.get_absolute_path(output)])
        self.exec_uv_cmd("compile", args)

    def get_absolute_path(self, path: str) -> str:
        return os.path.join(self.workspace.dir, path)