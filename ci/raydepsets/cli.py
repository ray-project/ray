<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
#!/usr/bin/env python3

=======
>>>>>>> cleanup tests
import click
from pathlib import Path
from typing import Dict, List, Optional
<<<<<<< HEAD
<<<<<<< HEAD
from depset import DepSet
import subprocess
from config import load_config, Config
=======
from dependencies.depset import DepSet
import subprocess
from dependencies.config import load_config, Config
>>>>>>> refactoring
=======
from depset import DepSet
import subprocess
from config import load_config, Config
>>>>>>> updating bazel file


class DependencySetManager:
    def __init__(self, storage_path: Path = Path.home() / ".depsets", config_path: Path = Path(__file__).parent / "depset.config.yaml"):
        self.storage_path = storage_path
        self.storage_path.mkdir(exist_ok=True)
        self.depsets: Dict[str, DepSet] = {}
        self.config = load_config(config_path)
        self._load()

    def _load(self):
        for depset_file in self.storage_path.glob("*.txt"):
            name = depset_file.stem
            self.depsets[name] = DepSet(str(depset_file))

    def list_depsets(self) -> List[str]:
        return self.depsets.keys()

    def get_depset(self, name: str) -> Optional[DepSet]:
        if name not in self.depsets:
            raise ValueError(f"Dependency set {name} does not exist")
        return self.depsets.get(name)

    def delete_depset(self, name: str):
        if name not in self.depsets:
            raise ValueError(f"Dependency set {name} does not exist")
        depset_path = self.storage_path / f"{name}.txt"
        if depset_path.exists():
            depset_path.unlink()
        del self.depsets[name]

    def add_depset(self, name: str, output: str):
        self.depsets[name] = DepSet(output)
        self.depsets[name].to_file(self.storage_path / f"{name}.txt")
=======
import click
from pathlib import Path
from typing import List
import subprocess
from ci.raydepsets.config import load_config, Depset

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
        self, config_path: Path = Path(__file__).parent / "depset.config.yaml"
    ):
        self.config = load_config(config_path)
>>>>>>> flattening raydepsets file structure

    def exec_uv_cmd(self, cmd: str, args: List[str]) -> str:
        cmd = f"uv pip {cmd} {' '.join(args)}"
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, shell=True)
        if status.returncode != 0:
            raise Exception(f"Failed to execute command: {cmd}")
        return status.stdout
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> scafolding for raydepsets
=======
>>>>>>> reducing raydepsets to compile operations

    def execute_all(configs: List[Config]):
        for config in configs:
            if config.operation == "compile":
                compile(
                    constraints=config.constraints,
                    requirements=config.requirements,
                    args=config.flags,
                    name=config.name,
                    output=config.output,
                )
            elif config.operation == "subset":
                subset(config.depset, config.requirements, config.name, config.output)
            elif config.operation == "expand":
                expand(
                    config.depsets, config.constraints, config.flags, config.name, config.output
                )

    def execute_single(config: Config):
        if config.operation == "compile":
            compile(
                constraints=config.constraints,
                requirements=config.requirements,
                args=config.flags,
                name=config.name,
                output=config.output,
            )

@click.group(name="depsets")
def cli():
    """Manage Python dependency sets."""
<<<<<<< HEAD
    pass
=======


@cli.command()
@click.argument("config_path")
@click.option("--mode", type=click.Choice(["single-rule", "multi-rule"]), default="multi-rule")
def load(config_path: str, mode: str = ""):
    """Load a dependency sets from a config file."""
    manager = DependencySetManager(config_path=config_path)
    if mode == "single-rule":
        execute_single(manager.config.depsets[0])
    else:
        execute_all(manager.config.depsets)

def compile(
    constraints: List[str],
    requirements: List[str],
    args: List[str],
    name: str,
    output: str = None,
):
    """Compile a dependency set."""
    try:
        manager = DependencySetManager()
        # Build args for uv pip compile
=======

    def get_depset(self, name: str) -> Depset:
        for depset in self.config.depsets:
            if depset.name == name:
                return depset
        raise Exception(f"Dependency set {name} not found")

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
>>>>>>> flattening raydepsets file structure
        if constraints:
            for constraint in constraints:
                args.extend(["-c", constraint])
        if requirements:
            for requirement in requirements:
<<<<<<< HEAD
                args.append(requirement)
        args.extend(["-o", f"{output}"])
        manager.exec_uv_cmd("compile", args)
        manager.add_depset(name, output)
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)
>>>>>>> reducing raydepsets to compile operations

<<<<<<< HEAD
if __name__ == "__main__":
    cli()
=======
import click


@click.group(name="depsets")
=======
@click.group(name="raydepsets")
>>>>>>> code cleanup
@click.pass_context
def cli(ctx):
    """Manage Python dependency sets."""
    pass
>>>>>>> raydepsets scaffolding (package management tool)  (#54265)
=======
                args.extend([requirement])
        args.extend(["-o", f"{output}"])
        try:
            self.exec_uv_cmd("compile", args)
        except Exception as e:
            raise Exception(f"Error: {str(e)}")
>>>>>>> flattening raydepsets file structure
