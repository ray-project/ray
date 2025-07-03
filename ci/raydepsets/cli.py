#!/usr/bin/env python3

import click
from pathlib import Path
from typing import Dict, List, Optional
from depset import DepSet
import subprocess
from config import load_config, Config


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

    def exec_uv_cmd(self, cmd: str, args: List[str]) -> str:
        cmd = f"uv pip {cmd} {' '.join(args)}"
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, shell=True)
        if status.returncode != 0:
            raise Exception(f"Failed to execute command: {cmd}")
        return status.stdout


@click.group(name="depsets")
@click.pass_context
def cli(ctx):
    """Manage Python dependency sets."""
    # Store flags in context for subcommands
    ctx.ensure_object(dict)


@cli.command()
@click.argument("config_path")
def load(config_path: str):
    """Load a dependency sets from a config file."""
    manager = DependencySetManager(config_path=config_path)
    for _, depconfig in manager.config.depsets.items():
        execute_config(depconfig.operation, depconfig)


def execute_config(func_name: str, config: Config):
    if func_name == "compile":
        compile(
            constraints=config.constraints,
            requirements=config.requirements,
            args=config.flags,
            name=config.name,
            output=config.output,
        )
    elif func_name == "subset":
        subset(config.depset, config.requirements, config.name, config.output)
    elif func_name == "expand":
        expand(
            config.depsets, config.constraints, config.flags, config.name, config.output
        )


@cli.command()
def list():
    """List all dependency sets."""
    manager = DependencySetManager()
    depsets = manager.list_depsets()
    click.echo("Available dependency sets:")
    for depset in depsets:
        click.echo(f"- {depset}")

@cli.command()
@click.argument("name")
def delete(name: str):
    """Delete a dependency set."""
    try:
        manager = DependencySetManager()
        manager.delete_depset(name)
        click.echo(f"Deleted dependency set {name}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)


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
        if constraints:
            for constraint in constraints:
                args.extend(["-c", constraint])
        if requirements:
            for requirement in requirements:
                args.append(requirement)
        args.extend(["-o", f"{output}"])
        manager.exec_uv_cmd("compile", args)
        manager.add_depset(name, output)
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)


def subset(
    source_depset_name: str, requirements: List[str], name: str, output: str = None
):
    """Subset a dependency set."""
    try:
        manager = DependencySetManager()
        source_depset = manager.get_depset(source_depset_name)
        args = []
        # Build args for uv pip compile
        if source_depset_name:
            args.extend(["-c", source_depset.requirements_fp])
        # Add each requirements file as a separate argument
        for requirement in requirements:
            args.append(requirement)
        args.extend(["-o", f"{output}"])
        manager.exec_uv_cmd("compile", args)
        manager.add_depset(name, output)
        click.echo(f"subset {name} depset from {source_depset_name}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)


def expand(
    source_depset_names: List[str],
    constraints: List[str],
    args: List[str],
    name: str,
    output: str = None,
):
    """Expand a dependency set."""
    try:
        manager = DependencySetManager()
        # Build args for uv pip compile
        if constraints:
            for constraint in constraints:
                args.extend(["-c", constraint])
        # Add each requirements file as a separate argument
        for source in source_depset_names:
            args.append(manager.depsets[source].requirements_fp)
        args.extend(["-o", f"{output}"])
        manager.exec_uv_cmd("compile", args)
        manager.add_depset(name, output)
        click.echo(f"Expanded {name} from {source_depset_names}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)


if __name__ == "__main__":
    cli()
