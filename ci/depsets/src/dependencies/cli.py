#!/usr/bin/env python3

import click
from pathlib import Path
from typing import Dict, List, Optional
from dependencies.depset import DepSet
import subprocess
from dependencies.config import load_config, Config

class DependencySetManager:
    def __init__(self, storage_path: Path = Path.home() / ".depsets"):
        self.storage_path = storage_path
        self.storage_path.mkdir(exist_ok=True)
        self.depsets: Dict[str, DepSet] = {}
        self._load()

    def _load(self):
        for depset_file in self.storage_path.glob("*.txt"):
            name = depset_file.stem
            self.depsets[name] = DepSet(str(depset_file))

    def list_depsets(self) -> List[str]:
        return self.depsets.keys()

    def get_depset(self, name: str) -> Optional[DepSet]:
        return self.depsets.get(name)

    def delete_depset(self, name: str):
        if name not in self.depsets:
            raise ValueError(f"Dependency set {name} does not exist")
        depset_path = self.storage_path / f"{name}.txt"
        if depset_path.exists():
            depset_path.unlink()
        del self.depsets[name]

    def add_depset(self, name: str, output: str):
        click.echo(f"adding depset: {name} from {output}")
        self.depsets[name] = DepSet(output)
        self.depsets[name].to_file(self.storage_path / f"{name}.txt")

    def exec_uv__cmd(self, cmd: str, args: List[str]) -> str:
        cmd = f"uv pip {cmd} {' '.join(args)}"
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, shell=True)
        if status.returncode != 0:
            raise Exception(f"Failed to execute command: {cmd}")
        return status.stdout

@click.group(name="depsets")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.option("--debug", is_flag=True, help="Enable debug output")
@click.pass_context
def cli(ctx, verbose, debug):
    """Manage Python dependency sets."""
    # Store flags in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["debug"] = debug


@cli.command()
@click.argument("file_path")
def load(file_path: str):
    """Load a dependency sets from a config file."""
    config = load_config(file_path)
    click.echo("Generated dependency sets from config file:")
    for _, depconfig in config.depsets.items():
        execute_config(depconfig.operation, depconfig)

def execute_config(func_name: str, config: Config):
    if func_name == "compile":
        compile(constraints=config.constraints, requirements=config.requirements, args=config.flags, name=config.name, output=config.output)
    elif func_name == "relax":
        relax(config.depset, config.degree, config.name, config.output)
    elif func_name == "subset":
        subset(config.depset, config.packages, config.name, config.output)
    elif func_name == "expand":
        expand(config.depsets, config.constraints, config.flags, config.name, config.output)

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
def show(name: str):
    """Show details of a specific dependency set."""
    manager = DependencySetManager()
    depset = manager.get_depset(name)
    if not depset:
        click.echo(f"Error: Dependency set {name} not found.", err=True)
        return
    click.echo(f"Dependency set from: {depset.requirements_fp}")
    click.echo(f"# of dependencies: {len(depset.dependencies)}")
    click.echo("Dependencies:")
    for dep in sorted(depset.dependencies, key=lambda x: x.name):
        click.echo(f"- {dep}")


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
        manager.exec_uv__cmd("compile", args)
        manager.add_depset(name, output)
        click.echo(f"DAG: {manager.depsets[name].dep_dag}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)


def subset(source_depset_name: str, packages: List[str], name: str, output: str = None):
    """Subset a dependency set."""
    try:
        manager = DependencySetManager()
        # Get the source depset
        source_depset = manager.get_depset(source_depset_name)
        if not source_depset:
            click.echo(f"Error: Source depset '{source_depset_name}' not found", err=True)
            return
        click.echo(f"length of dag: {len(source_depset.dep_dag.graph.keys())}")
        click.echo(f"source dag: {source_depset.dep_dag}")
        # Extract subgraph with all dependencies of mentioned packages
        subgraph = source_depset.dep_dag.extract_subgraph_with_dependencies(packages)
        # add left over packages to the subgraph
        for package in packages:
            subgraph.add_node(package)
        subgraph.to_requirements_txt(output)
        click.echo(f"DAG: {subgraph}")
        # Add to manager
        manager.add_depset(name, output)

        click.echo(f"Created subset '{name}' from '{source_depset_name}'")
        click.echo(f"Subgraph contains {len(subgraph.get_all_nodes())} packages total")

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
        manager.exec_uv__cmd("compile", args)
        manager.add_depset(name, output)
        click.echo(f"DAG: {manager.depsets[name].dep_dag}")
        click.echo(f"Expanded {name} from {source_depset_names}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)

def relax(source_depset_name: str, degree: int, name: str, output: str = None):
    """Relax a dependency set by selectively keeping and removing constraints"""
    try:
        manager = DependencySetManager()
        source_depset = manager.get_depset(source_depset_name)
        if not source_depset:
            click.echo(f"Error: Source depset '{source_depset_name}' not found", err=True)
            return
        n_degree_deps = source_depset.dep_dag.relax(degree)
        n_degree_deps.to_requirements_txt(output)
        manager.add_depset(name, output)
        click.echo(f"DAG: {manager.depsets[name].dep_dag}")
        click.echo(
            f"Relaxed depset: {name} to the {degree} degree. Output written to {output}"
        )
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)

if __name__ == "__main__":
    cli()
