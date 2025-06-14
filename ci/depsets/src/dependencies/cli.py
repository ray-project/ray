#!/usr/bin/env python3

import click
import json
from pathlib import Path
from typing import Dict, List, Optional
from dependencies.depset import DepSet, Dep
import subprocess

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

    def _save(self):
        for name, depset in self.depsets.items():
            output_path = self.storage_path / f"{name}.txt"
            with open(output_path, "w") as f:
                for dep in depset.dependencies:
                    f.write(f"{str(dep)}\n")

    def create_depset(self, name: str, requirements_path: Path) -> DepSet:
        if name in self.depsets:
            raise ValueError(f"Dependency set {name} already exists")

        # Copy the requirements file to our storage location
        output_path = self.storage_path / f"{name}.txt"
        with open(requirements_path) as src, open(output_path, "w") as dst:
            dst.write(src.read())

        depset = DepSet(str(output_path))
        self.depsets[name] = depset
        return depset

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

    def compile_depset(self, constraints: List[str], requirements: List[str], name: str):
        args = []
        for constraint in constraints:
            args.extend(["-c", constraint])
        args.extend(requirements)
        args.extend(["-o", name])
        return self.exec_uv__cmd("compile", args)

    def subset_depset(self, source: str, packages: List[str], name: str):
        source_depset = self.depsets[source]
        req_name = f"{name}.txt"
        new_depset = DepSet(req_name)
        for dep in source_depset.dependencies:
            if dep.name in packages:
                new_depset.dependencies.append(dep)

        with open(req_name, "w") as f:
            f.write(new_depset.to_txt())
        self.create_depset(name, req_name)

    def expand_depset(self, source: str, constraints: List[str], name: str):
        source_depset = self.depsets[source]
        req_name = f"{name}.txt"
        new_depset = DepSet(req_name)

        for dep in source_depset.dependencies:
            found_constraint = False
            for constraint in constraints:
                if dep.name in constraint:
                    new_depset.dependencies.append(Dep.from_requirement(constraint))
                    found_constraint = True
                    break
            if not found_constraint:
                new_depset.dependencies.append(dep)

        with open(req_name, "w") as f:
            f.write(new_depset.to_txt())
        self.create_depset(name, req_name)

    def exec_uv__cmd(self, cmd: str, args: List[str]) -> str:
        cmd = f"uv pip {cmd} {' '.join(args)}"
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, shell=True)
        if status.returncode != 0:
            raise Exception(f"Failed to execute command: {cmd}")
        return status.stdout

@click.group(name="depsets")
def cli():
    """Manage Python dependency sets."""
    pass

@cli.command()
@click.argument("name")
@click.argument("requirements_file", type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
def init(name: str, requirements_file: Path):
    """Initialize a new dependency set from a requirements file."""
    try:
        manager = DependencySetManager()
        depset = manager.create_depset(name, requirements_file)
        click.echo(f"Created dependency set {name} with {len(depset.dependencies)} dependencies")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
def list():
    """List all dependency sets."""
    try:
        manager = DependencySetManager()
        depsets = manager.list_depsets()
        if not depsets:
            click.echo("No dependency sets found.")
            return
        click.echo("Available dependency sets:")
        for depset in depsets:
            click.echo(f"- {depset}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)


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
    click.echo("Dependencies:")
    for dep in depset.dependencies:
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

@cli.command()
@click.option("--constraints", type=str, help="comma separated list of absolute filepaths for constraint files")
@click.option("--requirements", type=str, help="filename for requirements file")
@click.option("--output", type=str, help="filename for output file")
def compile(constraints: List[str], requirements: str, output: str):
    """Compile a dependency set."""
    try:
        manager = DependencySetManager()
        manager.compile_depset(constraints.split(","), requirements.split(","), output)
        click.echo(f"Compiled dependency set {output}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
@click.option("--source", type=str, help="name of source depset")
@click.option("--packages", type=str, help="filename for min package deps file")
@click.argument("name")
def subset(source: str, packages: str, name: str):
    """Subset a dependency set."""
    try:
        manager = DependencySetManager()
        with open(packages, "r") as f:
            packages = f.read().splitlines()
        manager.subset_depset(source, packages, name)
        click.echo(f"Created subset {name} from {source} with {len(packages)} dependencies")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
@click.option("--source", type=str, help="name of source depset")
@click.option("--constraints", type=str, help="filename for constraints file")
@click.argument("name")
def expand(source: str, constraints: str, name: str):
    """Subset a dependency set."""
    try:
        manager = DependencySetManager()
        with open(constraints, "r") as f:
            constraints = f.read().splitlines()
        manager.expand_depset(source, constraints, name)
        click.echo(f"Created subset {name} from {source} with {len(constraints)} constraints")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)

if __name__ == "__main__":
    cli()
