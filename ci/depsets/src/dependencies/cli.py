#!/usr/bin/env python3

import click
from pathlib import Path
from typing import Dict, List, Optional
from dependencies.depset import DepSet, Dep, parse_compiled_requirements
import subprocess
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_print(ctx, message: str):
    """Print debug message if debug flag is set"""
    if ctx and ctx.obj and ctx.obj.get("debug"):
        click.secho(f"DEBUG: {message}", fg='cyan', err=True)

def verbose_print(ctx, message: str):
    """Print verbose message if verbose flag is set"""
    if ctx and ctx.obj and ctx.obj.get("verbose"):
        click.secho(f"VERBOSE: {message}", fg='blue', err=True)


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

    def compile_depset(self, constraints: List[str], requirements: List[str], name: str, generate_hashes: bool, header: bool, no_cache: bool):
        args = []
        if generate_hashes:
            args.append("--generate-hashes")
        if not header:
            args.append("--no-header")
        if no_cache:
            args.append("--no-cache")
        if constraints:
            for constraint in constraints:
                args.extend(["-c", constraint])
        if requirements:
            args.extend(requirements)
        args.extend(["-o", f"{self.storage_path}/{name}.txt"])
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

    def build_dag(self, source: str):
        source_depset = self.depsets[source]
        print(f"# of deps for depset {source}: {len(source_depset.dependencies)}")
        with open(f"{source}_deps.txt", "w") as f:
            for dep in source_depset.dependencies:
                f.write(f"{dep}\n")

        source_depset.dep_dag = parse_compiled_requirements(source_depset.requirements_fp)
        with open(f"{source}_dag.txt", "w") as f:
            f.write(str(source_depset.dep_dag))

    def relax_depset(self, source: str, degree: int, name: str):
        source_depset = self.depsets[source]
        n_degree_deps = source_depset.dep_dag.relax(degree)
        with open(f"{name}_relaxed.txt", "w") as f:
            for item in sorted(n_degree_deps):
                f.write(f"{item}\n")
        # Copy the requirements file to our storage location
        output_path = self.storage_path / f"{name}.txt"
        with open(output_path, "w") as req_file:
            for dep in n_degree_deps:
                dep_obj = next((d for d in source_depset.dependencies if dep in d.name), None)
                if dep_obj is None:
                    continue
                req_file.write(f"{dep_obj}\n")
        return n_degree_deps

    def py_version(self, source: str, version: str, name: str, flags: str = ""):
        source_depset = self.depsets[source]
        depset_path = self.storage_path / f"{name}.txt"
        click.echo(f"Compiling {source_depset.requirements_fp} with python version {version} to {depset_path}")
        self.exec_uv__cmd("compile", [source_depset.requirements_fp, "-o", f"\"{depset_path}\"", "--python-version", version, flags if flags else ""])

    def exec_uv__cmd(self, cmd: str, args: List[str]) -> str:
        cmd = f"uv pip {cmd} {" ".join(args)}"
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
@click.option("--output", type=str, required=True, help="filename for output file")
@click.option("--generate-hashes", type=bool, default=True, help="generate hashes")
@click.option("--header", type=bool, default=False, help="no header")
@click.option("--no-cache", type=bool, default=False, help="no header")
def compile(constraints: str, requirements: str, output: str, generate_hashes: bool, header: bool, no_cache: bool):
    """Compile a dependency set."""
    try:
        manager = DependencySetManager()
        manager.compile_depset(constraints.split(",") if constraints else [], requirements.split(",") if requirements else [], output, generate_hashes, header, no_cache)
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

@cli.command()
@click.option("--source", type=str, help="name of source depset")
@click.option("--degree", type=int, help="degree of relaxation")
@click.argument("name")
def relax(source: str, degree: int, name: str):
    """Relax a dependency set by selectively keeping and removing constraints"""
    try:
        manager = DependencySetManager()
        manager.build_dag(source)
        click.echo(f"Built dag for {source}")
        manager.relax_depset(source, degree, name)
        click.echo(f"Relaxed depset: {name} to the {degree} degree. Output written to {name}.txt")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)

@cli.command()
@click.option("--source", type=str, help="name of source depset")
@click.option("--version", type=str, help="python version")
@click.option("--flags", type=str, help="flags to pass to uv pip compile")
@click.argument("name")
def py_version(source: str, version: str, name: str, flags: str = ""):
    """Set the python version for a dependency set."""
    try:
        manager = DependencySetManager()
        manager.py_version(source, version, name, flags)
        click.echo(f"Set python version for {name} to {version}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)

if __name__ == "__main__":
    cli()
