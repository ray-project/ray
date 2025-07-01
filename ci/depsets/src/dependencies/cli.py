#!/usr/bin/env python3

import click
from pathlib import Path
from typing import Dict, List, Optional
from dependencies.depset import DepSet, parse_compiled_requirements
import subprocess
import logging
import os
import yaml
from dependencies.config import load_config, Config

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def debug_print(ctx, message: str):
    """Print debug message if debug flag is set"""
    if ctx and ctx.obj and ctx.obj.get("debug"):
        click.secho(f"DEBUG: {message}", fg="cyan", err=True)


def verbose_print(ctx, message: str):
    """Print verbose message if verbose flag is set"""
    if ctx and ctx.obj and ctx.obj.get("verbose"):
        click.secho(f"VERBOSE: {message}", fg="blue", err=True)


def resolve_path_for_bazel(path_str: str) -> str:
    """Resolve paths relative to where bazel run was invoked."""
    if not path_str or os.path.isabs(path_str):
        return path_str

    # Use BUILD_WORKSPACE_DIRECTORY if available (set by bazel run)
    workspace_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if workspace_dir:
        return os.path.join(workspace_dir, path_str)

    # Fallback to current directory
    return os.path.abspath(path_str)

# def resolve_paths(paths: str) -> List[str]:
#     """Resolve paths relative to where bazel run was invoked."""
#     resolved_paths = []
#     if paths:
#         for c in paths.split(","):
#             resolved_path = resolve_path_for_bazel(c.strip())
#             resolved_paths.append(resolved_path)
#             click.echo(f"path resolved: {c.strip()} -> {resolved_path}")
#     return resolved_paths

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
    def load_config(self, file_path: str):
        with open(file_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    def add_depset(self, name: str, output: str):
        self.depsets[name] = DepSet(output)
        self.depsets[name].to_file(self.storage_path / f"{name}.txt")
    # def process_flags(
    #     self,
    #     unsafe_packages: tuple,
    #     index_url: str,
    #     extra_index_url: str,
    #     find_links: str,
    #     generate_hashes: bool,
    #     no_header: bool,
    #     no_cache: bool,
    #     index_strategy: str,
    #     strip_extras: bool,
    #     no_strip_markers: bool,
    #     emit_index_url: bool,
    #     emit_find_links: bool,
    #     python_version: str,
    #     prerelease: str,
    # ):
    #     args = []
    #     if unsafe_packages:
    #         for package in unsafe_packages:
    #             args.extend(["--unsafe-package", package])
    #     if index_url:
    #         args.extend(["--index-url", index_url])
    #     if extra_index_url:
    #         args.extend(["--extra-index-url", extra_index_url])
    #     if find_links:
    #         args.extend(["--find-links", find_links])
    #     if generate_hashes:
    #         args.append("--generate-hashes")
    #     if no_header:
    #         args.append("--no-header")
    #     if no_cache:
    #         args.append("--no-cache")
    #     if index_strategy:
    #         args.extend(["--index-strategy", index_strategy])
    #     if strip_extras:
    #         args.append("--strip-extras")
    #     if no_strip_markers:
    #         args.append("--no-strip-markers")
    #     if emit_index_url:
    #         args.append("--emit-index-url")
    #     if emit_find_links:
    #         args.append("--emit-find-links")
    #     if python_version:
    #         args.extend(["--python-version", python_version])
    #     if prerelease:
    #         args.extend(["--prerelease", prerelease])
    #     return args

    def compile_depset(
        self,
        constraints: List[str],
        requirements: List[str],
        args: List[str],
        name: str,
        output: str = None,
    ):
        if constraints:
            for constraint in constraints:
                args.extend(["-c", constraint])
        if requirements:
            for requirement in requirements:
                args.append(requirement)
        args.extend(["-o", f"{output}"])
        self.exec_uv__cmd("compile", args)
        self.add_depset(name, output)
        click.echo(f"length of dag: {len(self.depsets[name].dep_dag.graph.keys())}")

    def write_depset(self, depset: DepSet, output: str):
        with open(output, "w") as f:
            for dep in depset.dependencies:
                f.write(f"{dep}\n")

    def expand_depset(
        self,
        source_depsets: List[str],
        constraints: List[str],
        args: List[str],
        name: str,
        output: str = None,
    ):
        #source_depsets_objs = [self.depsets[source] for source in source_depsets]
        if constraints:
            for constraint in constraints:
                args.extend(["-c", constraint])
        # Add each requirements file as a separate argument
        for source in source_depsets:
            args.append(self.depsets[source].requirements_fp)
        args.extend(["-o", f"{output}"])
        self.exec_uv__cmd("compile", args)
        click.echo(f"keys: {self.depsets.keys()}")
        #click.echo(f"length of dag: {len(self.depsets[name].dep_dag.graph.keys())}")
        click.echo(f"name: {name}")
        #self.add_depset(name, output)

    def relax_depset(self, source: str, degree: int, name: str):
        source_depset = self.depsets[source]
        n_degree_deps = source_depset.dep_dag.relax(degree)

        # output_path = self.storage_path / f"{name}_relaxed.txt"
        # with open(output_path, "w") as f:
        #     for item in sorted(n_degree_deps):
        #         f.write(f"{item}\n")

        # Copy the requirements file to our storage location
        output_path = self.storage_path / f"{name}.txt"
        with open(output_path, "w") as req_file:
            for dep in n_degree_deps:
                dep_obj = next(
                    (
                        d
                        for d in source_depset.dependencies
                        if dep == d.name.split("==")[0].split(" ")[0]
                    ),
                    None,
                )
                if dep_obj is None:
                    continue
                req_file.write(f"{dep_obj}\n")
        return n_degree_deps

    def py_version(self, source: str, version: str, name: str, flags: str = ""):
        source_depset = self.depsets[source]
        depset_path = self.storage_path / f"{name}.txt"
        click.echo(
            f"Compiling {source_depset.requirements_fp} with python version {version} to {depset_path}"
        )
        self.exec_uv__cmd(
            "compile",
            [
                source_depset.requirements_fp,
                "-o",
                f'"{depset_path}"',
                "--python-version",
                version,
                flags if flags else "",
            ],
        )

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
        relax(config.name, config.degree)
    elif func_name == "subset":
        subset(config.depset, config.packages, config.name, config.output)
    elif func_name == "expand":
        expand(config.depsets, config.constraints, config.flags, config.name, config.output)

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
        manager.compile_depset(
            constraints=constraints,
            requirements=requirements,
            args=args,
            name=name,
            output=output,
        )
        click.echo(f"Compiled dependency set {name}")
        click.echo(f"Built dag for {name}")
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
        # Extract subgraph with all dependencies of mentioned packages
        subgraph = source_depset.dep_dag.extract_subgraph_with_dependencies(packages)

        subgraph.to_requirements_txt(output)

        # Add to manager
        manager.add_depset(name, output)

        click.echo(f"Created subset '{name}' from '{source_depset_name}'")
        click.echo(f"Subgraph contains {len(subgraph.get_all_nodes())} packages total")

    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)


def expand(
    source_depsets: List[str],
    constraints: List[str],
    args: List[str],
    name: str,
    output: str = None,
):
    """Expand a dependency set."""
    try:
        manager = DependencySetManager()
        manager.expand_depset(
            source_depsets,
            constraints,
            args,
            name,
            output,
        )
        click.echo(f"Expanded {name} from {source_depsets}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)


@cli.command()
@click.option("--source", "-s", type=str, help="name of source depset")
@click.option("--degree", "-d", type=int, help="degree of relaxation")
@click.argument("name")
def relax(source: str, degree: int, name: str):
    """Relax a dependency set by selectively keeping and removing constraints"""
    try:
        manager = DependencySetManager()
        manager.relax_depset(source, degree, name)
        click.echo(
            f"Relaxed depset: {name} to the {degree} degree. Output written to {name}.txt"
        )
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)


@cli.command()
@click.option("--source", "-s", type=str, help="name of source depset")
@click.option("--version", "-v", type=str, help="python version")
@click.option("--flags", "-f", type=str, help="flags to pass to uv pip compile")
@click.argument("name")
def py_version(source: str, version: str, name: str, flags: str = ""):
    """Set the python version for a dependency set."""
    try:
        manager = DependencySetManager()
        manager.py_version(source, version, name, flags)
        click.echo(f"Set python version for {name} to {version}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)

# def execute_config(config: dict):
#     visited = set()

#     def visit(name: str):
#         if name in visited:
#             return
#         depset = config.depsets[name]
#         for dep_name in depset.deps:
#             visit(dep_name)
#         handler = operation_map.get(depset.operation)
#         if not handler:
#             raise ValueError(f"Unknown operation: {depset.operation}")
#         handler(depset)
#         visited.add(name)

#     for name in config.depsets:
#         visit(name)

# operation_map = {
#     "compile": compile,
#     "relax": relax,
#     "py_version": py_version,
#     "subset": subset,
#     "expand": expand,
#     "delete": delete,
#     "show": show,
#     "list": list,
# }

@cli.command()
@click.option("--depset", "-d", type=str, help="name of source depset")
@click.option("--packages", "-p", type=str, help="comma-separated list of packages to extract")
@click.option("--output", "-o", type=str, help="output file path")
@click.argument("name")
def extract_subgraph(depset: str, packages: str, output: str, name: str):
    """Extract a subgraph containing specified packages and their dependencies."""
    try:
        manager = DependencySetManager()
        
        # Get the source depset
        source_depset = manager.get_depset(depset)
        if not source_depset:
            click.echo(f"Error: Source depset '{depset}' not found", err=True)
            return
        
        # Parse packages list
        package_list = [pkg.strip() for pkg in packages.split(",") if pkg.strip()]
        
        # Extract subgraph with all dependencies
        subgraph_with_deps = source_depset.dep_dag.extract_subgraph_with_dependencies(package_list)
        
        # Extract subgraph with only mentioned packages
        subgraph_only_mentioned = source_depset.dep_dag.extract_subgraph_only_mentioned(package_list)
        
        click.echo(f"Subgraph with all dependencies:")
        click.echo(f"  Nodes: {len(subgraph_with_deps.get_all_nodes())}")
        click.echo(f"  Graph: {subgraph_with_deps}")
        
        click.echo(f"\nSubgraph with only mentioned packages:")
        click.echo(f"  Nodes: {len(subgraph_only_mentioned.get_all_nodes())}")  
        click.echo(f"  Graph: {subgraph_only_mentioned}")
        
        # If output specified, write the full dependency subgraph
        if output:
            subgraph_packages = set(subgraph_with_deps.get_all_nodes())
            filtered_deps = [
                dep for dep in source_depset.dependencies 
                if dep.name in subgraph_packages
            ]
            
            with open(output, "w") as f:
                for dep in filtered_deps:
                    f.write(f"{dep}\n")
                    
            click.echo(f"\nWrote {len(filtered_deps)} dependencies to {output}")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)


@cli.command()
@click.option("--depset", "-d", type=str, required=True, help="name of source depset")
@click.option("--output", "-o", type=str, help="output requirements.txt file")
@click.option("--packages", "-p", type=str, help="comma-separated list of packages to include (optional)")
@click.option("--with-versions", is_flag=True, help="include version constraints")
def flatten(depset: str, output: str, packages: str, with_versions: bool):
    """Flatten a dependency graph into a requirements list."""
    try:
        manager = DependencySetManager()
        
        # Get the source depset
        source_depset = manager.get_depset(depset)
        if not source_depset:
            click.echo(f"Error: Source depset '{depset}' not found", err=True)
            return
        
        # If specific packages are mentioned, extract subgraph first
        if packages:
            package_list = [pkg.strip() for pkg in packages.split(",") if pkg.strip()]
            subgraph = source_depset.dep_dag.extract_subgraph_with_dependencies(package_list)
            click.echo(f"Extracted subgraph for packages: {', '.join(package_list)}")
        else:
            subgraph = source_depset.dep_dag
            click.echo("Using full dependency graph")
        
        # Flatten to requirements
        if with_versions:
            requirements = subgraph.flatten_to_requirements_with_versions(source_depset)
        else:
            requirements = subgraph.flatten_to_requirements()
        
        # Output results
        click.echo(f"\nFlattened requirements ({len(requirements)} packages):")
        for req in requirements:
            click.echo(f"  {req}")
        
        # Write to file if specified
        if output:
            if with_versions:
                count = subgraph.to_requirements_txt(output, source_depset)
            else:
                count = subgraph.to_requirements_txt(output)
            click.echo(f"\nWrote {count} requirements to {output}")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

if __name__ == "__main__":
    cli()
