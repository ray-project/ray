import click
from pathlib import Path
from ci.raydepsets.workspace import Workspace, Depset
from typing import List
import subprocess
import platform
import runfiles
from typing import Optional

DEFAULT_UV_FLAGS = """
    --generate-hashes
    --strip-extras
    --no-strip-markers
    --emit-index-url
    --emit-find-links
    --unsafe-package ray
    --unsafe-package grpcio-tools
    --unsafe-package setuptools
    --index-url https://pypi.org/simple
    --extra-index-url https://download.pytorch.org/whl/cpu
    --index-strategy unsafe-best-match
    --quiet
""".split()


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
        cmd = [uv_binary(), "pip", cmd, *args]
        click.echo(f"Executing command: {cmd}")
        status = subprocess.run(cmd, cwd=self.workspace.dir)
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
                name=depset.name,
                output=depset.output,
                append_flags=depset.append_flags,
                override_flags=depset.override_flags,
            )
            click.echo(f"Dependency set {depset.name} compiled successfully")

    def compile(
        self,
        constraints: List[str],
        requirements: List[str],
        name: str,
        output: str,
        append_flags: Optional[List[str]] = None,
        override_flags: Optional[List[str]] = None,
    ):
        """Compile a dependency set."""
        args = DEFAULT_UV_FLAGS.copy()
        if override_flags:
            args = _override_uv_flags(override_flags, args)
        if append_flags:
            args = _append_uv_flags(append_flags, args)
        if constraints:
            for constraint in constraints:
                args.extend(["-c", self.get_path(constraint)])
        if requirements:
            for requirement in requirements:
                args.extend([self.get_path(requirement)])
        if output:
            args.extend(["-o", self.get_path(output)])
        self.exec_uv_cmd("compile", args)

    def get_path(self, path: str) -> str:
        return (Path(self.workspace.dir) / path).as_posix()


def _override_uv_flags(flags: List[str], args: List[str]) -> List[str]:
    for flag in flags:
        for arg in args:
            if flag.startswith("--"):
                if flag == arg:
                    args.remove(arg)
            else:
                if flag[0] == arg:
                    args.remove(arg)

    return args + flags


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

    return new_args + flags


def _append_uv_flags(flags: List[str], args: List[str]) -> List[str]:
    args.extend(flags)
    return args


def uv_binary():
    r = runfiles.Create()
    system = platform.system()
    if system != "Linux" or platform.processor() != "x86_64":
        raise RuntimeError(
            f"Unsupported platform/processor: {system}/{platform.processor()}"
        )
    return r.Rlocation("uv_x86_64/uv-x86_64-unknown-linux-gnu/uv")
