import click
from pathlib import Path
from ci.raydepsets.config import load_config, Depset


@click.group(name="raydepsets")
def cli():
    """Manage Python dependency sets."""


@cli.command()
@click.argument("config_path", default="ci/raydepsets/depset.config.yaml")
@click.option("--name", type=str, default="")
def load(config_path: str, name: str = ""):
    """Load a dependency sets from a config file."""
    manager = DependencySetManager(config_path=config_path)


class DependencySetManager:
    def __init__(
        self, config_path: Path = Path(__file__).parent / "depset.config.yaml"
    ):
        self.config = load_config(config_path)

    def get_depset(self, name: str) -> Depset:
        for depset in self.config.depsets:
            if depset.name == name:
                return depset
        raise Exception(f"Dependency set {name} not found")
