import click

from ci.ray_ci.utils import logger
from ci.ray_ci.tester_container import TesterContainer, PIPELINE_POSTMERGE
from ray_release.configs.global_config import init_global_config
from ray_release.test_automation.state_machine import TestStateMachine


@click.command()
@click.option(
    "--production",
    is_flag=True,
    show_default=True,
    default=False,
    help=("Persist weekly green metric to S3"),
)
def main(production: bool) -> None:
    ray_repo = TestStateMachine.get_ray_repo()

