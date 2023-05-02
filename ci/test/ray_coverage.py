import click

@click.option(
    "--upload",
    default=3,
    type=int,
    help=(
        "Maximum number of concurrent test jobs to run. Higher number uses more "
        "capacity, but reduce the bisect duration"
    ),
)
def main() -> None: