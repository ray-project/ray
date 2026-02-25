import click

import ray.tune.cli.commands as commands


@click.group()
def cli():
    pass


@cli.command()
@click.argument("experiment_path", required=True, type=str)
@click.option("--sort", default=None, type=str, help="Select which column to sort on.")
@click.option(
    "--output",
    "-o",
    default=None,
    type=str,
    help="Select file to output information to.",
)
@click.option(
    "--filter",
    "filter_op",
    default=None,
    type=str,
    help="Select filter in the format '<column> <operator> <value>'.",
)
@click.option(
    "--columns", default=None, type=str, help="Select columns to be displayed."
)
@click.option(
    "--limit", default=None, type=int, help="Select number of rows to display."
)
@click.option("--desc", default=False, type=bool, help="Sort ascending vs. descending.")
def list_trials(experiment_path, sort, output, filter_op, columns, limit, desc):
    """Lists trials in the directory subtree starting at the given path."""
    if sort:
        sort = sort.split(",")
    if columns:
        columns = columns.split(",")
    commands.list_trials(experiment_path, sort, output, filter_op, columns, limit, desc)


@cli.command()
@click.argument("project_path", required=True, type=str)
@click.option("--sort", default=None, type=str, help="Select which column to sort on.")
@click.option(
    "--output",
    "-o",
    default=None,
    type=str,
    help="Select file to output information to.",
)
@click.option(
    "--filter",
    "filter_op",
    default=None,
    type=str,
    help="Select filter in the format '<column> <operator> <value>'.",
)
@click.option(
    "--columns", default=None, type=str, help="Select columns to be displayed."
)
@click.option(
    "--limit", default=None, type=int, help="Select number of rows to display."
)
@click.option("--desc", default=False, type=bool, help="Sort ascending vs. descending.")
def list_experiments(project_path, sort, output, filter_op, columns, limit, desc):
    """Lists experiments in the directory subtree."""
    if sort:
        sort = sort.split(",")
    if columns:
        columns = columns.split(",")
    commands.list_experiments(
        project_path, sort, output, filter_op, columns, limit, desc
    )


@cli.command()
@click.argument("path", required=True, type=str)
@click.option(
    "--filename", default="note.txt", type=str, help="Specify filename for note."
)
def add_note(path, filename):
    """Adds user notes as a text file at the given path."""
    commands.add_note(path, filename)


cli.add_command(list_trials, name="ls")
cli.add_command(list_trials, name="list-trials")
cli.add_command(list_experiments, name="lsx")
cli.add_command(list_experiments, name="list-experiments")
cli.add_command(add_note, name="add-note")


def main():
    return cli()


if __name__ == "__main__":
    main()
