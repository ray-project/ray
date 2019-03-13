from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
import ray.tune.commands as commands


@click.group()
def cli():
    pass


@cli.command()
@click.argument("experiment_path", required=True, type=str)
@click.option(
    "--sort", default=None, type=str, help="Select which column to sort on.")
@click.option(
    "--output",
    "-o",
    default=None,
    type=str,
    help="Select file to output information to.")
def list_trials(experiment_path, sort, output):
    """Lists trials in the directory subtree starting at the given path."""
    commands.list_trials(experiment_path, sort, output)


@cli.command()
@click.argument("project_path", required=True, type=str)
@click.option(
    "--sort", default=None, type=str, help="Select which column to sort on.")
@click.option(
    "--output",
    "-o",
    default=None,
    type=str,
    help="Select file to output information to.")
def list_experiments(project_path, sort, output):
    """Lists experiments in the directory subtree."""
    commands.list_experiments(project_path, sort, output)


@cli.command()
@click.argument("path", required=True, type=str)
@click.option(
    "--filename",
    default="note.txt",
    type=str,
    help="Specify filename for note.")
def add_note(path, filename):
    """Adds user notes as a text file at the given path."""
    commands.add_note(path, filename)


@cli.command()
@click.argument("tune_address", required=True, type=str)
@click.argument("port_forward", required=True, type=int)
@click.argument("trial_id", required=True, type=str)
def kill(tune_address, port_forward, trial_id):
    """Terminates trial within running TuneServer with specific trial_id."""
    commands.kill(tune_address, port_forward, trial_id)


cli.add_command(list_trials, name="ls")
cli.add_command(list_trials, name="list-trials")
cli.add_command(list_experiments, name="lsx")
cli.add_command(list_experiments, name="list-experiments")
cli.add_command(add_note, name="add-note")
cli.add_command(kill, name="kill")


def main():
    return cli()


if __name__ == "__main__":
    main()
