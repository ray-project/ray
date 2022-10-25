#!/usr/bin/env python

import collections
from rich.console import Console
from rich.table import Table
import typer

from ray.rllib import train as train_module
from ray.rllib.common import CLIArguments as cli
from ray.rllib.common import (
    EXAMPLES,
    FrameworkEnum,
    SupportedFileType,
    example_help,
    download_example_file,
)

# Main Typer CLI app
app = typer.Typer()
example_app = typer.Typer()


def example_error(example_id: str):
    return ValueError(
        f"Example {example_id} not found. Use `rllib example list` "
        f"to see available examples."
    )


@example_app.callback()
def example_callback():
    """RLlib command-line interface to run built-in examples. You can choose to list
    all available examples, get more information on an example or run a specific
    example.
    """
    pass


@example_app.command()
def list(
    filter: str = typer.Option(None, "--filter", "-f", help=example_help.get("filter"))
):
    """List all available RLlib examples that can be run from the command line.
    Note that many of these examples require specific hardware (e.g. a certain number
    of GPUs) to work.\n\n

    Example usage: `rllib example list --filter=cartpole`
    """

    table = Table(title="RLlib Examples")
    table.add_column("Example ID", justify="left", style="cyan", no_wrap=True)
    table.add_column("Description", justify="left", style="magenta")

    sorted_examples = collections.OrderedDict(sorted(EXAMPLES.items()))

    for name, value in sorted_examples.items():
        if filter:
            if filter.lower() in name:
                table.add_row(name, value["description"])
        else:
            table.add_row(name, value["description"])

    console = Console()
    console.print(table)
    console.print(
        "Run any RLlib example as using 'rllib example run <Example ID>'."
        "See 'rllib example run --help' for more information."
    )


def get_example_file(example_id):
    """Simple helper function to get the example file for a given example ID."""
    if example_id not in EXAMPLES:
        raise example_error(example_id)

    example = EXAMPLES[example_id]
    assert hasattr(
        example, "file"
    ), f"Example {example_id} does not have a 'file' attribute."
    return example.get("file")


@example_app.command()
def get(example_id: str = typer.Argument(..., help="The example ID of the example.")):
    """Print the configuration of an example.\n\n
    Example usage: `rllib example get atari-a2c`
    """
    example_file = get_example_file(example_id)
    example_file, temp_file = download_example_file(example_file)
    with open(example_file) as f:
        console = Console()
        console.print(f.read())


@example_app.command()
def run(example_id: str = typer.Argument(..., help="Example ID to run.")):
    """Run an RLlib example from the command line by simply providing its ID.\n\n

    Example usage: `rllib example run pong-impala`
    """
    example = EXAMPLES[example_id]
    example_file = get_example_file(example_id)
    example_file, temp_file = download_example_file(example_file)
    file_type = example.get("file_type", SupportedFileType.yaml)

    train_module.file(
        config_file=example_file,
        file_type=file_type,
        framework=FrameworkEnum.tf2,
        v=True,
        vv=False,
        trace=False,
        local_mode=False,
        ray_address=None,
        ray_ui=False,
        ray_num_cpus=None,
        ray_num_gpus=None,
        ray_num_nodes=None,
        ray_object_store_memory=None,
        resume=False,
        scheduler="FIFO",
        scheduler_config="{}",
    )

    if temp_file:
        temp_file.close()


# Register all subcommands
app.add_typer(example_app, name="example")
app.add_typer(train_module.train_app, name="train")


@app.command()
def evaluate(
    checkpoint: str = cli.Checkpoint,
    algo: str = cli.Algo,
    env: str = cli.Env,
    local_mode: bool = cli.LocalMode,
    render: bool = cli.Render,
    steps: int = cli.Steps,
    episodes: int = cli.Episodes,
    out: str = cli.Out,
    config: str = cli.Config,
    save_info: bool = cli.SaveInfo,
    use_shelve: bool = cli.UseShelve,
    track_progress: bool = cli.TrackProgress,
):
    """Roll out a reinforcement learning agent given a checkpoint argument.
    You have to provide an environment ("--env") an an RLlib algorithm ("--algo") to
    evaluate your checkpoint.

    Example usage:\n\n

        rllib evaluate /tmp/ray/checkpoint_dir/checkpoint-0 --algo DQN --env CartPole-v1
        --steps 1000000 --out rollouts.pkl
    """
    from ray.rllib import evaluate as evaluate_module

    evaluate_module.run(
        checkpoint=checkpoint,
        algo=algo,
        env=env,
        local_mode=local_mode,
        render=render,
        steps=steps,
        episodes=episodes,
        out=out,
        config=config,
        save_info=save_info,
        use_shelve=use_shelve,
        track_progress=track_progress,
    )


@app.command()
def rollout(
    checkpoint: str = cli.Checkpoint,
    algo: str = cli.Algo,
    env: str = cli.Env,
    local_mode: bool = cli.LocalMode,
    render: bool = cli.Render,
    steps: int = cli.Steps,
    episodes: int = cli.Episodes,
    out: str = cli.Out,
    config: str = cli.Config,
    save_info: bool = cli.SaveInfo,
    use_shelve: bool = cli.UseShelve,
    track_progress: bool = cli.TrackProgress,
):
    """Old rollout script. Please use `rllib evaluate` instead."""
    from ray.rllib.utils.deprecation import deprecation_warning

    deprecation_warning(old="rllib rollout", new="rllib evaluate", error=False)

    return evaluate(
        checkpoint=checkpoint,
        algo=algo,
        env=env,
        local_mode=local_mode,
        render=render,
        steps=steps,
        episodes=episodes,
        out=out,
        config=config,
        save_info=save_info,
        use_shelve=use_shelve,
        track_progress=track_progress,
    )


@app.callback()
def main_helper():
    """Welcome to the\n
    .                                                  ╔▄▓▓▓▓▄\n
    .                                                ╔██▀╙╙╙▀██▄\n
    . ╫█████████████▓   ╫████▓             ╫████▓    ██▌     ▐██   ╫████▒\n
    . ╫███████████████▓ ╫█████▓            ╫█████▓   ╫██     ╫██   ╫██████▒\n
    . ╫█████▓     ████▓ ╫█████▓            ╫█████▓    ╙▓██████▀    ╫██████████████▒\n
    . ╫███████████████▓ ╫█████▓            ╫█████▓       ╫█▒       ╫████████████████▒\n
    . ╫█████████████▓   ╫█████▓            ╫█████▓       ╫█▒       ╫██████▒    ╫█████▒\n
    . ╫█████▓███████▓   ╫█████▓            ╫█████▓       ╫█▒       ╫██████▒    ╫█████▒\n
    . ╫█████▓   ██████▓ ╫████████████████▄ ╫█████▓       ╫█▒       ╫████████████████▒\n
    . ╫█████▓     ████▓ ╫█████████████████ ╫█████▓       ╫█▒       ╫██████████████▒\n
    .                                        ╣▓▓▓▓▓▓▓▓▓▓▓▓██▓▓▓▓▓▓▓▓▓▓▓▓▄\n
    .                                        ╫██╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╙╫█▒\n
    .                                        ╫█  Command Line Interface █▒\n
    .                                        ╫██▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄╣█▒\n
    .                                         ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀\n
    .\n
        Example usage for training:\n
            rllib train --algo DQN --env CartPole-v1\n
            rllib train file tuned_examples/ppo/pendulum-ppo.yaml\n\n

        Example usage for evaluation:\n
            rllib evaluate /trial_dir/checkpoint_000001/checkpoint-1 --algo DQN\n\n

        Example usage for built-in examples:\n
            rllib example list\n
            rllib example get atari-ppo\n
            rllib example run atari-ppo\n
    """


def cli():
    # Keep this function here, it's referenced in the setup.py file, and exposes
    # the CLI as entry point ("rllib" command).
    app()


if __name__ == "__main__":
    cli()
