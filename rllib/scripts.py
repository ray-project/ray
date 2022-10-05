#!/usr/bin/env python

import collections
import typer

from ray.rllib import train as train_module
from ray.rllib.common import CLIArguments as cli
from ray.rllib.common import EXAMPLES, FrameworkEnum

# Main Typer CLI app
app = typer.Typer()
examples_app = typer.Typer()


@examples_app.command()
def list():
    from rich.console import Console
    from rich.table import Table

    table = Table(title="RLlib Examples")
    table.add_column("Example ID", justify="left", style="cyan", no_wrap=True)
    table.add_column("Description", justify="left", style="magenta")

    sorted_examples = collections.OrderedDict(sorted(EXAMPLES.items()))

    for name, value in sorted_examples.items():
        table.add_row(name, value["description"])

    console = Console()
    console.print(table)
    console.print(
        "Run any RLlib example as using 'rllib example run <Example ID>'."
        "See 'rllib example run --help' for more information."
    )


@examples_app.command()
def run(example_id: str = typer.Argument(..., help="Example ID to run.")):
    if example_id not in EXAMPLES.keys():
        raise ValueError(
            f"Example {example_id} not found. Use `rllib examples list` "
            f"to see available examples."
        )
    train_module.file(
        config_file=EXAMPLES[example_id]["file"],
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


# Register all subcommands
app.add_typer(examples_app, name="example")
app.add_typer(train_module.train_app, name="train")
# TODO: print (a list of) checkpoints available after training.


@app.command()
def evaluate(
    checkpoint: str = cli.Checkpoint,
    run: str = cli.Run,
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
    You have to provide an environment ("--env") an an RLlib algorithm ("--run") to
    evaluate your checkpoint.

    Example usage:\n\n

        rllib evaluate /tmp/ray/checkpoint_dir/checkpoint-0 --run DQN --env CartPole-v1
        --steps 1000000 --out rollouts.pkl
    """
    from ray.rllib import evaluate as evaluate_module

    evaluate_module.run(
        checkpoint=checkpoint,
        run=run,
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
    run: str = cli.Run,
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
    from ray.rllib.utils.deprecation import deprecation_warning

    deprecation_warning(old="rllib rollout", new="rllib evaluate", error=False)

    return evaluate(
        checkpoint=checkpoint,
        run=run,
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
            rllib train --run DQN --env CartPole-v1\n\n

        Example usage for evaluation:\n
            rllib evaluate /trial_dir/checkpoint_000001/checkpoint-1 --run DQN
    --env CartPole-v1
    """


def cli():
    # Keep this function here, it's referenced in the setup.py file, and exposes
    # the CLI as entry point ("rllib" command).
    app()


if __name__ == "__main__":
    cli()
