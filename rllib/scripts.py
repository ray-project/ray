#!/usr/bin/env python
import typer
from ray.rllib import train
from ray.rllib.common import CLIArguments as cli

# Main Typer CLI app
app = typer.Typer()

# Register all "train" sub-commands
app.add_typer(train.train_app, name="train")


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
    from ray.rllib import evaluate

    evaluate.run(
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
