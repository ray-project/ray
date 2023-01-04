"""Example showing how one can restore a connector enabled TF policy
checkpoint for a new self-play PyTorch training job.
The checkpointed policy may be trained with a different algorithm too.
"""

import argparse
import gymnasium as gym
from pathlib import Path
import pyspiel

import ray
from ray import air, tune
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.policy.policy import Policy
from ray.tune import CLIReporter

parser = argparse.ArgumentParser()
# This should point to a checkpointed policy that plays connect_four.
# Note that this policy may be trained with different algorithms than
# the current one.
parser.add_argument(
    "--checkpoint_file",
    type=str,
    default="",
    help=(
        "Path to a connector enabled checkpoint file for restoring,"
        "relative to //ray/rllib/ folder."
    ),
)
parser.add_argument(
    "--policy_id",
    default="main",
    help="ID of policy to load.",
)
parser.add_argument(
    "--train_iteration",
    type=int,
    default=10,
    help="Number of iterations to train.",
)
args = parser.parse_args()

assert args.checkpoint_file, "Must specify --checkpoint_file flag."


class AddPolicyCallback(DefaultCallbacks):
    def __init__(self):
        super().__init__()

    def on_algorithm_init(self, *, algorithm, **kwargs):
        checkpoint_path = str(
            Path(__file__)
            .parent.parent.parent.absolute()
            .joinpath(args.checkpoint_file)
        )
        policy = Policy.from_checkpoint(checkpoint_path)

        # Add restored policy to trainer.
        # Note that this policy doesn't have to be trained with the same algorithm
        # of the training stack. You can even mix up TF policies with a Torch stack.
        algorithm.add_policy(
            policy_id="opponent",
            policy=policy,
            evaluation_workers=True,
        )


if __name__ == "__main__":
    ray.init()

    gym.register(
        "open_spiel_env", lambda: OpenSpielEnv(pyspiel.load_game("connect_four"))
    )

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        # main policy plays against opponent policy.
        return "main" if episode.episode_id % 2 == agent_id else "opponent"

    config = (
        SACConfig()
        .environment("open_spiel_env")
        .framework("torch")
        .callbacks(AddPolicyCallback)
        .rollouts(
            num_rollout_workers=1,
            num_envs_per_worker=5,
            # We will be restoring a TF2 policy.
            # So tell the RolloutWorkers to enable TF eager exec as well, even if
            # framework is set to torch.
            enable_tf1_exec_eagerly=True,
        )
        .training(model={"fcnet_hiddens": [512, 512]})
        .multi_agent(
            # Initial policy map: Random and PPO. This will be expanded
            # to more policy snapshots taken from "main" against which "main"
            # will then play (instead of "random"). This is done in the
            # custom callback defined above (`SelfPlayCallback`).
            # Note: We will add the "opponent" policy with callback.
            policies={"main"},  # Our main policy, we'd like to optimize.
            # Assign agent 0 and 1 randomly to the "main" policy or
            # to the opponent ("random" at first). Make sure (via episode_id)
            # that "main" always plays against "random" (and not against
            # another "main").
            policy_mapping_fn=policy_mapping_fn,
            # Always just train the "main" policy.
            policies_to_train=["main"],
        )
    )

    stop = {
        "training_iteration": args.train_iteration,
    }

    # Train the "main" policy to play really well using self-play.
    tuner = tune.Tuner(
        "SAC",
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop,
            checkpoint_config=air.CheckpointConfig(
                checkpoint_at_end=True,
                checkpoint_frequency=10,
            ),
            verbose=2,
            progress_reporter=CLIReporter(
                metric_columns={
                    "training_iteration": "iter",
                    "time_total_s": "time_total_s",
                    "timesteps_total": "ts",
                    "episodes_this_iter": "train_episodes",
                    "policy_reward_mean/main": "reward_main",
                },
                sort_by_metric=True,
            ),
        ),
    )
    tuner.fit()

    ray.shutdown()
