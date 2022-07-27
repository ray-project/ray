"""Example showing how one can restore a connector enabled TF policy
checkpoint for a new self-play PyTorch training job.
The checkpointed policy may be trained with a different algorithm too.
"""

import argparse
import pyspiel

import ray
from ray import air, tune
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.policy import parse_policy_specs_from_checkpoint
from ray.tune import CLIReporter, register_env

parser = argparse.ArgumentParser()
# This should point to a checkpointed policy that plays connect_four.
# Note that this policy may be trained with different algorithms than
# the current one.
parser.add_argument(
    "--checkpoint_file",
    type=str,
    default="",
    help="Path to a connector enabled checkpoint file for restoring.",
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
        policy_config, policy_specs, policy_states = parse_policy_specs_from_checkpoint(
            args.checkpoint_file
        )

        assert args.policy_id in policy_specs, (
            f"Could not find policy {args.policy_id}. "
            f"Available policies are {list(policy_specs.keys())}"
        )
        policy_spec = policy_specs[args.policy_id]
        policy_state = (
            policy_states[args.policy_id] if args.policy_id in policy_states else None
        )
        config = merge_dicts(policy_config, policy_spec.config or {})

        # Add restored policy to trainer.
        # Note that this policy doesn't have to be trained with the same algorithm
        # of the training stack. You can even mix up TF policies with a Torch stack.
        algorithm.add_policy(
            policy_id="opponent",
            policy_cls=policy_spec.policy_class,
            observation_space=policy_spec.observation_space,
            action_space=policy_spec.action_space,
            config=config,
            policy_state=policy_state,
            evaluation_workers=True,
        )


if __name__ == "__main__":
    ray.init()

    register_env(
        "open_spiel_env", lambda _: OpenSpielEnv(pyspiel.load_game("connect_four"))
    )

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        # main policy plays against opponent policy.
        return "main" if episode.episode_id % 2 == agent_id else "opponent"

    config = {
        "env": "open_spiel_env",
        "callbacks": AddPolicyCallback,
        "model": {
            "fcnet_hiddens": [512, 512],
        },
        "num_envs_per_worker": 5,
        "multiagent": {
            # Initial policy map: Random and PPO. This will be expanded
            # to more policy snapshots taken from "main" against which "main"
            # will then play (instead of "random"). This is done in the
            # custom callback defined above (`SelfPlayCallback`).
            "policies": {
                # Our main policy, we'd like to optimize.
                "main": PolicySpec(),
                # Note: We will add the "opponent" policy with callback.
            },
            # Assign agent 0 and 1 randomly to the "main" policy or
            # to the opponent ("random" at first). Make sure (via episode_id)
            # that "main" always plays against "random" (and not against
            # another "main").
            "policy_mapping_fn": policy_mapping_fn,
            # Always just train the "main" policy.
            "policies_to_train": ["main"],
        },
        "num_workers": 1,
        "framework": "torch",
        # We will be restoring a TF2 policy.
        # So tell the RolloutWorkers to enable TF eager exec as well, even if
        # framework is set to torch.
        "enable_tf1_exec_eagerly": True,
    }

    stop = {
        "training_iteration": args.train_iteration,
    }

    # Train the "main" policy to play really well using self-play.
    tuner = tune.Tuner(
        "SAC",
        param_space=config,
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
