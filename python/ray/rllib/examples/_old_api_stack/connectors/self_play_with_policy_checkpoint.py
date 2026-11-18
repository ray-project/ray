# @OldAPIStack
"""Example showing to restore a connector enabled TF policy
checkpoint for a new self-play PyTorch training job.
You can train the checkpointed policy with a different algorithm too.
"""

import argparse
import os
import tempfile
from functools import partial

import ray
from ray import tune
from ray.rllib.algorithms.sac import SACConfig
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.env.utils import try_import_pyspiel
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.examples._old_api_stack.connectors.prepare_checkpoint import (
    create_open_spiel_checkpoint,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_EPISODES,
)
from ray.tune import CLIReporter, register_env
from ray.tune.result import TRAINING_ITERATION

pyspiel = try_import_pyspiel(error=True)
register_env(
    "open_spiel_env", lambda _: OpenSpielEnv(pyspiel.load_game("connect_four"))
)


parser = argparse.ArgumentParser()
parser.add_argument(
    "--train_iteration",
    type=int,
    default=10,
    help="Number of iterations to train.",
)
args = parser.parse_args()


MAIN_POLICY_ID = "main"
OPPONENT_POLICY_ID = "opponent"


class AddPolicyCallback(RLlibCallback):
    def __init__(self, checkpoint_dir):
        self._checkpoint_dir = checkpoint_dir
        super().__init__()

    def on_algorithm_init(self, *, algorithm, metrics_logger, **kwargs):
        policy = Policy.from_checkpoint(
            self._checkpoint_dir, policy_ids=[OPPONENT_POLICY_ID]
        )

        # Add restored policy to Algorithm.
        # Note that this policy doesn't have to be trained with the same algorithm
        # of the training stack. You can even mix up TF policies with a Torch stack.
        algorithm.add_policy(
            policy_id=OPPONENT_POLICY_ID,
            policy=policy,
            add_to_eval_env_runners=True,
        )


def policy_mapping_fn(agent_id, episode, worker, **kwargs):
    # main policy plays against opponent policy.
    return MAIN_POLICY_ID if episode.episode_id % 2 == agent_id else OPPONENT_POLICY_ID


def main(checkpoint_dir):
    config = (
        SACConfig()
        .environment("open_spiel_env")
        .framework("torch")
        .callbacks(partial(AddPolicyCallback, checkpoint_dir))
        .env_runners(
            num_env_runners=1,
            num_envs_per_env_runner=5,
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
            policies={MAIN_POLICY_ID},  # Our main policy, we'd like to optimize.
            # Assign agent 0 and 1 randomly to the "main" policy or
            # to the opponent ("random" at first). Make sure (via episode_id)
            # that "main" always plays against "random" (and not against
            # another "main").
            policy_mapping_fn=policy_mapping_fn,
            # Always just train the "main" policy.
            policies_to_train=[MAIN_POLICY_ID],
        )
    )

    stop = {TRAINING_ITERATION: args.train_iteration}

    # Train the "main" policy to play really well using self-play.
    tuner = tune.Tuner(
        "SAC",
        param_space=config.to_dict(),
        run_config=tune.RunConfig(
            stop=stop,
            checkpoint_config=tune.CheckpointConfig(
                checkpoint_at_end=True,
                checkpoint_frequency=10,
            ),
            verbose=2,
            progress_reporter=CLIReporter(
                metric_columns={
                    TRAINING_ITERATION: "iter",
                    "time_total_s": "time_total_s",
                    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": "ts",
                    f"{ENV_RUNNER_RESULTS}/{NUM_EPISODES}": "train_episodes",
                    (
                        f"{ENV_RUNNER_RESULTS}/module_episode_returns_mean/main"
                    ): "reward_main",
                },
                sort_by_metric=True,
            ),
        ),
    )
    tuner.fit()


if __name__ == "__main__":
    ray.init()

    with tempfile.TemporaryDirectory() as tmpdir:
        create_open_spiel_checkpoint(tmpdir)

        policy_checkpoint_path = os.path.join(
            tmpdir,
            "checkpoint_000000",
            "policies",
            OPPONENT_POLICY_ID,
        )

        main(policy_checkpoint_path)

    ray.shutdown()
