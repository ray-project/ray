"""Example showing how one can implement a simple self-play training workflow.

Uses the open spiel adapter of RLlib with the "connect_four" game and
a multi-agent setup with a "main" policy and n "main_v[x]" policies
(x=version number), which are all at-some-point-frozen copies of
"main". At the very beginning, "main" plays against RandomPolicy.

Checks for the training progress after each training update via a custom
callback. We simply measure the win rate of "main" vs the opponent
("main_v[x]" or RandomPolicy at the beginning) by looking through the
achieved rewards in the episodes in the train batch. If this win rate
reaches some configurable threshold, we add a new policy to
the policy map (a frozen copy of the current "main" one) and change the
policy_mapping_fn to make new matches of "main" vs any of the previous
versions of "main" (including the just added one).

After training for n iterations, a configurable number of episodes can
be played by the user against the "main" agent on the command line.
"""

import argparse
import gymnasium as gym
import os
import sys

import numpy as np

import ray
from ray import air, tune
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.utils import try_import_pyspiel, try_import_open_spiel
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.tune import CLIReporter

open_spiel = try_import_open_spiel(error=True)
pyspiel = try_import_pyspiel(error=True)

# Import after try_import_open_spiel, so we can error out with hints
from open_spiel.python.rl_environment import Environment  # noqa: E402


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "torch"],
        default="tf",
        help="The DL framework specifier.",
    )
    parser.add_argument("--num-cpus", type=int, default=0)
    parser.add_argument("--num-workers", type=int, default=2)
    parser.add_argument(
        "--from-checkpoint",
        type=str,
        default=None,
        help="Full path to a checkpoint file for restoring a previously saved "
        "Algorithm state.",
    )
    parser.add_argument(
        "--env",
        type=str,
        default="connect_four",
        choices=["markov_soccer", "connect_four"],
    )
    parser.add_argument(
        "--stop-iters", type=int, default=200, help="Number of iterations to train."
    )
    parser.add_argument(
        "--stop-timesteps",
        type=int,
        default=10000000,
        help="Number of timesteps to train.",
    )
    parser.add_argument(
        "--win-rate-threshold",
        type=float,
        default=0.95,
        help="Win-rate at which we setup another opponent by freezing the "
        "current main policy and playing against a uniform distribution "
        "of previously frozen 'main's from here on.",
    )
    parser.add_argument(
        "--num-episodes-human-play",
        type=int,
        default=10,
        help="How many episodes to play against the user on the command "
        "line after training has finished.",
    )
    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


def ask_user_for_action(time_step):
    """Asks the user for a valid action on the command line and returns it.

    Re-queries the user until she picks a valid one.

    Args:
        time_step: The open spiel Environment time-step object.
    """
    pid = time_step.observations["current_player"]
    legal_moves = time_step.observations["legal_actions"][pid]
    choice = -1
    while choice not in legal_moves:
        print("Choose an action from {}:".format(legal_moves))
        sys.stdout.flush()
        choice_str = input()
        try:
            choice = int(choice_str)
        except ValueError:
            continue
    return choice


class SelfPlayCallback(DefaultCallbacks):
    def __init__(self):
        super().__init__()
        # 0=RandomPolicy, 1=1st main policy snapshot,
        # 2=2nd main policy snapshot, etc..
        self.current_opponent = 0

    def on_train_result(self, *, algorithm, result, **kwargs):
        # Get the win rate for the train batch.
        # Note that normally, one should set up a proper evaluation config,
        # such that evaluation always happens on the already updated policy,
        # instead of on the already used train_batch.
        main_rew = result["hist_stats"].pop("policy_main_reward")
        opponent_rew = list(result["hist_stats"].values())[0]
        assert len(main_rew) == len(opponent_rew)
        won = 0
        for r_main, r_opponent in zip(main_rew, opponent_rew):
            if r_main > r_opponent:
                won += 1
        win_rate = won / len(main_rew)
        result["win_rate"] = win_rate
        print(f"Iter={algorithm.iteration} win-rate={win_rate} -> ", end="")
        # If win rate is good -> Snapshot current policy and play against
        # it next, keeping the snapshot fixed and only improving the "main"
        # policy.
        if win_rate > args.win_rate_threshold:
            self.current_opponent += 1
            new_pol_id = f"main_v{self.current_opponent}"
            print(f"adding new opponent to the mix ({new_pol_id}).")

            # Re-define the mapping function, such that "main" is forced
            # to play against any of the previously played policies
            # (excluding "random").
            def policy_mapping_fn(agent_id, episode, worker, **kwargs):
                # agent_id = [0|1] -> policy depends on episode ID
                # This way, we make sure that both policies sometimes play
                # (start player) and sometimes agent1 (player to move 2nd).
                return (
                    "main"
                    if episode.episode_id % 2 == agent_id
                    else "main_v{}".format(
                        np.random.choice(list(range(1, self.current_opponent + 1)))
                    )
                )

            new_policy = algorithm.add_policy(
                policy_id=new_pol_id,
                policy_cls=type(algorithm.get_policy("main")),
                policy_mapping_fn=policy_mapping_fn,
            )

            # Set the weights of the new policy to the main policy.
            # We'll keep training the main policy, whereas `new_pol_id` will
            # remain fixed.
            main_state = algorithm.get_policy("main").get_state()
            new_policy.set_state(main_state)
            # We need to sync the just copied local weights (from main policy)
            # to all the remote workers as well.
            algorithm.workers.sync_weights()
        else:
            print("not good enough; will keep learning ...")

        # +2 = main + random
        result["league_size"] = self.current_opponent + 2


if __name__ == "__main__":

    args = get_cli_args()
    ray.init(num_cpus=args.num_cpus or None, include_dashboard=False)

    gym.register("open_spiel_env", lambda: OpenSpielEnv(pyspiel.load_game(args.env)))

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        # agent_id = [0|1] -> policy depends on episode ID
        # This way, we make sure that both policies sometimes play agent0
        # (start player) and sometimes agent1 (player to move 2nd).
        return "main" if episode.episode_id % 2 == agent_id else "random"

    config = (
        PPOConfig()
        .environment("open_spiel_env")
        .framework(args.framework)
        .callbacks(SelfPlayCallback)
        .rollouts(num_envs_per_worker=5, num_rollout_workers=args.num_workers)
        .training(num_sgd_iter=20, model={"fcnet_hiddens": [512, 512]})
        .multi_agent(
            # Initial policy map: Random and PPO. This will be expanded
            # to more policy snapshots taken from "main" against which "main"
            # will then play (instead of "random"). This is done in the
            # custom callback defined above (`SelfPlayCallback`).
            policies={
                # Our main policy, we'd like to optimize.
                "main": PolicySpec(),
                # An initial random opponent to play against.
                "random": PolicySpec(policy_class=RandomPolicy),
            },
            # Assign agent 0 and 1 randomly to the "main" policy or
            # to the opponent ("random" at first). Make sure (via episode_id)
            # that "main" always plays against "random" (and not against
            # another "main").
            policy_mapping_fn=policy_mapping_fn,
            # Always just train the "main" policy.
            policies_to_train=["main"],
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    stop = {
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    # Train the "main" policy to play really well using self-play.
    results = None
    if not args.from_checkpoint:
        results = tune.Tuner(
            "PPO",
            param_space=config,
            run_config=air.RunConfig(
                stop=stop,
                verbose=2,
                progress_reporter=CLIReporter(
                    metric_columns={
                        "training_iteration": "iter",
                        "time_total_s": "time_total_s",
                        "timesteps_total": "ts",
                        "episodes_this_iter": "train_episodes",
                        "policy_reward_mean/main": "reward",
                        "win_rate": "win_rate",
                        "league_size": "league_size",
                    },
                    sort_by_metric=True,
                ),
                checkpoint_config=air.CheckpointConfig(
                    checkpoint_at_end=True,
                    checkpoint_frequency=10,
                ),
            ),
        ).fit()

    # Restore trained trainer (set to non-explore behavior) and play against
    # human on command line.
    if args.num_episodes_human_play > 0:
        num_episodes = 0
        config.explore = False
        algo = config.build()
        if args.from_checkpoint:
            algo.restore(args.from_checkpoint)
        else:
            checkpoint = results.get_best_result().checkpoint
            if not checkpoint:
                raise ValueError("No last checkpoint found in results!")
            algo.restore(checkpoint)

        # Play from the command line against the trained agent
        # in an actual (non-RLlib-wrapped) open-spiel env.
        human_player = 1
        env = Environment(args.env)

        while num_episodes < args.num_episodes_human_play:
            print("You play as {}".format("o" if human_player else "x"))
            time_step = env.reset()
            while not time_step.last():
                player_id = time_step.observations["current_player"]
                if player_id == human_player:
                    action = ask_user_for_action(time_step)
                else:
                    obs = np.array(time_step.observations["info_state"][player_id])
                    action = algo.compute_single_action(obs, policy_id="main")
                    # In case computer chooses an invalid action, pick a
                    # random one.
                    legal = time_step.observations["legal_actions"][player_id]
                    if action not in legal:
                        action = np.random.choice(legal)
                time_step = env.step([action])
                print(f"\n{env.get_state}")

            print(f"\n{env.get_state}")

            print("End of game!")
            if time_step.rewards[human_player] > 0:
                print("You win")
            elif time_step.rewards[human_player] < 0:
                print("You lose")
            else:
                print("Draw")
            # Switch order of players
            human_player = 1 - human_player

            num_episodes += 1

        algo.stop()

    ray.shutdown()
