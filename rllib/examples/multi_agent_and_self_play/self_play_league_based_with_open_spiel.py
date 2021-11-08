"""Example showing how one can implement a league-based training workflow.

Uses the open spiel adapter of RLlib with the "markov_soccer" game and
a simplified multi-agent, league-based setup:
https://deepmind.com/blog/article/AlphaStar-Grandmaster-level-in- \
StarCraft-II-using-multi-agent-reinforcement-learning

Our league consists of three groups of policies:
- main policies: The current main policy plus prior versions of it.
- main exploiters: Trained by playing only against different "main policies".
- league exploiters: Trained by playing against any policy in the league.

We start with 1 policy from each group, setting all 3 of these to an initial
PPO policy and allowing all 3 policies to be trained.
After each train update - via our custom callback - we decide for each
trainable policy, whether to make a copy and freeze it. Frozen policies
will not be altered anymore. However, they remain in the league for
future matches against trainable policies.
Matchmaking happens via a policy_mapping_fn, which needs to be altered
after every change (addition) to the league. The mapping function
randomly maps agents in a way, such that:
- Frozen main exploiters play against the one (currently trainable) main
  policy.
- Trainable main exploiters play against any main policy (including already
  frozen main policies).
- Frozen league exploiters play against any trainable policy in the league.
- Trainable league exploiters play against any policy in the league.

After training for n iterations, a configurable number of episodes can
be played by the user against the "main" agent on the command line.
"""

import argparse
import numpy as np
import os
from open_spiel.python.rl_environment import Environment
import pyspiel
import re

import ray
from ray import tune
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.examples.self_play_with_open_spiel import ask_user_for_action
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.policy.policy import PolicySpec
from ray.tune import register_env

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--from-checkpoint",
    type=str,
    default=None,
    help="Full path to a checkpoint file for restoring a previously saved "
    "Trainer state.")
parser.add_argument(
    "--env",
    type=str,
    default="markov_soccer",
    choices=["markov_soccer", "connect_four"])
parser.add_argument(
    "--stop-iters",
    type=int,
    default=1000,
    help="Number of iterations to train.")
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=10000000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--win-rate-threshold",
    type=float,
    default=0.85,
    help="Win-rate at which we setup another opponent by freezing the "
    "current main policy and playing against a uniform distribution "
    "of previously frozen 'main's from here on.")
parser.add_argument(
    "--num-episodes-human-play",
    type=int,
    default=10,
    help="How many episodes to play against the user on the command "
    "line after training has finished.")
args = parser.parse_args()


class LeagueBasedSelfPlayCallback(DefaultCallbacks):
    def __init__(self):
        super().__init__()
        # All policies in the league.
        self.main_policies = {"main", "main_0"}
        self.main_exploiters = {"main_exploiter_0", "main_exploiter_1"}
        self.league_exploiters = {"league_exploiter_0", "league_exploiter_1"}
        # Set of currently trainable policies in the league.
        self.trainable_policies = {
            "main", "main_exploiter_1", "league_exploiter_1"
        }
        # Set of currently non-trainable (frozen) policies in the league.
        self.non_trainable_policies = {
            "main_0", "league_exploiter_0", "main_exploiter_0"
        }
        # Store the win rates for league overview printouts.
        self.win_rates = {}

    def on_train_result(self, *, trainer, result, **kwargs):
        # Get the win rate for the train batch.
        # Note that normally, one should set up a proper evaluation config,
        # such that evaluation always happens on the already updated policy,
        # instead of on the already used train_batch.
        for policy_id, rew in result["hist_stats"].items():
            mo = re.match("^policy_(.+)_reward$", policy_id)
            if mo is None:
                continue
            policy_id = mo.group(1)

            # Calculate this policy's win rate.
            won = 0
            for r in rew:
                if r > 0.0:  # win = 1.0; loss = -1.0
                    won += 1
            win_rate = won / len(rew)
            self.win_rates[policy_id] = win_rate

            # Policy is frozen; ignore.
            if policy_id in self.non_trainable_policies:
                continue

            print(
                f"Iter={trainer.iteration} {policy_id}'s "
                f"win-rate={win_rate} -> ",
                end="")

            # If win rate is good -> Snapshot current policy and decide,
            # whether to freeze the copy or not.
            if win_rate > args.win_rate_threshold:
                is_main = re.match("^main(_\\d+)?$", policy_id)
                initializing_exploiters = False

                # First time, main manages a decent win-rate against random:
                # Add league_exploiter_0 and main_exploiter_0 to the mix.
                if is_main and len(self.trainable_policies) == 3:
                    initializing_exploiters = True
                    self.trainable_policies.add("league_exploiter_1")
                    self.trainable_policies.add("main_exploiter_1")
                else:
                    keep_training = False if is_main else np.random.choice(
                        [True, False], p=[0.3, 0.7])
                    if policy_id in self.main_policies:
                        new_pol_id = re.sub("_\\d+$",
                                            f"_{len(self.main_policies) - 1}",
                                            policy_id)
                        self.main_policies.add(new_pol_id)
                    elif policy_id in self.main_exploiters:
                        new_pol_id = re.sub("_\\d+$",
                                            f"_{len(self.main_exploiters)}",
                                            policy_id)
                        self.main_exploiters.add(new_pol_id)
                    else:
                        new_pol_id = re.sub("_\\d+$",
                                            f"_{len(self.league_exploiters)}",
                                            policy_id)
                        self.league_exploiters.add(new_pol_id)

                    if keep_training:
                        self.trainable_policies.add(new_pol_id)
                    else:
                        self.non_trainable_policies.add(new_pol_id)

                    print(f"adding new opponents to the mix ({new_pol_id}).")

                # Update our mapping function accordingly.
                def policy_mapping_fn(agent_id, episode, worker, **kwargs):

                    # Pick, whether this is ...
                    type_ = np.random.choice([1, 2])

                    # 1) League exploiter vs any other.
                    if type_ == 1:
                        league_exploiter = "league_exploiter_" + str(
                            np.random.choice(
                                list(range(len(self.league_exploiters)))))
                        # This league exploiter is frozen: Play against a
                        # trainable policy.
                        if league_exploiter not in self.trainable_policies:
                            opponent = np.random.choice(
                                list(self.trainable_policies))
                        # League exploiter is trainable: Play against any other
                        # non-trainable policy.
                        else:
                            opponent = np.random.choice(
                                list(self.non_trainable_policies))
                        print(f"{league_exploiter} vs {opponent}")
                        return league_exploiter if \
                            episode.episode_id % 2 == agent_id else opponent

                    # 2) Main exploiter vs main.
                    else:
                        main_exploiter = "main_exploiter_" + str(
                            np.random.choice(
                                list(range(len(self.main_exploiters)))))
                        # Main exploiter is frozen: Play against the main
                        # policy.
                        if main_exploiter not in self.trainable_policies:
                            main = "main"
                        # Main exploiter is trainable: Play against any
                        # frozen main.
                        else:
                            main = np.random.choice(
                                list(self.main_policies - {"main"}))
                        # print(f"{main_exploiter} vs {main}")
                        return main_exploiter if \
                            episode.episode_id % 2 == agent_id else main

                # Set the weights of the new polic(y/ies).
                if initializing_exploiters:
                    main_state = trainer.get_policy("main").get_state()
                    pol_map = trainer.workers.local_worker().policy_map
                    pol_map["main_0"].set_state(main_state)
                    pol_map["league_exploiter_1"].set_state(main_state)
                    pol_map["main_exploiter_1"].set_state(main_state)
                    # We need to sync the just copied local weights to all the
                    # remote workers as well.
                    trainer.workers.sync_weights(policies=[
                        "main_0", "league_exploiter_1", "main_exploiter_1"
                    ])

                    def _set(worker):
                        worker.set_policy_mapping_fn(policy_mapping_fn)
                        worker.set_policies_to_train(self.trainable_policies)

                    trainer.workers.foreach_worker(_set)
                else:
                    new_policy = trainer.add_policy(
                        policy_id=new_pol_id,
                        policy_cls=type(trainer.get_policy(policy_id)),
                        policy_mapping_fn=policy_mapping_fn,
                        policies_to_train=self.trainable_policies,
                    )
                    main_state = trainer.get_policy(policy_id).get_state()
                    new_policy.set_state(main_state)
                    # We need to sync the just copied local weights to all the
                    # remote workers as well.
                    trainer.workers.sync_weights(policies=[new_pol_id])

                self._print_league()

            else:
                print("not good enough; will keep learning ...")

    def _print_league(self):
        print("--- League ---")
        print("Trainable policies (win-rates):")
        for p in sorted(self.trainable_policies):
            wr = self.win_rates[p] if p in self.win_rates else 0.0
            print(f"\t{p}: {wr}")
        print("Frozen policies:")
        for p in sorted(self.non_trainable_policies):
            wr = self.win_rates[p] if p in self.win_rates else 0.0
            print(f"\t{p}: {wr}")
        print()


if __name__ == "__main__":
    ray.init(
        num_cpus=args.num_cpus or None,
        include_dashboard=False,
    )

    register_env("open_spiel_env",
                 lambda _: OpenSpielEnv(pyspiel.load_game(args.env)))

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        # At first, only have main play against the random main exploiter.
        return "main" if episode.episode_id % 2 == agent_id \
            else "main_exploiter_0"

    config = {
        "env": "open_spiel_env",
        "callbacks": LeagueBasedSelfPlayCallback,
        "num_sgd_iter": 20,
        "num_envs_per_worker": 5,
        "multiagent": {
            # Initial policy map: All PPO. This will be expanded
            # to more policy snapshots. This is done in the
            # custom callback defined above (`LeagueBasedSelfPlayCallback`).
            "policies": {
                # Our main policy, we'd like to optimize.
                "main": PolicySpec(),
                # First frozen version of main (after we reach n% win-rate).
                "main_0": PolicySpec(),
                # Initial main exploiters (one random, one trainable).
                "main_exploiter_0": PolicySpec(policy_class=RandomPolicy),
                "main_exploiter_1": PolicySpec(),
                # Initial league exploiters (one random, one trainable).
                "league_exploiter_0": PolicySpec(policy_class=RandomPolicy),
                "league_exploiter_1": PolicySpec(),
            },
            "policy_mapping_fn": policy_mapping_fn,
            # At first, only train main_0 (until good enough to win against
            # random).
            "policies_to_train": ["main"],
        },
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
    }

    stop = {
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    # Train the "main" policy to play really well using self-play.
    results = None
    if not args.from_checkpoint:
        results = tune.run(
            "PPO",
            config=config,
            stop=stop,
            checkpoint_at_end=True,
            checkpoint_freq=10,
            verbose=3)

    # Restore trained trainer (set to non-explore behavior) and play against
    # human on command line.
    if args.num_episodes_human_play > 0:
        num_episodes = 0
        trainer = PPOTrainer(config=dict(config, **{"explore": False}))
        if args.from_checkpoint:
            trainer.restore(args.from_checkpoint)
        else:
            checkpoint = results.get_last_checkpoint()
            if not checkpoint:
                raise ValueError("No last checkpoint found in results!")
            trainer.restore(checkpoint)

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
                    obs = np.array(
                        time_step.observations["info_state"][player_id])
                    action = trainer.compute_single_action(
                        obs, policy_id="main")
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

    ray.shutdown()
