"""Example showing how to implement a league-based training workflow.

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
import functools

import numpy as np

import ray
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.utils import try_import_pyspiel, try_import_open_spiel
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.examples.multi_agent_and_self_play.utils import (
    ask_user_for_action,
    SelfPlayLeagueBasedCallback,
    SelfPlayLeagueBasedCallbackOldAPIStack,
)
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.examples.rl_module.random_rl_module import RandomRLModule
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

open_spiel = try_import_open_spiel(error=True)
pyspiel = try_import_pyspiel(error=True)

# Import after try_import_open_spiel, so we can error out with hints
from open_spiel.python.rl_environment import Environment  # noqa: E402


parser = add_rllib_example_script_args(default_timesteps=2000000)
parser.add_argument(
    "--env",
    type=str,
    default="markov_soccer",
    choices=["markov_soccer", "connect_four"],
)
parser.add_argument(
    "--win-rate-threshold",
    type=float,
    default=0.85,
    help="Win-rate at which we setup another opponent by freezing the "
    "current main policy and playing against a uniform distribution "
    "of previously frozen 'main's from here on.",
)
parser.add_argument(
    "--min-league-size",
    type=float,
    default=8,
    help="Minimum number of policies/RLModules to consider the test passed. "
    "The initial league size is 2: `main` and `random`. "
    "`--min-league-size=3` thus means that one new policy/RLModule has been "
    "added so far (b/c the `main` one has reached the `--win-rate-threshold "
    "against the `random` Policy/RLModule).",
)
parser.add_argument(
    "--num-episodes-human-play",
    type=int,
    default=0,
    help="How many episodes to play against the user on the command "
    "line after training has finished.",
)
parser.add_argument(
    "--from-checkpoint",
    type=str,
    default=None,
    help="Full path to a checkpoint file for restoring a previously saved "
    "Algorithm state.",
)


if __name__ == "__main__":
    args = parser.parse_args()

    register_env(
        "open_spiel_env",
        lambda _: OpenSpielEnv(pyspiel.load_game(args.env)),
    )

    def policy_mapping_fn(agent_id, episode, worker=None, **kwargs):
        # At first, only have main play against the random main exploiter.
        return "main" if episode.episode_id % 2 == agent_id else "main_exploiter_0"

    def agent_to_module_mapping_fn(agent_id, episode, **kwargs):
        # At first, only have main play against the random main exploiter.
        return "main" if hash(episode.id_) % 2 == agent_id else "main_exploiter_0"

    def _get_multi_agent():
        names = {
            # Our main policy, we'd like to optimize.
            "main",
            # First frozen version of main (after we reach n% win-rate).
            "main_0",
            # Initial main exploiters (one random, one trainable).
            "main_exploiter_0",
            "main_exploiter_1",
            # Initial league exploiters (one random, one trainable).
            "league_exploiter_0",
            "league_exploiter_1",
        }
        if args.enable_new_api_stack:
            policies = names
            spec = {
                mid: SingleAgentRLModuleSpec(
                    module_class=(
                        RandomRLModule
                        if mid in ["main_exploiter_0", "league_exploiter_0"]
                        else None
                    )
                )
                for mid in names
            }
        else:
            policies = {
                mid: PolicySpec(
                    policy_class=(
                        RandomPolicy
                        if mid in ["main_exploiter_0", "league_exploiter_0"]
                        else None
                    )
                )
                for mid in names
            }
            spec = None
        return {"policies": policies, "spec": spec}

    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # Use new API stack ...
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .environment("open_spiel_env")
        .framework(args.framework)
        # Set up the main piece in this experiment: The league-bases self-play
        # callback, which controls adding new policies/Modules to the league and
        # properly matching the different policies in the league with each other.
        .callbacks(
            functools.partial(
                SelfPlayLeagueBasedCallback
                if args.enable_new_api_stack
                else SelfPlayLeagueBasedCallbackOldAPIStack,
                win_rate_threshold=args.win_rate_threshold,
            )
        )
        .rollouts(
            num_rollout_workers=args.num_env_runners,
            num_envs_per_worker=1 if args.enable_new_api_stack else 5,
            # Set up the correct env-runner to use depending on
            # old-stack/new-stack and multi-agent settings.
            env_runner_cls=(
                None if not args.enable_new_api_stack else MultiAgentEnvRunner
            ),
        )
        .resources(
            num_learner_workers=args.num_gpus,
            num_gpus_per_learner_worker=1 if args.num_gpus else 0,
            num_cpus_for_local_worker=1,
        )
        .training(
            num_sgd_iter=20,
            model=dict(
                **({"uses_new_env_runners": True} if args.enable_new_api_stack else {}),
            ),
        )
        .multi_agent(
            # Initial policy map: All PPO. This will be expanded
            # to more policy snapshots. This is done in the
            # custom callback defined above (`LeagueBasedSelfPlayCallback`).
            policies=_get_multi_agent()["policies"],
            policy_mapping_fn=(
                agent_to_module_mapping_fn
                if args.enable_new_api_stack
                else policy_mapping_fn
            ),
            # At first, only train main_0 (until good enough to win against
            # random).
            policies_to_train=["main"],
        )
        .rl_module(
            rl_module_spec=MultiAgentRLModuleSpec(
                module_specs=_get_multi_agent()["spec"]
            ),
        )
    )

    # Run everything as configured.
    # Train the "main" policy to play really well using self-play.
    results = None
    if not args.from_checkpoint:
        stop = {
            "timesteps_total": args.stop_timesteps,
            "training_iteration": args.stop_iters,
            "league_size": args.min_league_size,
        }
        results = run_rllib_example_script_experiment(config, args, stop=stop)

    # Restore trained Algorithm (set to non-explore behavior) and play against
    # human on command line.
    if args.num_episodes_human_play > 0:
        num_episodes = 0
        # Switch off exploration for better inference performance.
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
                    if config.uses_new_env_runners:
                        action = algo.workers.local_worker().module.forward_inference(
                            {"obs": obs}
                        )
                    else:
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
