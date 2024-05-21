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

import functools

import numpy as np

from ray.air.constants import TRAINING_ITERATION
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.env.utils import try_import_pyspiel, try_import_open_spiel
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.examples.multi_agent.utils import (
    ask_user_for_action,
    SelfPlayCallback,
    SelfPlayCallbackOldAPIStack,
)
from ray.rllib.examples._old_api_stack.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

open_spiel = try_import_open_spiel(error=True)
pyspiel = try_import_pyspiel(error=True)

# Import after try_import_open_spiel, so we can error out with hints.
from open_spiel.python.rl_environment import Environment  # noqa: E402


parser = add_rllib_example_script_args(default_timesteps=2000000)
parser.set_defaults(env="connect_four")
parser.add_argument(
    "--win-rate-threshold",
    type=float,
    default=0.95,
    help="Win-rate at which we setup another opponent by freezing the "
    "current main policy and playing against a uniform distribution "
    "of previously frozen 'main's from here on.",
)
parser.add_argument(
    "--min-league-size",
    type=float,
    default=3,
    help="Minimum number of policies/RLModules to consider the test passed. "
    "The initial league size is 2: `main` and `random`. "
    "`--min-league-size=3` thus means that one new policy/RLModule has been "
    "added so far (b/c the `main` one has reached the `--win-rate-threshold "
    "against the `random` Policy/RLModule).",
)
parser.add_argument(
    "--num-episodes-human-play",
    type=int,
    default=10,
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

    register_env("open_spiel_env", lambda _: OpenSpielEnv(pyspiel.load_game(args.env)))

    def agent_to_module_mapping_fn(agent_id, episode, **kwargs):
        # agent_id = [0|1] -> module depends on episode ID
        # This way, we make sure that both modules sometimes play agent0
        # (start player) and sometimes agent1 (player to move 2nd).
        return "main" if hash(episode.id_) % 2 == agent_id else "random"

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        return "main" if episode.episode_id % 2 == agent_id else "random"

    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .api_stack(
            enable_rl_module_and_learner=args.enable_new_api_stack,
            enable_env_runner_and_connector_v2=args.enable_new_api_stack,
        )
        .environment("open_spiel_env")
        .framework(args.framework)
        # Set up the main piece in this experiment: The league-bases self-play
        # callback, which controls adding new policies/Modules to the league and
        # properly matching the different policies in the league with each other.
        .callbacks(
            functools.partial(
                (
                    SelfPlayCallback
                    if args.enable_new_api_stack
                    else SelfPlayCallbackOldAPIStack
                ),
                win_rate_threshold=args.win_rate_threshold,
            )
        )
        .env_runners(
            num_env_runners=(args.num_env_runners or 2),
            num_envs_per_env_runner=1 if args.enable_new_api_stack else 5,
        )
        .learners(
            num_learners=args.num_gpus,
            num_gpus_per_learner=1 if args.num_gpus else 0,
        )
        .resources(
            num_cpus_for_main_process=1,
        )
        .multi_agent(
            # Initial policy map: Random and default algo one. This will be expanded
            # to more policy snapshots taken from "main" against which "main"
            # will then play (instead of "random"). This is done in the
            # custom callback defined above (`SelfPlayCallback`).
            policies=(
                {
                    # Our main policy, we'd like to optimize.
                    "main": PolicySpec(),
                    # An initial random opponent to play against.
                    "random": PolicySpec(policy_class=RandomPolicy),
                }
                if not args.enable_new_api_stack
                else {"main", "random"}
            ),
            # Assign agent 0 and 1 randomly to the "main" policy or
            # to the opponent ("random" at first). Make sure (via episode_id)
            # that "main" always plays against "random" (and not against
            # another "main").
            policy_mapping_fn=(
                agent_to_module_mapping_fn
                if args.enable_new_api_stack
                else policy_mapping_fn
            ),
            # Always just train the "main" policy.
            policies_to_train=["main"],
        )
        .rl_module(
            model_config_dict={
                "fcnet_hiddens": [512, 512],
                "uses_new_env_runners": args.enable_new_api_stack,
            },
            rl_module_spec=MultiAgentRLModuleSpec(
                module_specs={
                    "main": SingleAgentRLModuleSpec(),
                    "random": SingleAgentRLModuleSpec(module_class=RandomRLModule),
                }
            ),
        )
    )

    # Only for PPO, change the `num_sgd_iter` setting.
    if args.algo == "PPO":
        config.training(num_sgd_iter=20)

    stop = {
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
        TRAINING_ITERATION: args.stop_iters,
        "league_size": args.min_league_size,
    }

    # Train the "main" policy to play really well using self-play.
    results = None
    if not args.from_checkpoint:
        results = run_rllib_example_script_experiment(config, args, stop=stop)

    # Restore trained Algorithm (set to non-explore behavior) and play against
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
            # Switch order of players.
            human_player = 1 - human_player

            num_episodes += 1

        algo.stop()
