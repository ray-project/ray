from gymnasium.spaces import Dict, Tuple, Box, Discrete, MultiDiscrete
import os

from ray.tune.registry import register_env
from ray.rllib.connectors.env_to_module import (
    AddObservationsFromEpisodesToBatch,
    FlattenObservations,
    WriteObservationsToEpisodes,
)
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.env.multi_agent import MultiAgentNestedSpaceRepeatAfterMeEnv
from ray.rllib.examples.env.nested_space_repeat_after_me_env import (
    NestedSpaceRepeatAfterMeEnv,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls


# Read in common example script command line arguments.
parser = add_rllib_example_script_args(default_timesteps=200000, default_reward=-500.0)


if __name__ == "__main__":
    args = parser.parse_args()

    # Define env-to-module-connector pipeline for the new stack.
    def _env_to_module_pipeline(env):
        return [
            AddObservationsFromEpisodesToBatch(),
            FlattenObservations(multi_agent=args.num_agents > 0),
            WriteObservationsToEpisodes(),
        ]

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda c: MultiAgentNestedSpaceRepeatAfterMeEnv(
                config=dict(c, **{"num_agents": args.num_agents})
            ),
        )
    else:
        register_env("env", lambda c: NestedSpaceRepeatAfterMeEnv(c))

    # Define the AlgorithmConfig used.
    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # Use new API stack for PPO only.
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .environment(
            "env",
            env_config={
                "space": Dict(
                    {
                        "a": Tuple(
                            [Dict({"d": Box(-15.0, 3.0, ()), "e": Discrete(3)})]
                        ),
                        "b": Box(-10.0, 10.0, (2,)),
                        "c": MultiDiscrete([3, 3]),
                        "d": Discrete(2),
                    }
                ),
                "episode_len": 100,
            },
        )
        .framework(args.framework)
        .rollouts(
            env_to_module_connector=_env_to_module_pipeline,
            num_rollout_workers=args.num_env_runners,
            # Setup the correct env-runner to use depending on
            # old-stack/new-stack and multi-agent settings.
            env_runner_cls=(
                None
                if not args.enable_new_api_stack
                else SingleAgentEnvRunner
                if args.num_agents == 0
                else MultiAgentEnvRunner
            ),
        )
        # No history in Env (bandit problem).
        .training(
            gamma=0.0,
            lr=0.0005,
            model=(
                {} if not args.enable_new_api_stack else {"uses_new_env_runners": True}
            ),
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Fix some PPO-specific settings.
    if args.algo == "PPO":
        config.training(
            # We don't want high entropy in this Env.
            entropy_coeff=0.00005,
            num_sgd_iter=4,
            vf_loss_coeff=0.01,
        )

    # Run everything as configured.
    run_rllib_example_script_experiment(config, args)
