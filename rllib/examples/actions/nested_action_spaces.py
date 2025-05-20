from gymnasium.spaces import Dict, Tuple, Box, Discrete, MultiDiscrete

from ray.tune.registry import register_env
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.examples.envs.classes.multi_agent import (
    MultiAgentNestedSpaceRepeatAfterMeEnv,
)
from ray.rllib.examples.envs.classes.nested_space_repeat_after_me_env import (
    NestedSpaceRepeatAfterMeEnv,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls


# Read in common example script command line arguments.
parser = add_rllib_example_script_args(default_timesteps=200000, default_reward=-500.0)
parser.set_defaults(enable_new_api_stack=True)


if __name__ == "__main__":
    args = parser.parse_args()

    # Define env-to-module-connector pipeline for the new stack.
    def _env_to_module_pipeline(env):
        return FlattenObservations(multi_agent=args.num_agents > 0)

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
    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
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
        .env_runners(env_to_module_connector=_env_to_module_pipeline)
        # No history in Env (bandit problem).
        .training(
            gamma=0.0,
            lr=0.0005,
        )
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Fix some PPO-specific settings.
    if args.algo == "PPO":
        base_config.training(
            # We don't want high entropy in this Env.
            entropy_coeff=0.00005,
            num_epochs=4,
            vf_loss_coeff=0.01,
        )

    # Run everything as configured.
    run_rllib_example_script_experiment(base_config, args)
