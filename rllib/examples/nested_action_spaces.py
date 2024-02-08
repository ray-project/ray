from gymnasium.spaces import Dict, Tuple, Box, Discrete, MultiDiscrete
import os

import ray
from ray.tune.registry import register_env
from ray.rllib.connectors.env_to_module import (
    AddLastObservationToBatch,
    FlattenObservations,
    WriteObservationsToEpisodes,
)
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.env.nested_space_repeat_after_me_env import (
    NestedSpaceRepeatAfterMeEnv,
)
from ray.rllib.utils.test_utils import (
    add_rllib_examples_script_args,
    run_rllib_examples_script_experiment,
)
from ray.tune.registry import get_trainable_cls


parser = add_rllib_examples_script_args(default_timesteps=200000, default_reward=-500.0)


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)
    register_env(
        "NestedSpaceRepeatAfterMeEnv", lambda c: NestedSpaceRepeatAfterMeEnv(c)
    )

    # Define env-to-module-connector pipeline for the new stack.
    def _env_to_module_pipeline(env):
        obs_space = env.single_observation_space
        act_space = env.single_action_space
        return [
            AddLastObservationToBatch(obs_space, act_space),
            FlattenObservations(obs_space, act_space),
            WriteObservationsToEpisodes(obs_space, act_space),
        ]

    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # Use new API stack for PPO only.
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .environment(
            "NestedSpaceRepeatAfterMeEnv",
            env_config={
                "space": Dict(
                    {
                        "a": Tuple(
                            [Dict({"d": Box(-10.0, 10.0, ()), "e": Discrete(3)})]
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
            num_rollout_workers=0,
            num_envs_per_worker=20,
            env_runner_cls=SingleAgentEnvRunner if args.enable_new_api_stack else None,
        )
        # No history in Env (bandit problem).
        .training(
            gamma=0.0,
            lr=0.0005,
            model=(
                {} if not args.enable_new_api_stack
                else {"uses_new_env_runners": True}
            ),
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    if args.algo == "PPO":
        config.training(
            # We don't want high entropy in this Env.
            entropy_coeff=0.00005,
            num_sgd_iter=4,
            vf_loss_coeff=0.01,
        )

    run_rllib_examples_script_experiment(config, args)
