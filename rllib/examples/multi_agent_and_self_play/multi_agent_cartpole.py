"""Simple example of setting up an agent-to-module mapping function.

Control the number of agents and policies (RLModules) via --num-agents and
--num-policies.

This works with hundreds of agents and policies, but note that initializing
many policies might take some time.
"""

from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

parser = add_rllib_example_script_args(
    default_iters=200,
    default_timesteps=100000,
    default_reward=600.0,
)
parser.add_argument("--num-policies", type=int, default=2)


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentCartPole(config={"num_agents": args.num_agents}),
        )

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("env" if args.num_agents > 0 else "CartPole-v1")
        .rollouts(
            # TODO (sven): MAEnvRunner does not support vectorized envs yet
            #  due to gym's env checkers and non-compatability with RLlib's
            #  MultiAgentEnv API.
            num_envs_per_worker=1 if args.num_agents > 0 else 20,
        )
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    run_rllib_example_script_experiment(base_config, args)
