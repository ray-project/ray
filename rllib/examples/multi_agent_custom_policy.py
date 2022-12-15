"""Example of running a custom hand-coded policy alongside trainable policies.

This example has two policies:
    (1) a simple simple policy trained with PPO optimizer
    (2) a hand-coded policy that acts at random in the env (doesn't learn)

In the console output, you can see the PPO policy does much better than random:
Result for PPO_multi_cartpole_0:
  ...
  policy_reward_mean:
    learnable_policy: 185.23
    random: 21.255
  ...
"""

import argparse
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import register_env

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=20, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)
parser.add_argument(
    "--enable-rl-module-api", action="store_true", help="Use RLModule API."
)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    # Simple environment with 4 independent cartpole entities
    register_env(
        "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": 4})
    )

    config = (
        PPOConfig()
        .environment("multi_agent_cartpole")
        .framework(args.framework)
        .multi_agent(
            # The multiagent Policy map.
            policies={
                # The Policy we are actually learning.
                "learnable_policy": PolicySpec(
                    config=PPOConfig.overrides(framework_str=args.framework)
                ),
                # Random policy we are playing against.
                "random": PolicySpec(policy_class=RandomPolicy),
            },
            # Map to either random behavior or PR learning behavior based on
            # the agent's ID.
            policy_mapping_fn=lambda agent_id, **kwargs: ["learnable_policy", "random"][
                agent_id % 2
            ],
            # We wouldn't have to specify this here as the RandomPolicy does
            # not learn anyways (it has an empty `learn_on_batch` method), but
            # it's good practice to define this list here either way.
            policies_to_train=["learnable_policy"],
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    if args.enable_rl_module_api:
        config = config.rl_module(_enable_rl_module_api=True).rollouts(
            enable_connectors=True
        )

    stop = {
        "training_iteration": args.stop_iters,
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    results = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(stop=stop, verbose=1),
    ).fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
