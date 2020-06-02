"""Example of running a custom hand-coded policy alongside trainable policies.

This example has two policies:
    (1) a simple PG policy
    (2) a hand-coded policy that acts at random in the env (doesn't learn)

In the console output, you can see the PG policy does much better than random:
Result for PG_multi_cartpole_0:
  ...
  policy_reward_mean:
    pg_policy: 185.23
    random: 21.255
  ...
"""

import argparse
import gym

import ray
from ray import tune
from ray.tune.registry import register_env
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=20)
parser.add_argument("--stop-reward", type=float, default=150)
parser.add_argument("--stop-timesteps", type=int, default=100000)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    # Simple environment with 4 independent cartpole entities
    register_env("multi_agent_cartpole",
                 lambda _: MultiAgentCartPole({"num_agents": 4}))
    single_env = gym.make("CartPole-v0")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    stop = {
        "training_iteration": args.stop_iters,
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    results = tune.run(
        "PG",
        stop=stop,
        config={
            "env": "multi_agent_cartpole",
            "multiagent": {
                "policies": {
                    "pg_policy": (None, obs_space, act_space, {
                        "framework": "torch" if args.torch else "tf",
                    }),
                    "random": (RandomPolicy, obs_space, act_space, {}),
                },
                "policy_mapping_fn": (
                    lambda agent_id: ["pg_policy", "random"][agent_id % 2]),
            },
            "framework": "torch" if args.torch else "tf",
        },
    )

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
