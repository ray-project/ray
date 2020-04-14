"""Example of A3C / PPO alternating training.

Also an integration test for https://github.com/ray-project/ray/issues/7387
"""

import argparse
import gym

import ray
from ray.rllib.agents.a3c.a3c import A3CTrainer
from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy
from ray.rllib.agents.ppo.ppo import PPOTrainer
from ray.rllib.agents.ppo.ppo_tf_policy import PPOTFPolicy
from ray.rllib.tests.test_multi_agent_env import MultiCartpole
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env

parser = argparse.ArgumentParser()
parser.add_argument("--num-iters", type=int, default=20)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    # Simple environment with 4 independent cartpole entities
    register_env("multi_cartpole", lambda _: MultiCartpole(4))
    single_env = gym.make("CartPole-v0")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    policies = {
        "ppo_policy": (PPOTFPolicy, obs_space, act_space, {}),
        "a3c_policy": (A3CTFPolicy, obs_space, act_space, {}),
    }

    def policy_mapping_fn(agent_id):
        if agent_id % 2 == 0:
            return "ppo_policy"
        else:
            return "a3c_policy"

    ppo_trainer = PPOTrainer(
        env="multi_cartpole",
        config={
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": policy_mapping_fn,
                "policies_to_train": ["ppo_policy"],
            },
            "observation_filter": "NoFilter",
        })

    a3c_trainer = A3CTrainer(
        env="multi_cartpole",
        config={
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": policy_mapping_fn,
                "policies_to_train": ["a3c_policy"],
            },
            "gamma": 0.95,
        })

    for i in range(args.num_iters):
        print("== Iteration", i, "==")

        print("-- A3C --")
        print(pretty_print(a3c_trainer.train()))

        print("-- PPO --")
        print(pretty_print(ppo_trainer.train()))

        # swap weights to synchronize
        a3c_trainer.set_weights(ppo_trainer.get_weights(["ppo_policy"]))
        ppo_trainer.set_weights(a3c_trainer.get_weights(["a3c_policy"]))
