from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Example of using two different training methods at once in multi-agent.

Here we create a number of CartPole agents, some of which are trained with
DQN, and some of which are trained with PPO. We periodically sync weights
between the two trainers (note that no such syncing is needed when using just
a single training method).

For a simpler example, see also: multiagent_cartpole.py
"""

import argparse
import gym

import ray
from ray.rllib.agents.dqn.dqn import DQNAgent
from ray.rllib.agents.dqn.dqn_policy_graph import DQNPolicyGraph
from ray.rllib.agents.ppo.ppo import PPOAgent
from ray.rllib.agents.ppo.ppo_policy_graph import PPOPolicyGraph
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

    # You can also have multiple policy graphs per trainer, but here we just
    # show one each for PPO and DQN.
    policy_graphs = {
        "ppo_policy": (PPOPolicyGraph, obs_space, act_space, {}),
        "dqn_policy": (DQNPolicyGraph, obs_space, act_space, {}),
    }

    def policy_mapping_fn(agent_id):
        if agent_id % 2 == 0:
            return "ppo_policy"
        else:
            return "dqn_policy"

    ppo_trainer = PPOAgent(
        env="multi_cartpole",
        config={
            "multiagent": {
                "policy_graphs": policy_graphs,
                "policy_mapping_fn": policy_mapping_fn,
                "policies_to_train": ["ppo_policy"],
            },
            # disable filters, otherwise we would need to synchronize those
            # as well to the DQN agent
            "observation_filter": "NoFilter",
        })

    dqn_trainer = DQNAgent(
        env="multi_cartpole",
        config={
            "multiagent": {
                "policy_graphs": policy_graphs,
                "policy_mapping_fn": policy_mapping_fn,
                "policies_to_train": ["dqn_policy"],
            },
            "gamma": 0.95,
            "n_step": 3,
        })

    # disable DQN exploration when used by the PPO trainer
    ppo_trainer.optimizer.foreach_evaluator(
        lambda ev: ev.for_policy(
            lambda pi: pi.set_epsilon(0.0), policy_id="dqn_policy"))

    # You should see both the printed X and Y approach 200 as this trains:
    # info:
    #   policy_reward_mean:
    #     dqn_policy: X
    #     ppo_policy: Y
    for i in range(args.num_iters):
        print("== Iteration", i, "==")

        # improve the DQN policy
        print("-- DQN --")
        print(pretty_print(dqn_trainer.train()))

        # improve the PPO policy
        print("-- PPO --")
        print(pretty_print(ppo_trainer.train()))

        # swap weights to synchronize
        dqn_trainer.set_weights(ppo_trainer.get_weights(["ppo_policy"]))
        ppo_trainer.set_weights(dqn_trainer.get_weights(["dqn_policy"]))
