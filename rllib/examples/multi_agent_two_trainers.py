"""Example of using two different training methods at once in multi-agent.

Here we create a number of CartPole agents, some of which are trained with
DQN, and some of which are trained with PPO. We periodically sync weights
between the two trainers (note that no such syncing is needed when using just
a single training method).

For a simpler example, see also: multiagent_cartpole.py
"""

import argparse
import gym
import os

import ray
from ray.rllib.agents.dqn import DQNTrainer, DQNTFPolicy, DQNTorchPolicy
from ray.rllib.agents.ppo import PPOTrainer, PPOTFPolicy, PPOTorchPolicy
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env

parser = argparse.ArgumentParser()
# Use torch for both policies.
parser.add_argument("--torch", action="store_true")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=20)
parser.add_argument("--stop-reward", type=float, default=50)
parser.add_argument("--stop-timesteps", type=int, default=100000)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init()

    # Simple environment with 4 independent cartpole entities
    register_env("multi_agent_cartpole",
                 lambda _: MultiAgentCartPole({"num_agents": 4}))
    single_dummy_env = gym.make("CartPole-v0")
    obs_space = single_dummy_env.observation_space
    act_space = single_dummy_env.action_space

    # You can also have multiple policies per trainer, but here we just
    # show one each for PPO and DQN.
    policies = {
        "ppo_policy": (PPOTorchPolicy if args.torch else PPOTFPolicy,
                       obs_space, act_space, {}),
        "dqn_policy": (DQNTorchPolicy if args.torch else DQNTFPolicy,
                       obs_space, act_space, {}),
    }

    def policy_mapping_fn(agent_id):
        if agent_id % 2 == 0:
            return "ppo_policy"
        else:
            return "dqn_policy"

    ppo_trainer = PPOTrainer(
        env="multi_agent_cartpole",
        config={
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": policy_mapping_fn,
                "policies_to_train": ["ppo_policy"],
            },
            "model": {
                "vf_share_layers": True,
            },
            "num_sgd_iter": 6,
            "vf_loss_coeff": 0.01,
            # disable filters, otherwise we would need to synchronize those
            # as well to the DQN agent
            "observation_filter": "MeanStdFilter",
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            "framework": "torch" if args.torch else "tf",
        })

    dqn_trainer = DQNTrainer(
        env="multi_agent_cartpole",
        config={
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": policy_mapping_fn,
                "policies_to_train": ["dqn_policy"],
            },
            "model": {
                "vf_share_layers": True,
            },
            "gamma": 0.95,
            "n_step": 3,
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            "framework": "torch" if args.torch else "tf"
        })

    # You should see both the printed X and Y approach 200 as this trains:
    # info:
    #   policy_reward_mean:
    #     dqn_policy: X
    #     ppo_policy: Y
    for i in range(args.stop_iters):
        print("== Iteration", i, "==")

        # improve the DQN policy
        print("-- DQN --")
        result_dqn = dqn_trainer.train()
        print(pretty_print(result_dqn))

        # improve the PPO policy
        print("-- PPO --")
        result_ppo = ppo_trainer.train()
        print(pretty_print(result_ppo))

        # Test passed gracefully.
        if args.as_test and \
                result_dqn["episode_reward_mean"] > args.stop_reward and \
                result_ppo["episode_reward_mean"] > args.stop_reward:
            print("test passed (both agents above requested reward)")
            quit(0)

        # swap weights to synchronize
        dqn_trainer.set_weights(ppo_trainer.get_weights(["ppo_policy"]))
        ppo_trainer.set_weights(dqn_trainer.get_weights(["dqn_policy"]))

    # Desired reward not reached.
    if args.as_test:
        raise ValueError("Desired reward ({}) not reached!".format(
            args.stop_reward))
