"""Example of using two different training methods at once in multi-agent.

Here we create a number of CartPole agents, some of which are trained with
DQN, and some of which are trained with PPO. We periodically sync weights
between the two algorithms (note that no such syncing is needed when using just
a single training method).

For a simpler example, see also: multiagent_cartpole.py
"""
# TODO (Kourosh): Migrate this example to the RLModule API.
import argparse

import gymnasium as gym
import os

import ray
from ray.rllib.algorithms.dqn import DQNConfig, DQNTFPolicy, DQNTorchPolicy
from ray.rllib.algorithms.ppo import (
    PPOConfig,
    PPOTF1Policy,
    PPOTF2Policy,
    PPOTorchPolicy,
)
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env

parser = argparse.ArgumentParser()
# Use torch for both policies.
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
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
    "--stop-reward", type=float, default=50.0, help="Reward at which we stop training."
)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init()

    # Simple environment with 4 independent cartpole entities
    register_env(
        "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": 4})
    )
    single_dummy_env = gym.make("CartPole-v1")
    obs_space = single_dummy_env.observation_space
    act_space = single_dummy_env.action_space

    def select_policy(algorithm, framework):
        if algorithm == "PPO":
            if framework == "torch":
                return PPOTorchPolicy
            elif framework == "tf":
                return PPOTF1Policy
            else:
                return PPOTF2Policy
        elif algorithm == "DQN":
            if framework == "torch":
                return DQNTorchPolicy
            else:
                return DQNTFPolicy
        else:
            raise ValueError("Unknown algorithm: ", algorithm)

    # Construct two independent Algorithm configs
    ppo_config = (
        PPOConfig()
        .environment("multi_agent_cartpole")
        .framework(args.framework)
        # disable filters, otherwise we would need to synchronize those
        # as well to the DQN agent
        .rollouts(observation_filter="MeanStdFilter")
        .training(
            model={"vf_share_layers": True},
            vf_loss_coeff=0.01,
            num_sgd_iter=6,
            _enable_learner_api=False,
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        .rl_module(_enable_rl_module_api=False)
    )

    dqn_config = (
        DQNConfig()
        .environment("multi_agent_cartpole")
        .framework(args.framework)
        # disable filters, otherwise we would need to synchronize those
        # as well to the DQN agent
        .rollouts(observation_filter="MeanStdFilter")
        .training(
            model={"vf_share_layers": True},
            n_step=3,
            gamma=0.95,
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    # Specify two policies, each with their own config created above
    # You can also have multiple policies per algorithm, but here we just
    # show one each for PPO and DQN.
    policies = {
        "ppo_policy": (
            select_policy("PPO", args.framework),
            obs_space,
            act_space,
            ppo_config,
        ),
        "dqn_policy": (
            select_policy("DQN", args.framework),
            obs_space,
            act_space,
            dqn_config,
        ),
    }

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        if agent_id % 2 == 0:
            return "ppo_policy"
        else:
            return "dqn_policy"

    # Add multi-agent configuration options to both configs and build them.
    ppo_config.multi_agent(
        policies=policies,
        policy_mapping_fn=policy_mapping_fn,
        policies_to_train=["ppo_policy"],
    )
    ppo = ppo_config.build()

    dqn_config.multi_agent(
        policies=policies,
        policy_mapping_fn=policy_mapping_fn,
        policies_to_train=["dqn_policy"],
    )
    dqn = dqn_config.build()

    # You should see both the printed X and Y approach 200 as this trains:
    # info:
    #   policy_reward_mean:
    #     dqn_policy: X
    #     ppo_policy: Y
    for i in range(args.stop_iters):
        print("== Iteration", i, "==")

        # improve the DQN policy
        print("-- DQN --")
        result_dqn = dqn.train()
        print(pretty_print(result_dqn))

        # improve the PPO policy
        print("-- PPO --")
        result_ppo = ppo.train()
        print(pretty_print(result_ppo))

        # Test passed gracefully.
        if (
            args.as_test
            and result_dqn["episode_reward_mean"] > args.stop_reward
            and result_ppo["episode_reward_mean"] > args.stop_reward
        ):
            print("test passed (both agents above requested reward)")
            quit(0)

        # swap weights to synchronize
        dqn.set_weights(ppo.get_weights(["ppo_policy"]))
        ppo.set_weights(dqn.get_weights(["dqn_policy"]))

    # Desired reward not reached.
    if args.as_test:
        raise ValueError("Desired reward ({}) not reached!".format(args.stop_reward))
