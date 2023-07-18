"""Simple example of setting up a multi-agent policy mapping.

Control the number of agents and policies via --num-agents and --num-policies.

This works with hundreds of agents and policies, but note that initializing
many TF policies will take some time.

Also, TF evals might slow down with large numbers of policies. To debug TF
execution, set the TF_TIMELINE_DIR environment variable.
"""

import argparse
import os
import random

import ray
from ray import tune, air
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved


tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()

parser.add_argument("--num-agents", type=int, default=4)
parser.add_argument("--num-policies", type=int, default=2)
parser.add_argument(
    "--framework",
    choices=["tf2", "torch"],  # tf will be deprecated with the new Learner stack
    default="torch",
    help="The DL framework specifier.",
)

parser.add_argument(
    "--num-gpus",
    type=int,
    default=int(os.environ.get("RLLIB_NUM_GPUS", "0")),
    help="Number of GPUs to use for training.",
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
    "--stop-timesteps", type=int, default=50000, help="Number of timesteps to train."
)

parser.add_argument(
    "--stop-reward-per-agent",
    type=float,
    default=150.0,
    help="Min. reward per agent at which we stop training.",
)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init()

    # Each policy can have a different configuration (including custom model).
    def gen_policy(i):
        gammas = [0.95, 0.99]
        # just change the gammas between the two policies.
        # changing the module is not a critical part of this example.
        # the important part is that the policies are different.
        config = {
            "gamma": gammas[i % len(gammas)],
        }

        return PolicySpec(config=config)

    # Setup PPO with an ensemble of `num_policies` different policies.
    policies = {"policy_{}".format(i): gen_policy(i) for i in range(args.num_policies)}
    policy_ids = list(policies.keys())

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        pol_id = random.choice(policy_ids)
        return pol_id

    scaling_config = {
        "num_learner_workers": args.num_gpus,
        "num_gpus_per_learner_worker": int(args.num_gpus > 0),
    }

    config = (
        PPOConfig()
        .rollouts(rollout_fragment_length="auto", num_rollout_workers=3)
        .environment(MultiAgentCartPole, env_config={"num_agents": args.num_agents})
        .framework(args.framework)
        .training(num_sgd_iter=10, sgd_minibatch_size=2**9, train_batch_size=2**12)
        .multi_agent(policies=policies, policy_mapping_fn=policy_mapping_fn)
        .rl_module(_enable_rl_module_api=True)
        .training(_enable_learner_api=True)
        .resources(**scaling_config)
    )

    stop_reward = args.stop_reward_per_agent * args.num_agents
    stop = {
        "episode_reward_mean": stop_reward,
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    results = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(stop=stop, verbose=3),
    ).fit()

    if args.as_test:
        check_learning_achieved(results, stop_reward)
    ray.shutdown()
