"""Simple example of how to restore only one of n agents from a trained
multi-agent Algorithm using Ray tune.

Control the number of agents and policies via --num-agents and --num-policies.
"""

import argparse
import gym
import os
import random

import ray
from ray import air
from ray import tune
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()

parser.add_argument("--num-agents", type=int, default=4)
parser.add_argument("--num-policies", type=int, default=2)
parser.add_argument("--pre-training-iters", type=int, default=5)
parser.add_argument("--num-cpus", type=int, default=0)
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
    "--stop-iters", type=int, default=200, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    # Get obs- and action Spaces.
    single_env = gym.make("CartPole-v1")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    # Setup PPO with an ensemble of `num_policies` different policies.
    policies = {
        f"policy_{i}": (None, obs_space, act_space, {})
        for i in range(args.num_policies)
    }
    policy_ids = list(policies.keys())

    def policy_mapping_fn(agent_id, episode, worker, **kwargs):
        pol_id = random.choice(policy_ids)
        return pol_id

    config = (
        PPOConfig()
        .environment(MultiAgentCartPole, env_config={"num_agents": args.num_agents})
        .framework(args.framework)
        .training(num_sgd_iter=10)
        .multi_agent(policies=policies, policy_mapping_fn=policy_mapping_fn)
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    # Do some training and store the checkpoint.
    results = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={"training_iteration": args.pre_training_iters},
            verbose=1,
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=1, checkpoint_at_end=True
            ),
        ),
    ).fit()
    print("Pre-training done.")

    best_checkpoint = results.get_best_result().checkpoint
    print(f".. best checkpoint was: {best_checkpoint}")

    policy_0_checkpoint = os.path.join(
        best_checkpoint.to_directory(), "policies/policy_0"
    )
    restored_policy_0 = Policy.from_checkpoint(policy_0_checkpoint)

    print("Starting new tune.Tuner().fit()")

    # Start our actual experiment.
    stop = {
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    class RestoreWeightsCallback(DefaultCallbacks):
        def on_algorithm_init(self, *, algorithm: "Algorithm", **kwargs) -> None:
            algorithm.set_weights({"policy_0": restored_policy_0.get_weights()})

    # Make sure, the non-1st policies are not updated anymore.
    config.policies_to_train = [pid for pid in policy_ids if pid != "policy_0"]
    config.callbacks = RestoreWeightsCallback

    results = tune.run(
        "PPO",
        stop=stop,
        config=config.to_dict(),
        verbose=1,
    )

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
