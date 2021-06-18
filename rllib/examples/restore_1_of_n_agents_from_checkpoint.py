"""Simple example of how to restore only one of n agents from a trained
multi-agent Trainer using Ray tune.

The trick/workaround is to use an intermediate trainer that loads the
trained checkpoint into all policies and then reverts those policies
that we don't want to restore, then saves a new checkpoint, from which
tune can pick up training.

Control the number of agents and policies via --num-agents and --num-policies.
"""

import argparse
import gym
import os
import random

import ray
from ray import tune
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
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
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.")
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.")
parser.add_argument(
    "--stop-iters",
    type=int,
    default=200,
    help="Number of iterations to train.")
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=100000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--stop-reward",
    type=float,
    default=150.0,
    help="Reward at which we stop training.")

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    # Get obs- and action Spaces.
    single_env = gym.make("CartPole-v0")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    # Setup PPO with an ensemble of `num_policies` different policies.
    policies = {
        f"policy_{i}": (None, obs_space, act_space, {})
        for i in range(args.num_policies)
    }
    policy_ids = list(policies.keys())

    def policy_mapping_fn(agent_id):
        pol_id = random.choice(policy_ids)
        return pol_id

    config = {
        "env": MultiAgentCartPole,
        "env_config": {
            "num_agents": args.num_agents,
        },
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_sgd_iter": 10,
        "multiagent": {
            "policies": policies,
            "policy_mapping_fn": policy_mapping_fn,
        },
        "framework": args.framework,
    }

    # Do some training and store the checkpoint.
    results = tune.run(
        "PPO",
        config=config,
        stop={"training_iteration": args.pre_training_iters},
        verbose=1,
        checkpoint_freq=1,
        checkpoint_at_end=True,
    )
    print("Pre-training done.")

    best_checkpoint = results.get_best_checkpoint(
        results.trials[0], mode="max")
    print(f".. best checkpoint was: {best_checkpoint}")

    # Create a new dummy Trainer to "fix" our checkpoint.
    new_trainer = PPOTrainer(config=config)
    # Get untrained weights for all policies.
    untrained_weights = new_trainer.get_weights()
    # Restore all policies from checkpoint.
    new_trainer.restore(best_checkpoint)
    # Set back all weights (except for 1st agent) to original
    # untrained weights.
    new_trainer.set_weights(
        {pid: w
         for pid, w in untrained_weights.items() if pid != "policy_0"})
    # Create the checkpoint from which tune can pick up the
    # experiment.
    new_checkpoint = new_trainer.save()
    new_trainer.stop()
    print(".. checkpoint to restore from (all policies reset, "
          f"except policy_0): {new_checkpoint}")

    print("Starting new tune.run")

    # Start our actual experiment.
    stop = {
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    # Make sure, the non-1st policies are not updated anymore.
    config["multiagent"]["policies_to_train"] = [
        pid for pid in policy_ids if pid != "policy_0"
    ]

    results = tune.run(
        "PPO",
        stop=stop,
        config=config,
        verbose=1,
        restore=new_checkpoint,
    )

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
