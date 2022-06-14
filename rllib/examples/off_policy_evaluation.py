"""Example on using Counterfactual Off-Policy Estimation Methods
with an offline dataset. We train a DQN evaluation policy on the
dataset for `n_iters` training steps before evaluation.

To generate the offline dataset with 1024 episodes on e.g. CartPole-v0, run:
`rllib train --run=PG --env=CartPole-v0 --framework=torch \
    --config='{"num_workers": 8, "output": "/tmp/cartpole-out/"}' \
    --stop='{"episodes_total": 1024}' -v`

"""

import argparse
import os
import time
from pathlib import Path

import gym
import numpy as np

from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators import (
    DirectMethod,
    DoublyRobust,
    ImportanceSampling,
    WeightedImportanceSampling,
)

rllib_dir = Path(__file__).parent.parent
parser = argparse.ArgumentParser()
parser.add_argument(
    "--input-path",
    type=str,
    default=os.path.join(rllib_dir, "tests/data/cartpole/large.json"),
)
parser.add_argument("--env-name", type=str, default="CartPole-v0")
parser.add_argument("--gamma", type=float, default=0.99)
parser.add_argument("--train-iters", type=int, default=50)
parser.add_argument("--num-workers", type=int, default=2)
parser.add_argument("--evaluation-num-workers", type=int, default=5)
parser.add_argument("--train-test-split-val", type=float, default=5)
parser.add_argument("--evaluation-episodes", type=int, default=128)
parser.add_argument("--timeout-s", type=int, default=1800)

if __name__ == "__main__":
    start = time.time()
    args = parser.parse_args()
    if args.train_test_split_val >= 1.0:
        args.train_test_split_val = int(args.train_test_split_val)
    config = (
        DQNConfig()
        .rollouts(num_rollout_workers=args.num_workers)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        .training(gamma=args.gamma)
        .environment(env=args.env_name)
        .exploration(
            explore=True,
            exploration_config={
                "type": "SoftQ",
                "temperature": 1.0,
            },
        )
        .evaluation(
            evaluation_interval=None,
            evaluation_duration_unit="episodes",
            evaluation_duration=args.evaluation_episodes,
            evaluation_num_workers=args.evaluation_num_workers,
            evaluation_config={
                "input": "dataset",
                "input_config": {"format": "json", "path": args.input_path},
                "metrics_episode_collection_timeout_s": args.timeout_s,
            },
            off_policy_estimation_methods={
                "train_test_split_val": args.train_test_split_val,
                "is": {"type": ImportanceSampling},
                "wis": {"type": WeightedImportanceSampling},
                "dm_qreg": {"type": DirectMethod, "q_model_type": "qreg"},
                "dm_fqe": {"type": DirectMethod, "q_model_type": "fqe"},
                "dr_qreg": {"type": DoublyRobust, "q_model_type": "qreg"},
                "dr_fqe": {"type": DoublyRobust, "q_model_type": "fqe"},
            },
        )
        .framework("torch")
        .rollouts(batch_mode="complete_episodes")
    )

    trainer = config.build()

    for _ in range(args.train_iters):
        results = trainer.train()
    print(
        results["episode_reward_mean"],
        "reward",
        results["time_total_s"],
        "seconds",
        results["timesteps_total"],
        "timesteps",
        results["episodes_total"],
        "episodes",
    )
    results = trainer.evaluate()

    # Simulate Monte-Carlo rollouts
    mc_ret = []
    env = gym.make(args.env_name)
    for _ in range(args.evaluation_episodes):
        obs = env.reset()
        done = False
        rewards = []
        while not done:
            act = trainer.compute_single_action(obs)
            obs, reward, done, _ = env.step(act)
            rewards.append(reward)
        ret = 0
        for r in reversed(rewards):
            ret = r + args.gamma * ret
        mc_ret.append(ret)

    estimates = results["evaluation"]["off_policy_estimator"]
    print("Simulation", "mean:", np.mean(mc_ret), "std:", np.std(mc_ret))
    for k, v in estimates.items():
        print(k, "mean:", v["v_new_mean"], "std:", v["v_new_std"])
    print("Ran OPE in", time.time() - start)
