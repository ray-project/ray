import argparse
import gym
import os

import numpy as np
import ray
from ray.ml import Checkpoint
from ray.ml.config import RunConfig
from ray.ml.predictors.integrations.rl.rl_predictor import RLPredictor
from ray.ml.train.integrations.rl.rl_trainer import RLTrainer
from ray.ml.result import Result
from ray.rllib.agents.marwil import BCTrainer
from ray.tune.tuner import Tuner


def generate_offline_data(path: str):
    print(f"Generating offline data for training at {path}")
    trainer = RLTrainer(
        algorithm="PPO",
        run_config=RunConfig(stop={"timesteps_total": 5000}),
        config={
            "env": "CartPole-v0",
            "output": "dataset",
            "output_config": {
                "format": "json",
                "path": path,
                "max_num_samples_per_file": 1,
            },
            "batch_mode": "complete_episodes",
        },
    )
    trainer.fit()


def train_rl_bc_offline(path: str, num_workers: int, use_gpu: bool = False) -> Result:
    print("Starting offline training")
    dataset = ray.data.read_json(
        path, parallelism=num_workers, ray_remote_args={"num_cpus": 1}
    )

    trainer = RLTrainer(
        run_config=RunConfig(stop={"training_iteration": 5}),
        scaling_config={
            "num_workers": num_workers,
            "use_gpu": use_gpu,
        },
        datasets={"train": dataset},
        algorithm=BCTrainer,
        config={
            "env": "CartPole-v0",
            "framework": "tf",
            "evaluation_num_workers": 1,
            "evaluation_interval": 1,
            "evaluation_config": {"input": "sampler"},
        },
    )

    # Todo (krfricke/xwjiang): Enable checkpoint config in RunConfig
    # result = trainer.fit()
    tuner = Tuner(
        trainer,
        _tuner_kwargs={"checkpoint_at_end": True},
    )
    result = tuner.fit()[0]
    return result


def train_rl_ppo_online(num_workers: int, use_gpu: bool = False) -> Result:
    print("Starting online training")
    trainer = RLTrainer(
        run_config=RunConfig(stop={"training_iteration": 5}),
        scaling_config={
            "num_workers": num_workers,
            "use_gpu": use_gpu,
        },
        algorithm="PPO",
        config={
            "env": "CartPole-v0",
            "framework": "tf",
        },
    )
    # Todo (krfricke/xwjiang): Enable checkpoint config in RunConfig
    # result = trainer.fit()
    tuner = Tuner(
        trainer,
        _tuner_kwargs={"checkpoint_at_end": True},
    )
    result = tuner.fit()[0]
    return result


def evaluate_using_checkpoint(checkpoint: Checkpoint, num_episodes) -> list:
    predictor = RLPredictor.from_checkpoint(checkpoint)

    env = gym.make("CartPole-v0")

    rewards = []
    for i in range(num_episodes):
        obs = env.reset()
        reward = 0.0
        done = False
        while not done:
            action = predictor.predict([obs])
            obs, r, done, _ = env.step(action[0])
            reward += r
        rewards.append(reward)

    return rewards


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", default=False, action="store_true")
    parser.add_argument(
        "--path", required=False, default="/tmp/out", help="Path to (offline) data"
    )
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=1,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    parser.add_argument(
        "--num-eval-episodes",
        "-E",
        type=int,
        default=4,
        help="Number of evaluation episodes.",
    )
    args, _ = parser.parse_known_args()

    ray.init(address=args.address)
    if args.offline:
        if not os.path.exists(args.path) or not os.listdir(args.path):
            generate_offline_data(args.path)
        result = train_rl_bc_offline(
            path=args.path, num_workers=args.num_workers, use_gpu=args.use_gpu
        )
    else:
        result = train_rl_ppo_online(num_workers=args.num_workers, use_gpu=args.use_gpu)
    eval_checkpoint = result.checkpoint

    rewards = evaluate_using_checkpoint(
        eval_checkpoint, num_episodes=args.num_eval_episodes
    )
    print(
        f"Average reward over {args.num_eval_episodes} episodes: " f"{np.mean(rewards)}"
    )
