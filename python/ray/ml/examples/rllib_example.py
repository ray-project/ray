import argparse

import ray
from ray.ml.config import RunConfig
from ray.ml.train.integrations.rllib.rl_trainer import RLTrainer
from ray.ml.result import Result
from ray.rllib.agents.marwil import BCTrainer


def train_rllib_bc_offline(
    path: str, num_workers: int, use_gpu: bool = False
) -> Result:
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
        param_space={
            "env": "CartPole-v0",
            "framework": "tf",
            "evaluation_num_workers": 1,
            "evaluation_interval": 1,
            "evaluation_config": {"input": "sampler"},
        },
    )
    result = trainer.fit()
    print(result.metrics)

    return result


def train_rllib_bc_online(num_workers: int, use_gpu: bool = False) -> Result:
    trainer = RLTrainer(
        run_config=RunConfig(stop={"training_iteration": 5}),
        scaling_config={
            "num_workers": num_workers,
            "use_gpu": use_gpu,
        },
        algorithm="PPO",
        param_space={
            "env": "CartPole-v0",
            "framework": "tf",
            "evaluation_num_workers": 1,
            "evaluation_interval": 1,
            "evaluation_config": {"input": "sampler"},
        },
    )
    result = trainer.fit()
    print(result.metrics)

    return result


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
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    args, _ = parser.parse_known_args()

    ray.init(address=args.address)
    if args.offline:
        train_rllib_bc_offline(
            path=args.path, num_workers=args.num_workers, use_gpu=args.use_gpu
        )
    else:
        train_rllib_bc_online(num_workers=args.num_workers, use_gpu=args.use_gpu)
