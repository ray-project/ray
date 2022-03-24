import argparse

import ray
from ray.ml.train.integrations.rllib.rl_trainer import RLTrainer
from ray.ml.result import Result
from ray.rllib.agents.marwil import BCTrainer


def train_rllib_bc(num_workers: int, use_gpu: bool = False) -> Result:
    path = "/tmp/out"

    # Curently doesn't seem to work, to unblock dev pass `input_config`
    dataset = ray.data.read_json(
        path, parallelism=num_workers, ray_remote_args={"num_cpus": 1}
    )

    trainer = RLTrainer(
        scaling_config={
            "num_workers": num_workers,
            "use_gpu": use_gpu,
        },
        # datasets={"train": dataset},
        algorithm=BCTrainer,
        param_space={
            "env": "CartPole-v0",
            "framework": "tf",
            "evaluation_num_workers": 1,
            "evaluation_interval": 1,
            "evaluation_config": {"input": "sampler"},
            "input": "dataset",
            "input_config": {"format": "native", "path": lambda: dataset},
        },
    )
    result = trainer.fit()
    print(result.metrics)

    return result


def train_old_style():
    from ray import tune

    path = "/tmp/out"
    # parallelism = 2
    # dataset = ray.data.read_json(
    #     path, parallelism=parallelism, ray_remote_args={"num_cpus": 1}
    # ).fully_executed()

    analysis = tune.run(
        "BC",
        stop={"timesteps_total": 500000},
        config={
            "env": "CartPole-v0",
            "framework": "tf",
            "evaluation_num_workers": 1,
            "evaluation_interval": 1,
            "evaluation_config": {"input": "sampler"},
            "input": "dataset",
            "input_config": {"format": "json", "path": path},
        },
    )
    print(analysis.trials[0].checkpoint)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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

    ray.init(address="auto")  # args.address)
    result = train_rllib_bc(num_workers=args.num_workers, use_gpu=args.use_gpu)
