import argparse

import ray
from ray.ml.checkpoint import Checkpoint
from ray.ml.config import RunConfig
from ray.ml.train.integrations.rl.rl_trainer import RLTrainer
from ray.ml.result import Result
from ray.tune.tuner import Tuner


def train_rl_ppo_online(num_workers: int, use_gpu: bool = False) -> Result:
    trainer = RLTrainer(
        run_config=RunConfig(stop={"training_iteration": 15}),
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
    tuner = Tuner(
        trainer,
        run_config=RunConfig(stop={"training_iteration": 15}),
        _tuner_kwargs={"checkpoint_at_end": True, "checkpoint_freq": 1},
    )
    return tuner.fit()[0]


def serve_rl_model(checkpoint: Checkpoint):
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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
    args, _ = parser.parse_known_args()

    ray.init(address=args.address)

    if True:
        result = train_rl_ppo_online(num_workers=args.num_workers, use_gpu=args.use_gpu)
        checkpoint = result.checkpoint
        print(result.checkpoint)
        print(result.checkpoint._local_path)
    else:
        checkpoint = Checkpoint.from_directory(
            "/Users/kai/ray_results/AIRPPOTrainer_2022-04-21_16-02-18/AIRPPOTrainer_09b06_00000_0_2022-04-21_16-02-19"
        )

    serve_rl_model(checkpoint)
