import argparse
import gym
import numpy as np

import ray
from ray.ml.checkpoint import Checkpoint
from ray.ml.config import RunConfig
from ray.ml.predictors.integrations.rl.rl_serve_env import RLServeEnv
from ray.ml.predictors.integrations.rl.rl_predictor import RLPredictor
from ray.ml.tests.test_rl_predictor import _DummyPolicy
from ray.ml.train.integrations.rl.rl_trainer import RLTrainer
from ray.ml.result import Result
from ray.rllib import RolloutWorker
from ray.serve.model_wrappers import ModelWrapperDeployment
from ray import serve
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


def serve_rl_model(checkpoint: Checkpoint, name="RLModel") -> ModelWrapperDeployment:
    serve.start(detached=True)
    deployment = ModelWrapperDeployment.options(name=name)
    deployment.deploy(RLPredictor, checkpoint)
    return deployment


def run_external_env(
    endpoint_uri: str = "http://127.0.0.1:8000/RLModel", num_batches: int = 3
):
    sub_env = gym.make("CartPole-v0")

    ev = RolloutWorker(
        env_creator=lambda _: RLServeEnv(endpoint_uri=endpoint_uri, env=sub_env),
        policy_spec=_DummyPolicy,
        rollout_fragment_length=40,
        batch_mode="complete_episodes",
    )

    rewards = []
    for _ in range(num_batches):
        batch = ev.sample()
        reward = np.sum(batch["rewards"])
        rewards.append(reward)

    for i, r in enumerate(rewards):
        print(f"Batch {i} reward: {r:.2f}")


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
    parser.add_argument(
        "--checkpoint", type=str, default=None, help="Path to checkpoint to load"
    )
    args, _ = parser.parse_known_args()

    ray.init(address=args.address)

    if not args.checkpoint:
        result = train_rl_ppo_online(num_workers=args.num_workers, use_gpu=args.use_gpu)
        checkpoint = result.checkpoint
    else:
        checkpoint = Checkpoint.from_directory(args.checkpoint)

    serve_rl_model(checkpoint)
    run_external_env()
    serve.shutdown()
