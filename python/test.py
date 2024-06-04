import ray 
from ray.train.torch import TorchTrainer
from ray.train import RunConfig, ScalingConfig
import time

from ray.runtime_env import RuntimeEnv
ray.init(runtime_env=RuntimeEnv(env_vars={"RAY_TRAIN_ENABLE_STATE_TRACKING": "1"}))

def train_func():
    print("Training Starts")
    time.sleep(100)

datasets = {
    "train": ray.data.range(100),
    "val": ray.data.range(100)
}

trainer = TorchTrainer(
    train_func,
    run_config=RunConfig(
        name="my_train_run",
        storage_path="/tmp/cluster_storage"
    ),
    scaling_config=ScalingConfig(
        num_workers=8,
        use_gpu=False
    ),
    datasets=datasets
)
trainer.fit()