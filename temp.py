from ray.air import session
from ray.air.config import RunConfig, ScalingConfig
from ray.train.data_parallel_trainer import TorchTrainer


def train_loop_per_worker():
    print("Node rank", session.get_node_rank())


trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    scaling_config=ScalingConfig(num_workers=1),
    run_config=RunConfig(verbose=0),
)
results = trainer.fit()
