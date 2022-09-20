import argparse
from concurrent.futures import process

import numpy as np
import torch
import torch.nn as nn

import ray.train as train
from ray.air import session

from ray.air.config import ScalingConfig

from torch.nn.parallel import DistributedDataParallel as DDP

from ray.train._internal.utils import construct_train_func
from ray.train._internal.backend_executor import BackendExecutor
from ray.train.torch import TorchConfig
from ray.train import TrainingIterator
from ray.train._internal.checkpoint import CheckpointManager
from ray.train._internal.dataset_spec import DataParallelIngestSpec
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train._internal.dataset_spec import RayDatasetSpec
from ray.util.queue import Queue



class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1000):
        x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def train_func(config):
    data_size = config.get("data_size", 1000)
    batch_size = config.get("batch_size", 32)
    hidden_size = config.get("hidden_size", 1)
    lr = config.get("lr", 1e-2)
    epochs = config.get("epochs", 3)

    train_dataset = LinearDataset(2, 5, size=data_size)
    train_loader = torch.utils.data.DataLoader(train_dataset, batch_size=batch_size)
    train_loader = train.torch.prepare_data_loader(train_loader)
    device = train.torch.get_device()

    pg1 = torch.distributed.new_group(list(range(session.get_world_size())))
    pg2 = torch.distributed.new_group(list(range(session.get_world_size())))

    model = nn.Linear(1, hidden_size)
    model.to(device)
    model = DDP(model, device_ids=[session.get_local_rank()], output_device=[session.get_local_rank()], process_group=pg1)

    model2 = nn.Linear(1, hidden_size)
    model2.to(device)
    model2 = DDP(model2, device_ids=[session.get_local_rank()], output_device=[session.get_local_rank()], process_group=pg2)

    loss_fn = nn.MSELoss()
    loss_fn2 = nn.MSELoss()

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    optimizer2 = torch.optim.SGD(model2.parameters(), lr=lr)

    results = []
    for idx in range(epochs):

        for X, y in train_loader:
            # Compute prediction error
            pred = model(X)
            pred2 = model2(X)
            loss = loss_fn(pred, y)
            loss2 = loss_fn2(pred2, y)

            # Backpropagation
            optimizer.zero_grad()
            loss.backward()
            model1_grad = model_grad_norm(model)
            optimizer.step()

            optimizer2.zero_grad()
            loss2.backward()
            model2_grad = model_grad_norm(model2)
            optimizer2.step()

        print(f"epoch: {idx}, rank: {session.get_local_rank()}, model1_grad: {model1_grad}, model2_grad: {model2_grad}")


        result = {"loss" : loss.item(), "loss2" : loss2.item()}

        results.append(result)
        session.report(result)

    # return required for backwards compatibility with the old API
    # TODO(team-ml) clean up and remove return
    return results


def train_linear(num_workers=2, use_gpu=True, epochs=3):
    queue = Queue(maxsize=100)
    scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)
    backend_config = TorchConfig()
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs, "queue": queue}
    train_loop_per_worker = construct_train_func(
        train_func,
        config,
        fn_arg_name="train_loop_per_worker",
        discard_returns=False,
    )

    backend_executor = BackendExecutor(
            backend_config=backend_config,
            num_workers=scaling_config.num_workers,
            num_cpus_per_worker=scaling_config.num_cpus_per_worker,
            num_gpus_per_worker=scaling_config.num_gpus_per_worker,
            max_retries=0,
        )

    checkpoint_manager = CheckpointManager(
                checkpoint_strategy=None, run_dir=None
            )

    # Start the remote actors.
    backend_executor.start(initialization_hook=None)

    empty_spec = RayDatasetSpec(dataset_or_dict=None)

    training_iterator = TrainingIterator(
        backend_executor=backend_executor,
        backend_config=backend_config,
        train_func=train_loop_per_worker,
        dataset_spec=empty_spec,
        checkpoint_manager=checkpoint_manager,
        checkpoint=None,
        checkpoint_strategy=None,
    )

    for results in training_iterator:
        first_worker_results = results


def model_grad_norm(model):
    total_norm = 0
    for p in model.parameters():
        param_norm = p.grad.detach().data.norm(2)
        total_norm += param_norm.item() ** 2
    total_norm = total_norm ** 0.5
    return total_norm

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
        "--use-gpu", action="store_true", help="Whether to use GPU for training."
    )
    parser.add_argument(
        "--epochs", type=int, default=3, help="Number of epochs to train for."
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )

    args, _ = parser.parse_known_args()

    import ray

    if args.smoke_test:
        ray.init(num_cpus=4)
        train_linear()
    else:
        ray.init(address=args.address)
        train_linear(
            num_workers=args.num_workers, use_gpu=args.use_gpu, epochs=args.epochs
        )