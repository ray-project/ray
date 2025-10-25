from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union

from ray.train import Checkpoint, DataConfig
from ray.train.trainer import GenDataset
from ray.train.v2._internal.execution.local_mode.torch import LocalTorchController
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.util import PublicAPI

if TYPE_CHECKING:
    # NOTE: `ray.train.torch` module imports in this file will break
    # with a circular import error if the TorchTrainer class is captured
    # in the scope of a Ray task.
    from ray.train.torch.config import TorchConfig


@PublicAPI(stability="stable")
class TorchTrainer(DataParallelTrainer):
    """A Trainer for data parallel PyTorch training.

    At a high level, this Trainer does the following:

    1. Launches multiple workers as defined by the ``scaling_config``.
    2. Sets up a distributed PyTorch environment
       on these workers as defined by the ``torch_config``.
    3. Ingests the input ``datasets`` based on the ``dataset_config``.
    4. Runs the input ``train_loop_per_worker(train_loop_config)``
       on all workers.

    For more details, see:

    * :ref:`PyTorch Guide <train-pytorch>`
    * :ref:`PyTorch Lightning Guide <train-pytorch-lightning>`
    * :ref:`Hugging Face Transformers Guide <train-pytorch-transformers>`

    Example:

        .. testcode::

            import os
            import tempfile

            import torch
            from torch import nn
            from torch.nn.parallel import DistributedDataParallel

            import ray.train
            from ray.train.torch import TorchTrainer


            # If using GPUs, set this to True.
            use_gpu = False
            # Number of processes to run training on.
            num_workers = 2

            # Define your network structure.
            class NeuralNetwork(nn.Module):
                def __init__(self):
                    super(NeuralNetwork, self).__init__()
                    self.layer1 = nn.Linear(1, 32)
                    self.relu = nn.ReLU()
                    self.layer2 = nn.Linear(32, 1)

                def forward(self, input):
                    return self.layer2(self.relu(self.layer1(input)))

            # Training loop.
            def train_fn_per_worker(config):

                # Read configurations.
                lr = config["lr"]
                batch_size = config["batch_size"]
                num_epochs = config["num_epochs"]

                # Fetch training dataset.
                train_dataset_shard = ray.train.get_dataset_shard("train")

                # Instantiate and prepare model for training.
                model = NeuralNetwork()
                model = ray.train.torch.prepare_model(model)

                # Define loss and optimizer.
                loss_fn = nn.MSELoss()
                optimizer = torch.optim.SGD(model.parameters(), lr=lr)

                # Create data loader.
                dataloader = train_dataset_shard.iter_torch_batches(
                    batch_size=batch_size, dtypes=torch.float
                )

                # Train multiple epochs.
                for epoch in range(num_epochs):
                    # Train epoch.
                    for batch in dataloader:
                        output = model(batch["input"])
                        loss = loss_fn(output, batch["label"])
                        optimizer.zero_grad()
                        loss.backward()
                        optimizer.step()

                    # Create checkpoint.
                    base_model = (
                        model.module
                        if isinstance(model, DistributedDataParallel)
                        else model
                    )

                    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                        torch.save(
                            {"model_state_dict": base_model.state_dict()},
                            os.path.join(temp_checkpoint_dir, "model.pt"),
                        )
                        checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)
                        # Report metrics and checkpoint.
                        ray.train.report({"loss": loss.item()}, checkpoint=checkpoint)

            # Define datasets.
            train_dataset = ray.data.from_items(
                [{"input": [x], "label": [2 * x + 1]} for x in range(128)]
            )

            # Initialize the Trainer.
            trainer = TorchTrainer(
                train_fn_per_worker,
                train_loop_config={"num_epochs": 1, "lr": 0.01, "batch_size": 32},
                scaling_config=ray.train.ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
                datasets={"train": train_dataset},
            )

            # Train the model.
            result = trainer.fit()

            # Inspect the results.
            final_loss = result.metrics["loss"]

    Args:

        train_loop_per_worker: The training function to execute on each worker.
            This function can either take in zero arguments or a single ``Dict``
            argument which is set by defining ``train_loop_config``.
            Within this function you can use any of the
            :ref:`Ray Train Loop utilities <train-loop-api>`.
        train_loop_config: A configuration ``Dict`` to pass in as an argument to
            ``train_loop_per_worker``.
            This is typically used for specifying hyperparameters. Passing large
            datasets via `train_loop_config` is not recommended and may introduce
            large overhead and unknown issues with serialization and deserialization.
        torch_config: The configuration for setting up the PyTorch Distributed backend.
            If set to None, a default configuration will be used in which
            GPU training uses NCCL and CPU training uses Gloo.
        scaling_config: The configuration for how to scale data parallel training.
            ``num_workers`` determines how many Python processes are used for training,
            and ``use_gpu`` determines whether or not each process should use GPUs.
            See :class:`~ray.train.ScalingConfig` for more info.
        run_config: The configuration for the execution of the training run.
            See :class:`~ray.train.RunConfig` for more info.
        datasets: The Ray Datasets to ingest for training.
            Datasets are keyed by name (``{name: dataset}``).
            Each dataset can be accessed from within the ``train_loop_per_worker``
            by calling ``ray.train.get_dataset_shard(name)``.
            Sharding and additional configuration can be done by
            passing in a ``dataset_config``.
        dataset_config: The configuration for ingesting the input ``datasets``.
            By default, all the Ray Dataset are split equally across workers.
            See :class:`~ray.train.DataConfig` for more details.
        resume_from_checkpoint: [Deprecated]
        metadata: [Deprecated]
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        torch_config: Optional["TorchConfig"] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        dataset_config: Optional[DataConfig] = None,
        # TODO: [Deprecated]
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        from ray.train.torch.config import TorchConfig

        torch_config = torch_config or TorchConfig()
        if not torch_config.backend:
            is_gpu_training = scaling_config and scaling_config.use_gpu
            torch_config.backend = "nccl" if is_gpu_training else "gloo"

        super(TorchTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=torch_config,
            scaling_config=scaling_config,
            run_config=run_config,
            dataset_config=dataset_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )

    def _get_local_controller(self) -> LocalTorchController:
        return LocalTorchController(
            experiment_name=self.run_config.name,
            datasets=self.datasets,
        )
