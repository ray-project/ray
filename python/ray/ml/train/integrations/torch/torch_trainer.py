from typing import Callable, Optional, Dict, Union

from ray.train.torch import TorchConfig
from ray.ml.trainer import GenDataset
from ray.ml.train.data_parallel_trainer import DataParallelTrainer
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.util import PublicAPI


@PublicAPI(stability="alpha")
class TorchTrainer(DataParallelTrainer):
    """A Trainer for data parallel PyTorch training.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors. These actors already have the necessary torch process group already
    configured for distributed PyTorch training.

    The ``train_loop_per_worker`` function is expected to take in either 0 or 1
    arguments:

    .. code-block:: python

        def train_loop_per_worker():
            ...

    .. code-block:: python

        def train_loop_per_worker(config: Dict):
            ...

    If ``train_loop_per_worker`` accepts an argument, then
    ``train_loop_config`` will be passed in as the argument. This is useful if you
    want to tune the values in ``train_loop_config`` as hyperparameters.

    If the ``datasets`` dict contains a training dataset (denoted by
    the "train" key), then it will be split into multiple dataset
    shards that can then be accessed by ``ray.train.get_dataset_shard("train")`` inside
    ``train_loop_per_worker``. All the other datasets will not be split and
    ``ray.train.get_dataset_shard(...)`` will return the the entire Dataset.

    Inside the ``train_loop_per_worker`` function, you can use any of the
    :ref:`Ray Train function utils <train-api-func-utils>`.

    .. code-block:: python

        def train_loop_per_worker():
            # Report intermediate results for callbacks or logging.
            train.report(...)

            # Checkpoints the provided args as restorable state.
            train.save_checkpoint(...)

            # Returns dict of last saved checkpoint.
            train.load_checkpoint()

            # Returns the Ray Dataset shard for the given key.
            train.get_dataset_shard("my_dataset")

            # Returns the total number of workers executing training.
            train.get_world_size()

            # Returns the rank of this worker.
            train.get_world_rank()

            # Returns the rank of the worker on the current node.
            train.get_local_rank()

    You can also use any of the :ref:`Torch specific function utils
    <train-api-torch-utils>`.

    .. code-block:: python

        def train_loop_per_worker():
            # Prepares model for distribted training by wrapping in
            # `DistributedDataParallel` and moving to correct device.
            train.torch.prepare_model(...)

            # Configures the dataloader for distributed training by adding a
            # `DistributedSampler`.
            # You should NOT use this if you are doing
            # `train.get_dataset_shard(...).to_torch(...)`
            train.torch.prepare_data_loader(...)

            # Returns the current torch device.
            train.torch.get_device()

    To save a model to use for the ``TorchPredictor``, you must save it under the
    "model" kwarg in ``train.save_checkpoint()``.

    Example:
        .. code-block:: python

            import torch
            import torch.nn as nn

            import ray
            from ray import train
            from ray.ml.train.integrations.torch import TorchTrainer

            input_size = 1
            layer_size = 15
            output_size = 1
            num_epochs = 3

            class NeuralNetwork(nn.Module):
                def __init__(self):
                    super(NeuralNetwork, self).__init__()
                    self.layer1 = nn.Linear(input_size, layer_size)
                    self.relu = nn.ReLU()
                    self.layer2 = nn.Linear(layer_size, output_size)

                def forward(self, input):
                    return self.layer2(self.relu(self.layer1(input)))

            def train_loop_per_worker():
                dataset_shard = train.get_dataset_shard("train")
                model = NeuralNetwork()
                loss_fn = nn.MSELoss()
                optimizer = optim.SGD(model.parameters(), lr=0.1)

                model = train.torch.prepare_model(model)

                for epoch in range(num_epochs):
                    for batch in iter(dataset_shard.to_torch(batch_size=32)):
                        output = model(input)
                        loss = loss_fn(output, labels)
                        optimizer.zero_grad()
                        loss.backward()
                        optimizer.step()
                        print(f"epoch: {epoch}, loss: {loss.item()}")

                    train.save_checkpoint(model=model.state_dict())

            train_dataset = ray.data.from_items([1, 2, 3])
            scaling_config = {"num_workers": 3}
            # If using GPUs, use the below scaling config instead.
            # scaling_config = {"num_workers": 3, "use_gpu": True}
            trainer = TorchTrainer(
                train_loop_per_worker=train_loop_per_worker,
                scaling_config=scaling_config,
                datasets={"train": train_dataset})
            result = trainer.fit()

    Args:
        train_loop_per_worker: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_loop_config: Configurations to pass into
            ``train_loop_per_worker`` if it accepts an argument.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ``ray.ml.preprocessor.Preprocessor`` to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        *,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        train_loop_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not torch_config:
            torch_config = TorchConfig()

        super(TorchTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=torch_config,
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )
