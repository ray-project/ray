from typing import Dict, Callable, Optional, Union, TYPE_CHECKING

from ray.air.config import ScalingConfig, RunConfig, DatasetConfig
from ray.train.trainer import GenDataset
from ray.air.checkpoint import Checkpoint


from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.horovod.config import HorovodConfig
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
class HorovodTrainer(DataParallelTrainer):
    """A Trainer for data parallel Horovod training.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors. These actors already have the necessary Horovod setup already
    configured for distributed Horovod training.

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
    shards that can then be accessed by ``session.get_dataset_shard("train")`` inside
    ``train_loop_per_worker``. All the other datasets will not be split and
    ``session.get_dataset_shard(...)`` will return the the entire Dataset.

    Inside the ``train_loop_per_worker`` function, you can use any of the
    :ref:`Ray AIR session methods <air-session-ref>`.

    .. code-block:: python

        def train_loop_per_worker():
            # Report intermediate results for callbacks or logging and
            # checkpoint data.
            session.report(...)

            # Returns dict of last saved checkpoint.
            session.get_checkpoint()

            # Returns the Ray Dataset shard for the given key.
            session.get_dataset_shard("my_dataset")

            # Returns the total number of workers executing training.
            session.get_world_size()

            # Returns the rank of this worker.
            session.get_world_rank()

            # Returns the rank of the worker on the current node.
            session.get_local_rank()

    Any returns from the ``train_loop_per_worker`` will be discarded and not
    used or persisted anywhere.

    You could use ``TensorflowPredictor`` or ``TorchPredictor`` in conjunction with
    HorovodTrainer. You must save the model under the "model" kwarg in the
    ``Checkpoint`` passed to ``session.report()``, so that it can be used by
    corresponding predictors.

    Example:

    .. code-block:: python

        import ray
        import ray.train as train
        import ray.train.torch. # Need this to use `train.torch.get_device()`
        import horovod.torch as hvd
        import torch
        import torch.nn as nn
        from ray.air import session, Checkpoint
        from ray.train.horovod import HorovodTrainer
        from ray.air.config import ScalingConfig

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
            hvd.init()
            dataset_shard = session.get_dataset_shard("train")
            model = NeuralNetwork()
            device = train.torch.get_device()
            model.to(device)
            loss_fn = nn.MSELoss()
            lr_scaler = 1
            optimizer = torch.optim.SGD(model.parameters(), lr=0.1 * lr_scaler)
            # Horovod: wrap optimizer with DistributedOptimizer.
            optimizer = hvd.DistributedOptimizer(
                optimizer,
                named_parameters=model.named_parameters(),
                op=hvd.Average,
            )
            for epoch in range(num_epochs):
                model.train()
                for batch in dataset_shard.iter_torch_batches(
                    batch_size=32, dtypes=torch.float
                ):
                    inputs, labels = torch.unsqueeze(batch["x"], 1), batch["y"]
                    inputs.to(device)
                    labels.to(device)
                    outputs = model(inputs)
                    loss = loss_fn(outputs, labels)
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()
                    print(f"epoch: {epoch}, loss: {loss.item()}")
                session.report(
                    {},
                    checkpoint=Checkpoint.from_dict(
                        dict(model=model.state_dict())
                    ),
                )
        train_dataset = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
        scaling_config = ScalingConfig(num_workers=3)
        # If using GPUs, use the below scaling config instead.
        # scaling_config = ScalingConfig(num_workers=3, use_gpu=True)
        trainer = HorovodTrainer(
            train_loop_per_worker=train_loop_per_worker,
            scaling_config=scaling_config,
            datasets={"train": train_dataset},
        )
        result = trainer.fit()

    Args:
        train_loop_per_worker: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_loop_config: Configurations to pass into
            ``train_loop_per_worker`` if it accepts an argument.
        horovod_config: Configuration for setting up the Horovod backend.
            If set to None, use the default configuration. This replaces the
            ``backend_config`` arg of ``DataParallelTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        horovod_config: Optional[HorovodConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=horovod_config or HorovodConfig(),
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )
