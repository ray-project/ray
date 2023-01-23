from typing import TYPE_CHECKING, Callable, Dict, Optional, Union

from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.torch.config import TorchConfig
from ray.train.trainer import GenDataset
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
class TorchTrainer(DataParallelTrainer):
    """A Trainer for data parallel PyTorch training.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors. These actors already have the necessary torch process group
    configured for distributed PyTorch training.

    The ``train_loop_per_worker`` function is expected to take in either 0 or 1
    arguments:

    .. testcode::

        def train_loop_per_worker():
            ...

    .. testcode::

        from typing import Dict, Any
        def train_loop_per_worker(config: Dict[str, Any]):
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
    :ref:`Ray AIR session methods <air-session-ref>`. See full example code below.

    .. testcode::

        def train_loop_per_worker():
            # Report intermediate results for callbacks or logging and
            # checkpoint data.
            session.report(...)

            # Get dict of last saved checkpoint.
            session.get_checkpoint()

            # Session returns the Ray Dataset shard for the given key.
            session.get_dataset_shard("my_dataset")

            # Get the total number of workers executing training.
            session.get_world_size()

            # Get the rank of this worker.
            session.get_world_rank()

            # Get the rank of the worker on the current node.
            session.get_local_rank()

    You can also use any of the Torch specific function utils,
    such as :func:`ray.train.torch.get_device` and :func:`ray.train.torch.prepare_model`

    .. testcode::

        def train_loop_per_worker():
            # Prepares model for distribted training by wrapping in
            # `DistributedDataParallel` and moving to correct device.
            train.torch.prepare_model(...)

            # Configures the dataloader for distributed training by adding a
            # `DistributedSampler`.
            # You should NOT use this if you are doing
            # `session.get_dataset_shard(...).iter_torch_batches(...)`
            train.torch.prepare_data_loader(...)

            # Get the current torch device.
            train.torch.get_device()

    Any returns from the ``train_loop_per_worker`` will be discarded and not
    used or persisted anywhere.

    To save a model to use for the ``TorchPredictor``, you must save it under the
    "model" kwarg in ``Checkpoint`` passed to ``session.report()``.

    .. note::
        If you are wrapping your ``model`` with ``prepare_model``, save ``model`` to
        session is equivalent to saving ``model.module`` to session.
        When you load from a saved checkpoint, make sure that you first
        load ``state_dict`` to model before calling ``prepare_model``.
        Otherwise, you will run into errors like
        ``Error(s) in loading state_dict for DistributedDataParallel:
        Missing key(s) in state_dict: "module.conv1.weight", ...``. See snippet below.

        .. testcode::

            from torchvision.models import resnet18
            from ray.air import session
            from ray.air.checkpoint import Checkpoint
            import ray.train as train

            def train_func():
                ...
                model = resnet18()
                # The following wraps model with DDP.
                # An effect of that is in `state_dict`, keys are prefixed by
                # `module.`. For example:
                # `layer1.0.bn1.bias` becomes `module.layer1.0.bn1.bias`.
                model = train.torch.prepare_model(model)
                for epoch in range(3):
                    ...
                    ckpt = Checkpoint.from_dict({
                        "epoch": epoch,
                        "model": model.state_dict(),
                        # "model": model.module.state_dict(),
                        # ** The above two are equivalent **
                    })
                    session.report({"foo": "bar"}, ckpt)

    Example:

        .. testcode::

            import torch
            import torch.nn as nn

            import ray
            from ray import train
            from ray.air import session, Checkpoint
            from ray.train.torch import TorchTrainer
            from ray.air.config import ScalingConfig
            from ray.air.config import RunConfig
            from ray.air.config import CheckpointConfig

            # Define NN layers archicture, epochs, and number of workers
            input_size = 1
            layer_size = 32
            output_size = 1
            num_epochs = 200
            num_workers = 3

            # Define your network structure
            class NeuralNetwork(nn.Module):
                def __init__(self):
                    super(NeuralNetwork, self).__init__()
                    self.layer1 = nn.Linear(input_size, layer_size)
                    self.relu = nn.ReLU()
                    self.layer2 = nn.Linear(layer_size, output_size)

                def forward(self, input):
                    return self.layer2(self.relu(self.layer1(input)))

            # Define your train worker loop
            def train_loop_per_worker():

                # Fetch training set from the session
                dataset_shard = session.get_dataset_shard("train")
                model = NeuralNetwork()

                # Loss function, optimizer, prepare model for training.
                # This moves the data and prepares model for distributed
                # execution
                loss_fn = nn.MSELoss()
                optimizer = torch.optim.Adam(model.parameters(),
                            lr=0.01,
                            weight_decay=0.01)
                model = train.torch.prepare_model(model)

                # Iterate over epochs and batches
                for epoch in range(num_epochs):
                    for batches in dataset_shard.iter_torch_batches(batch_size=32,
                                dtypes=torch.float):

                        # Add batch or unsqueeze as an additional dimension [32, x]
                        inputs, labels = torch.unsqueeze(batches["x"], 1), batches["y"]
                        output = model(inputs)

                        # Make output shape same as the as labels
                        loss = loss_fn(output.squeeze(), labels)

                        # Zero out grads, do backward, and update optimizer
                        optimizer.zero_grad()
                        loss.backward()
                        optimizer.step()

                        # Print what's happening with loss per 30 epochs
                        if epoch % 20 == 0:
                            print(f"epoch: {epoch}/{num_epochs}, loss: {loss:.3f}")

                    # Report and record metrics, checkpoint model at end of each
                    # epoch
                    session.report({"loss": loss.item(), "epoch": epoch},
                                         checkpoint=Checkpoint.from_dict(
                                         dict(epoch=epoch, model=model.state_dict()))
                    )

            torch.manual_seed(42)
            train_dataset = ray.data.from_items(
                [{"x": x, "y": 2 * x + 1} for x in range(200)]
            )

            # Define scaling and run configs
            # If using GPUs, use the below scaling config instead.
            # scaling_config = ScalingConfig(num_workers=3, use_gpu=True)
            scaling_config = ScalingConfig(num_workers=num_workers)
            run_config = RunConfig(checkpoint_config=CheckpointConfig(num_to_keep=1))

            trainer = TorchTrainer(
                train_loop_per_worker=train_loop_per_worker,
                scaling_config=scaling_config,
                run_config=run_config,
                datasets={"train": train_dataset})

            result = trainer.fit()

            best_checkpoint_loss = result.metrics['loss']

            # Assert loss is less 0.09
            assert best_checkpoint_loss <= 0.09

    .. testoutput::
        :hide:
        :options: +ELLIPSIS

        ...

    Args:

        train_loop_per_worker: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_loop_config: Configurations to pass into
            ``train_loop_per_worker`` if it accepts an argument.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ``ray.data.Preprocessor`` to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not torch_config:
            torch_config = TorchConfig()

        super(TorchTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )
