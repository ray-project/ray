import functools
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, Optional, Type, Tuple, Union

from packaging.version import Version

import accelerate

from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchConfig
from ray.train.trainer import GenDataset

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor
    from ray.tune.trainable import Trainable

from ray.train.torch import TorchTrainer, get_device
from ray.train.torch.config import _set_torch_distributed_env_vars

try:
    from ray.train.huggingface.accelerate._accelerate_utils import (
        launch_command,
        AccelerateDefaultNamespace,
        AccelerateConfigWrapper,
        load_accelerate_config,
    )
except ImportError as e:
    if "AccelerateTrainer requires accelerate" not in e.msg:
        raise
    launch_command = None
    AccelerateDefaultNamespace = None
    AccelerateConfigWrapper = None
    load_accelerate_config = None


class AccelerateTrainer(TorchTrainer):
    """A Trainer for data parallel HuggingFace Accelerate training with PyTorch.

    This Trainer is a wrapper around the :class:`~ray.train.torch.TorchTrainer`,
    providing the following extra functionality:
    1. Loading and parsing of Accelerate configuration files (created by
    ``accelerate config`` CLI command),
    2. Applying the configuration files on all workers, making sure the environment
    is set up correctly.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors. These actors already have the necessary torch process group
    configured for distributed PyTorch training, as well as all environment variables
    required by Accelerate, as defined in the configuration file. This allows you
    to use Accelerate APIs (such as ``Accelerator``) inside ``train_loop_per_worker``
    as you would without Ray.

    Inside the ``train_loop_per_worker`` function, In addition to Accelerate APIs, you
    can use any of the :ref:`Ray AIR session methods <air-session-ref>`.
    See full example code below.

    .. testcode::

        def train_loop_per_worker():
            # Report intermediate results for callbacks or logging and
            # checkpoint data.
            session.report(...)

            # Get dict of last saved checkpoint.
            session.get_checkpoint()

            # Session returns the Datastream shard for the given key.
            session.get_dataset_shard("my_dataset")

            # Get the total number of workers executing training.
            session.get_world_size()

            # Get the rank of this worker.
            session.get_world_rank()

            # Get the rank of the worker on the current node.
            session.get_local_rank()

    For more information, see the documentation of
    :class:`~ray.train.torch.TorchTrainer`.

    .. note::

        You need to use ``session.report()`` to communicate results and checkpoints
        back to Ray Train.

    Accelerate integrations with DeepSpeed, FSDP, MegatronLM etc. are fully supported.
    If the Accelerate configuration contains a path to a DeepSpeed config file
    (``deepspeed_config_file``), that file will also be loaded and applied on the
    workers.

    The following Accelerate configuration options will be ignored and automatically
    set by the Trainer according to Ray AIR configs (eg. ``ScalingConfig``):
    - Number of machines (``num_machines``)
    - Number of processes (``num_processes``)
    - Rank of the current machine (``machine_rank``)
    - Local rank of the current machine
    - GPU IDs (``gpu_ids``)
    - Number of CPU threads per process (``num_cpu_threads_per_process``)
    - IP of the head process (``main_process_ip``)
    - Port of the head process (``main_process_port``)
    - Whether all machines are on the same network (``same_network``)
    - Whether to force a CPU-only mode (``cpu``/``use_cpu``)
    - rdzv backend (``rdzv_backend``)
    - Main training function (``main_training_function``)
    - Type of launcher

    This Trainer requires ``accelerate>=0.17.0`` package.
    It is tested with ``accelerate==0.17.1``.

    Example:
        .. testcode::

            import torch
            import torch.nn as nn

            from accelerate import Accelerator

            import ray
            from ray.air import session, Checkpoint
            from ray.train.huggingface.accelerate import AccelerateTrainer
            from ray.air.config import ScalingConfig
            from ray.air.config import RunConfig
            from ray.air.config import CheckpointConfig

            # If using GPUs, set this to True.
            use_gpu = False

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

                # Initialize the Accelerator
                accelerator = Accelerator()

                # Fetch training set from the session
                dataset_shard = session.get_dataset_shard("train")
                model = NeuralNetwork()

                # Loss function, optimizer, prepare model for training.
                # This moves the data and prepares model for distributed
                # execution
                loss_fn = nn.MSELoss()
                optimizer = torch.optim.Adam(
                    model.parameters(), lr=0.01, weight_decay=0.01
                )
                model, optimizer = accelerator.prepare(model, optimizer)

                # Iterate over epochs and batches
                for epoch in range(num_epochs):
                    for batches in dataset_shard.iter_torch_batches(
                        batch_size=32, dtypes=torch.float
                    ):

                        # Add batch or unsqueeze as an additional dimension [32, x]
                        inputs, labels = torch.unsqueeze(batches["x"], 1), batches["y"]
                        output = model(inputs)

                        # Make output shape same as the as labels
                        loss = loss_fn(output.squeeze(), labels)

                        # Zero out grads, do backward, and update optimizer
                        optimizer.zero_grad()
                        accelerator.backward(loss)
                        optimizer.step()

                        # Print what's happening with loss per 30 epochs
                        if epoch % 20 == 0:
                            print(f"epoch: {epoch}/{num_epochs}, loss: {loss:.3f}")

                    # Report and record metrics, checkpoint model at end of each
                    # epoch
                    session.report(
                        {"loss": loss.item(), "epoch": epoch},
                        checkpoint=Checkpoint.from_dict(
                            dict(
                                epoch=epoch,
                                model=accelerator.unwrap_model(model).state_dict(),
                            )
                        ),
                    )

            torch.manual_seed(42)
            train_dataset = ray.data.from_items(
                [{"x": x, "y": 2 * x + 1} for x in range(200)]
            )

            # Define scaling and run configs
            scaling_config = ScalingConfig(num_workers=3, use_gpu=use_gpu)
            run_config = RunConfig(checkpoint_config=CheckpointConfig(num_to_keep=1))

            trainer = AccelerateTrainer(
                train_loop_per_worker=train_loop_per_worker,
                # Instead of using a dict, you can run ``accelerate config``.
                # The default value of None will then load that configuration
                # file.
                accelerate_config={},
                scaling_config=scaling_config,
                run_config=run_config,
                datasets={"train": train_dataset},
            )

            result = trainer.fit()

            best_checkpoint_loss = result.metrics["loss"]

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
        accelerate_config: Accelerate configuration to be applied on every worker.
            This can be a path to a file generated with ``accelerate config``,
            a configuration dict or None, in which case it will load the configuration
            file from the default location as defined by Accelerate.
        torch_config: Configuration for setting up the PyTorch backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        dataset_config: Configuration for dataset ingest.
        run_config: Configuration for the execution of the training run.
        datasets: Any Datastreams to use for training. Use
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
        accelerate_config: Optional[Union[dict, str, Path, os.PathLike]] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        if Version(accelerate.__version__) < Version("0.17.0.dev0"):
            raise RuntimeError(
                "AccelerateTrainer requires accelerate>=0.17.0, "
                f"got {accelerate.__version__}"
            )

        self.accelerate_config = accelerate_config
        (
            self._accelerate_config_raw,
            self._deepspeed_config_file_raw,
        ) = self._unwrap_accelerate_config_if_needed(accelerate_config)

        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def _unwrap_accelerate_config_if_needed(
        self,
        accelerate_config: Optional[
            Union[dict, str, Path, os.PathLike, AccelerateConfigWrapper]
        ],
    ) -> Tuple[str, Optional[str]]:
        # The AccelerateConfigWrapper is used to deal with the issue of the
        # Trainer being initialized twice (by the user and by us in the Trainable).
        # If it's initialized by the user, accelerate_config will not be an instance
        # of AccelerateConfigWrapper. This means we should read the config file from
        # given path.
        # If accelerate_config is an instance of AccelerateConfigWrapper, that means
        # we are dealing with a file that was already read, and we should instead use
        # the string in the wrapper as the raw contents of the file. This should
        # only happen internally, during initialization of this class in the Trainable.
        if isinstance(accelerate_config, AccelerateConfigWrapper):
            return (
                accelerate_config.config_raw,
                accelerate_config.deepspeed_config_raw,
            )
        else:
            return load_accelerate_config(accelerate_config)

    def as_trainable(self) -> Type["Trainable"]:
        # We want to load the config when the Trainer is first instantiated,
        # and share the contents with the Trainables (which may be on different)
        # nodes
        old_accelerate_config = self._param_dict.get("accelerate_config", None)
        self._param_dict["accelerate_config"] = AccelerateConfigWrapper(
            self._accelerate_config_raw, self._deepspeed_config_file_raw
        )
        try:
            ret = super().as_trainable()
        finally:
            self._param_dict["accelerate_config"] = old_accelerate_config
        return ret

    def training_loop(self) -> None:
        old_train_loop_per_worker = self._train_loop_per_worker
        self._train_loop_per_worker = self._wrap_train_loop_per_worker(
            self._train_loop_per_worker,
            self._accelerate_config_raw,
            self._deepspeed_config_file_raw,
        )
        try:
            ret = super().training_loop()
        finally:
            self._train_loop_per_worker = old_train_loop_per_worker
        return ret

    @classmethod
    def _wrap_train_loop_per_worker(
        cls,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        accelerate_config_raw: str,
        deepspeed_config_file_raw: str,
    ):
        """Wrap around train_loop_per_worker to set necessary Accelerate env vars."""

        @functools.wraps(train_loop_per_worker)
        def _accelerate_train_loop_per_worker(*args, **kwargs):
            with tempfile.TemporaryDirectory() as tempdir:
                # Write Accelerate config to file so it can be read
                # by Accelerate
                temp_config_file = os.path.join(tempdir, "default_config.yaml")
                with open(temp_config_file, "w") as f:
                    f.write(accelerate_config_raw)

                # Set by TorchBackend
                master_addr = os.environ["MASTER_ADDR"]
                master_port = os.environ["MASTER_PORT"]

                namespace = AccelerateDefaultNamespace()
                namespace.config_file = temp_config_file
                namespace.num_processes = 1
                namespace.num_machines = session.get_world_size()
                namespace.machine_rank = session.get_world_rank()
                namespace.num_cpu_threads_per_process = (
                    session.get_trial_resources().bundles[-1].get("CPU", 1)
                )
                namespace.gpu_ids = None
                namespace.main_process_ip = master_addr
                namespace.main_process_port = master_port
                namespace.same_network = False

                device = get_device()
                if isinstance(device, list):
                    device = device[0]
                if device.type == "cpu":
                    os.environ["LOCAL_RANK"] = "-1"
                    namespace.use_cpu = True
                else:
                    namespace.use_cpu = False

                # Handle DeepSpeed config
                if isinstance(deepspeed_config_file_raw, dict):
                    namespace.deepspeed_config_file = deepspeed_config_file_raw
                elif deepspeed_config_file_raw:
                    deepspeed_config_file = os.path.join(
                        tempdir, "deepspeed_config.json"
                    )
                    with open(deepspeed_config_file, "w") as f:
                        f.write(deepspeed_config_file_raw)
                    namespace.deepspeed_config_file = deepspeed_config_file

                # Let Accelerate set all env vars
                launch_command(namespace)

                # Set our env vars again to override Accelerate
                os.environ["MASTER_ADDR"] = master_addr
                os.environ["MASTER_PORT"] = master_port
                _set_torch_distributed_env_vars()

                if device.type == "cpu":
                    os.environ["LOCAL_RANK"] = "-1"

                return train_loop_per_worker(*args, **kwargs)

        return _accelerate_train_loop_per_worker
