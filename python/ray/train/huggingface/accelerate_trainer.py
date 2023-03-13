import os
import tempfile
from argparse import Namespace
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, Optional, Tuple, Type, Union

from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchConfig
from ray.train.trainer import GenDataset
from ray.train._internal.utils import construct_train_func

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor
    from ray.tune.trainable import Trainable

from accelerate.commands.config import default_config_file, load_config_from_file
from ray.train.torch import TorchTrainer, get_device
from ray.train.torch.config import _set_torch_distributed_env_vars

from ray.train.huggingface._accelerate_utils import (
    launch_command,
    launch_command_parser,
)

TRAIN_LOOP_PER_WORKER_KEY = "_train_loop_per_worker"
ACCELERATE_CONFIG_RAW_KEY = "_accelerate_config_raw"
DEEPSPEED_CONFIG_RAW_KEY = "_deepspeed_config_file_raw"
RESERVED_KEYS = {
    TRAIN_LOOP_PER_WORKER_KEY,
    ACCELERATE_CONFIG_RAW_KEY,
    DEEPSPEED_CONFIG_RAW_KEY,
}


class _AccelerateDefaultNamespace(Namespace):
    @property
    def parser(self):
        return launch_command_parser()

    def __getattr__(self, name: str):
        if name == "training_script_args":
            return []
        return self.parser.get_default(name)


class _AccelerateConfigWrapper:
    """
    Lets Trainables know to treat this as already loaded file content instead of path.
    """

    def __init__(
        self, config_raw: str, deepspeed_config_raw: Optional[str] = None
    ) -> None:
        self.config_raw = config_raw
        self.deepspeed_config_raw = deepspeed_config_raw

    def __bool__(self) -> bool:
        return bool(self.config_raw)


class AccelerateTrainer(TorchTrainer):
    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        accelerate_config: Optional[Union[str, Path, os.PathLike]] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        self.accelerate_config = accelerate_config or default_config_file
        if isinstance(self.accelerate_config, _AccelerateConfigWrapper):
            self._accelerate_config_raw = self.accelerate_config.config_raw
            self._deepspeed_config_file_raw = (
                self.accelerate_config.deepspeed_config_raw
            )
            train_loop_config = self._create_train_loop_per_worker(
                train_loop_per_worker,
                train_loop_config,
                self._accelerate_config_raw,
                self._deepspeed_config_file_raw,
            )
        else:
            (
                self._accelerate_config_raw,
                self._deepspeed_config_file_raw,
            ) = self._load_accelerate_config()
        super().__init__(
            train_loop_per_worker=_accelerate_train_loop_per_worker,
            train_loop_config=train_loop_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def _load_accelerate_config(self) -> Tuple[str, Optional[str]]:
        # We only load config to dict to obtain the deepspeed_config_file
        config = load_config_from_file(self.accelerate_config)
        deepspeed_config_file = getattr(config, "deepspeed_config_file", None)
        deepspeed_config_file_raw = None

        if deepspeed_config_file and not isinstance(deepspeed_config_file, dict):
            with open(deepspeed_config_file, "r") as f:
                deepspeed_config_file_raw = f.read()

        # Otherwise, we want to pass raw contents to Trainables for maximum
        # compatibility.
        with open(self.accelerate_config, "r") as f:
            return f.read(), deepspeed_config_file_raw

    def as_trainable(self) -> Type["Trainable"]:
        # We want to load the config when the Trainer is first instantiated,
        # and share the contents with the Trainables (which may be on different)
        # nodes
        old_accelerate_config = self._param_dict.get("accelerate_config", None)
        self._param_dict["accelerate_config"] = _AccelerateConfigWrapper(
            self._accelerate_config_raw, self._deepspeed_config_file_raw
        )
        try:
            ret = super().as_trainable()
        finally:
            self._param_dict["accelerate_config"] = old_accelerate_config
        return ret

    @classmethod
    def _create_train_loop_per_worker(
        cls,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        train_loop_per_worker_config,
        accelerate_config_raw: str,
        deepspeed_config_file_raw: str,
    ):
        """Wrap around train_loop_per_worker to set necessary Accelerate env vars."""
        train_loop_per_worker_config = (
            train_loop_per_worker_config.copy() if train_loop_per_worker_config else {}
        )
        for key in train_loop_per_worker_config:
            if key in RESERVED_KEYS:
                raise ValueError(
                    f"'{key}' is a reserved key in `train_loop_per_worker_config`."
                )
        if train_loop_per_worker:
            train_loop_per_worker_config[
                TRAIN_LOOP_PER_WORKER_KEY
            ] = train_loop_per_worker
        if accelerate_config_raw:
            train_loop_per_worker_config[
                ACCELERATE_CONFIG_RAW_KEY
            ] = accelerate_config_raw
        if deepspeed_config_file_raw:
            train_loop_per_worker_config[
                DEEPSPEED_CONFIG_RAW_KEY
            ] = deepspeed_config_file_raw
        return train_loop_per_worker_config


def _accelerate_train_loop_per_worker(config):
    train_loop_per_worker = config.pop(TRAIN_LOOP_PER_WORKER_KEY)
    accelerate_config_raw = config.pop(ACCELERATE_CONFIG_RAW_KEY)
    deepspeed_config_file_raw = config.pop(DEEPSPEED_CONFIG_RAW_KEY, None)

    with tempfile.TemporaryDirectory() as tempdir:
        # Write Accelerate config to file so it can be read
        # by Accelerate
        temp_config_file = os.path.join(tempdir, "default_config.yaml")
        with open(temp_config_file, "w") as f:
            f.write(accelerate_config_raw)

        # Set by TorchBackend
        master_addr = os.environ["MASTER_ADDR"]
        master_port = os.environ["MASTER_PORT"]

        namespace = _AccelerateDefaultNamespace()
        namespace.config_file = temp_config_file
        namespace.num_processes = 1
        namespace.num_machines = session.get_world_size()
        namespace.machine_rank = session.get_world_rank()
        namespace.num_cpu_threads_per_process = session.get_trial_resources().bundles[
            -1
        ]["CPU"]
        namespace.gpu_ids = None
        namespace.main_process_ip = master_addr
        namespace.main_process_port = master_port

        # Handle DeepSpeed config
        if isinstance(deepspeed_config_file_raw, dict):
            namespace.deepspeed_config_file = deepspeed_config_file_raw
        elif deepspeed_config_file_raw:
            deepspeed_config_file = os.path.join(tempdir, "deepspeed_config.json")
            with open(deepspeed_config_file, "w") as f:
                f.write(deepspeed_config_file_raw)
            namespace.deepspeed_config_file = deepspeed_config_file

        # Let Accelerate set all env vars
        launch_command(namespace)

        # Set our env vars again to override Accelerate
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = master_port
        _set_torch_distributed_env_vars()

        device = get_device()
        if isinstance(device, list):
            device = device[0]
        if device.type == "cpu":
            os.environ["LOCAL_RANK"] = "-1"

        train_loop_per_worker = construct_train_func(
            train_loop_per_worker,
            config,
            fn_arg_name="train_loop_per_worker",
            discard_returns=True,
        )
        return train_loop_per_worker()
