import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, Optional, Type, Union

try:
    from packaging.version import Version
except ImportError:
    from distutils.version import LooseVersion as Version

import accelerate

from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.air.config import DatasetConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchConfig
from ray.train.trainer import GenDataset
from ray.train._internal.utils import construct_train_func

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

TRAIN_LOOP_PER_WORKER_KEY = "_train_loop_per_worker"
ACCELERATE_CONFIG_RAW_KEY = "_accelerate_config_raw"
DEEPSPEED_CONFIG_RAW_KEY = "_deepspeed_config_file_raw"
RESERVED_KEYS = {
    TRAIN_LOOP_PER_WORKER_KEY,
    ACCELERATE_CONFIG_RAW_KEY,
    DEEPSPEED_CONFIG_RAW_KEY,
}


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

        if Version(accelerate.__version__) < Version("0.17.0.dev0"):
            raise RuntimeError(
                "AccelerateTrainer requires accelerate>=0.17.0, "
                f"got {accelerate.__version__}"
            )

        self.accelerate_config = accelerate_config
        if isinstance(self.accelerate_config, AccelerateConfigWrapper):
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
            ) = load_accelerate_config(self.accelerate_config)
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

        namespace = AccelerateDefaultNamespace()
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
        namespace.same_network = False

        device = get_device()
        if isinstance(device, list):
            device = device[0]
        if device.type == "cpu":
            os.environ["LOCAL_RANK"] = "-1"
            namespace.use_cpu = True
            namespace.multi_gpu = False
        else:
            namespace.use_cpu = False
            namespace.multi_gpu = True

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

        if device.type == "cpu":
            os.environ["LOCAL_RANK"] = "-1"

        train_loop_per_worker = construct_train_func(
            train_loop_per_worker,
            config,
            fn_arg_name="train_loop_per_worker",
            discard_returns=True,
        )
        return train_loop_per_worker()
