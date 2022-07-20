import inspect
import os
from typing import Optional, Dict, Tuple, Type, Union, Callable, Any, TYPE_CHECKING

import ray.cloudpickle as cpickle
from ray.air.checkpoint import Checkpoint
from ray.air.config import ScalingConfig, RunConfig
from ray.train.trainer import BaseTrainer, GenDataset
from ray.air._internal.checkpointing import (
    load_preprocessor_from_dir,
    save_preprocessor_to_dir,
)
from ray.air.result import Result
from ray.train.alpa.config import AlpaConfig
 
from ray.tune import Trainable, PlacementGroupFactory
from ray.tune.logger import Logger
from ray.tune.registry import get_trainable_cls
from ray.tune.resources import Resources
from ray.util import PublicAPI
from ray.util.ml_utils.dict import merge_dicts

if TYPE_CHECKING:
    from ray.air.preprocessor import Preprocessor
from ray.tune.trainable import wrap_function

import inspect
import logging
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    Union,
    Type,
    TYPE_CHECKING,
)

import ray
from ray import tune
from ray.air.constants import MODEL_KEY, PREPROCESSOR_KEY
from ray.train.constants import (
    TRAIN_DATASET_KEY,
    WILDCARD_KEY,
)
from ray.train.trainer import BaseTrainer
from ray.air.config import ScalingConfig, RunConfig, DatasetConfig
from ray.train.trainer import GenDataset
from ray.air.checkpoint import Checkpoint
from ray.train._internal.dataset_spec import DataParallelIngestSpec
from ray.train import BackendConfig, TrainingIterator
from ray.train._internal.backend_executor import BackendExecutor
from ray.train._internal.checkpoint import TuneCheckpointManager
from ray.train._internal.utils import construct_train_func
from ray.util.annotations import DeveloperAPI
from ray.util.ml_utils.checkpoint_manager import CheckpointStrategy, _TrackedCheckpoint

try:
    import alpa
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "alpa isn't installed. To install alpa, run 'pip install " "alpa'."
    )

if TYPE_CHECKING:
    from ray.air.preprocessor import Preprocessor

from ray.train.data_parallel_trainer import DataParallelTrainer, _load_checkpoint

from ray.train.alpa.utils import is_ray_node_resource, ScalingConfigWithIPs

from icecream import ic 

import alpa
from alpa.util import update_jax_platform
from alpa.device_mesh import VirtualPhysicalMesh

logger = logging.getLogger(__name__)

@PublicAPI(stability="beta")
class AlpaTrainer(BaseTrainer):
    """Alpa: Automating Parallelism trainer.

    References:
        Alpa: Automating Inter- and Intra-Operator
            Parallelism for Distributed Deep Learning
        https://arxiv.org/pdf/2201.12023.pdf
    """

    _dataset_config = {
        TRAIN_DATASET_KEY: DatasetConfig(fit=True, split=False),
        WILDCARD_KEY: DatasetConfig(split=False),
    }
    
    
    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        alpa_config: Optional[AlpaConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):

        if not ray.is_initialized():
            ray.init()

        # connect to the ray cluster
        if not alpa.api.is_initialized:
            alpa.init("ray")

        cluster = alpa.get_global_cluster()
        
        ic("Distributed Training with Alpa using "
            f"{cluster.num_cpus} cpus and {cluster.num_devices} gpus."
        )
        
        logger.info(
             "Distributed Training with Alpa using "
            f"{cluster.num_cpus} cpus and {cluster.num_devices} gpus."
        )
        
        # scaling parameters 
        self.scaling_config = scaling_config
        self.resources_per_worker = self.scaling_config.resources_per_worker
        
        num_workers = self.scaling_config.num_workers
        num_gpus = int(self.scaling_config.use_gpu)
        if 'GPU' in self.resources_per_worker: 
            num_gpus = self.resources_per_worker['GPU']
        
        ic(num_workers, num_gpus)
        ic(scaling_config, self.resources_per_worker)

        # head node info
        from ray._private.worker import _global_node as ray_global_node
        self.head_info = ray_global_node.address_info
        self.head_ip = self.head_info["node_ip_address"]
        ic(self.head_ip)
        
        # Gather host ids
        self.host_info = []
        self.host_ips = []
        
        for node in ray.nodes():
            for key in node["Resources"]:
                if is_ray_node_resource(key):
                    self.host_info.append(node)
                    self.host_ips.append(key.split('node:')[-1])
                    
        # Gather device info
        self.host_num_devices = []
        for host_info in self.host_info:
            number = host_info["Resources"]["GPU"]
            assert number.is_integer()
            self.host_num_devices.append(int(number))
        
        ic(self.host_num_devices, self.host_info, self.host_ips)        

        # number of workers filter
        # the number of workers can not exceeed the number of devices
        num_workers = min(num_workers, len(self.host_info))
        node_ids = [i for i in range(num_workers)]
        node_ips = [self.host_ips[i] for i in range(num_workers)]
        node_info = [self.host_info[i] for i in range(num_workers)]
        
        # filter the number of gpus per worker
        self.host_num_devices = [ self.host_num_devices[i] for i in range(num_workers)]
        num_devices_per_host = min(self.host_num_devices)
        num_devices_per_host = min(num_gpus, num_devices_per_host)

        self.vp_mesh = VirtualPhysicalMesh(host_ids=node_ids,
                            host_info=node_info,
                            head_ip=self.head_ip,
                            num_devices_per_host=num_devices_per_host,
                            parent=None)

        alpa.device_mesh.set_global_virtual_physical_mesh(self.vp_mesh)

        self._train_loop = train_loop_per_worker
        self._train_loop_config = train_loop_config
        
        super(AlpaTrainer, self).__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def training_loop(self) -> None:
        self._train_loop(self._train_loop_config)
        
    def as_trainable(self) -> Type[Trainable]:
        """Convert self to a ``tune.Trainable`` class."""

        base_config = self._param_dict
        trainer_cls = self.__class__
        scaling_config = self.scaling_config

        def train_func(config, checkpoint_dir=None):
            # config already contains merged values.
            # Instantiate new Trainer in Trainable.
            trainer = trainer_cls(**config)

            if checkpoint_dir:
                trainer.resume_from_checkpoint = Checkpoint.from_directory(
                    checkpoint_dir
                )

            trainer.setup()
            trainer.preprocess_datasets()
            trainer.training_loop()

        # Change the name of the training function to match the name of the Trainer
        # class. This will mean the Tune trial name will match the name of Trainer on
        # stdout messages and the results directory.
        train_func.__name__ = trainer_cls.__name__

        trainable_cls = wrap_function(train_func)

        class TrainTrainable(trainable_cls):
            """Add default resources to the Trainable."""

            # Workaround for actor name not being logged correctly
            # if __repr__ is not directly defined in a class.
            def __repr__(self):
                return super().__repr__()

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

                # Create a new config by merging the dicts.
                # run_config is not a tunable hyperparameter so it does not need to be
                # merged.
                run_config = base_config.pop("run_config", None)
                self._merged_config = merge_dicts(base_config, self.config)
                self._merged_config["run_config"] = run_config

            def _trainable_func(self, config, reporter, checkpoint_dir):
                # We ignore the config passed by Tune and instead use the merged
                # config which includes the initial Trainer args.
                super()._trainable_func(self._merged_config, reporter, checkpoint_dir)

            @classmethod
            def default_resource_request(cls, config):
                # `config["scaling_config"] is a dataclass when passed via the
                # `scaling_config` argument in `Trainer` and is a dict when passed
                # via the `scaling_config` key of `param_spec`.

                # Conversion logic must be duplicated in `TrainTrainable.__init__`
                # because this is a class method.
                updated_scaling_config = config.get("scaling_config", scaling_config)
                updated_scaling_config_dict = updated_scaling_config.__dict__
                updated_scaling_config_dict['ips'] = self.host_ips
                ic(updated_scaling_config_dict)
                ic(ScalingConfigWithIPs(**updated_scaling_config))
                if isinstance(updated_scaling_config, dict):
                    updated_scaling_config = ScalingConfigWithIPs(**updated_scaling_config_dict)
                # validated_scaling_config = trainer_cls._validate_scaling_config(
                #     updated_scaling_config
                # )
                ic(updated_scaling_config)
                return updated_scaling_config.as_placement_group_factory()
            
        return TrainTrainable
