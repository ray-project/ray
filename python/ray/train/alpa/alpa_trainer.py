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

from ray.train.alpa.utils import is_ray_node_resource

from icecream import ic 


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

        # cluster = alpa.get_global_cluster()
        
        # ic("Distributed Training with Alpa using "
        #     f"{cluster.num_cpus} cpus and {cluster.num_devices} gpus."
        # )
        
        # vp = alpa.device_mesh.get_global_virtual_physical_mesh()
        # ic(vp)
        
        # logger.info(
        #      "Distributed Training with Alpa using "
        #     f"{cluster.num_cpus} cpus and {cluster.num_devices} gpus."
        # )
        
        self.scaling_config = scaling_config
         
        # scaling_config = self._validate_scaling_config(self.scaling_config)
        
        self.resources_per_worker = self.scaling_config.resources_per_worker
        
        num_cpus = self.scaling_config['num_cpus']
        num_gpus = int(self.scaling_config['use_gpu'])
        if 'GPU' in self.resources_per_worker: 
            num_gpus = self.resources_per_worker['GPU']
        
        ic(num_cpus, num_gpus)
        ic(scaling_config, self.resources_per_worker)

        
        from ray._private.worker import _global_node as ray_global_node
        # try:
        self.head_info = ray_global_node.address_info
        # except AttributeError as ae:
        #     raise RuntimeError(
        #         "Cannot access ray global node. Did you call ray.init?") \
        #         from ae
        self.head_ip = self.head_info["node_ip_address"]

        # Gather host ids
        self.host_info = []
        for node in ray.nodes():
            for key in node["Resources"]:
                if is_ray_node_resource(key):
                    self.host_info.append(node)

        # Gather device info
        self.host_num_devices = []
        for host_info in self.host_info:
            number = host_info["Resources"]["GPU"]
            assert number.is_integer()
            self.host_num_devices.append(int(number))
        
        ic(self.host_num_devices, self.host_info)

        # from ray.worker import _global_node as ray_global_node
        
        
        # Gather host ids
        host_info = []
        host_ips = []
        for node in ray.nodes():
            for key in node["Resources"]:
                if is_ray_node_resource(key):
                    host_ips.append(key.split('node:')[-1])
                    host_info.append(node)

        ic(host_info, host_ips)

        # num_devices_per_host = worker_group.num_gpus_per_worker
        # node_ids = [i for i in range(len(worker_group))]

        # # filter by workergourp
        # node_ips = [ w.metadata.node_ip for w in worker_group.workers ]
        # host_ip_2_host_info_dict = dict(zip(host_ips, host_info))
        # node_info = [ host_ip_2_host_info_dict[node_ip] for node_ip in node_ips]

        # self.vp_mesh = VirtualPhysicalMesh(host_ids=node_ids,
        #                     host_info=node_info,
        #                     head_ip=head_ip,
        #                     num_devices_per_host=num_devices_per_host,
        #                     parent=None)

        alpa.device_mesh.set_global_virtual_physical_mesh(self.vp_mesh)


        exit()
        
        cluster = alpa.get_global_cluster()
        logger.info(
             "Distributed Training with Alpa using "
            f"{cluster.num_cpus} cpus and {cluster.num_devices} gpus."
        )

        # self._train_loop = train_loop
        # self._train_loop_config = train_loop_config
        
        # self._datasets = datasets

        # super(AlpaTrainer, self).__init__(
        #     train_loop_per_worker=train_loop_per_worker,
        #     train_loop_config=train_loop_config,
        #     backend_config=alpa_config,
        #     scaling_config=scaling_config,
        #     dataset_config=dataset_config,
        #     run_config=run_config,
        #     datasets=datasets,
        #     preprocessor=preprocessor,
        #     resume_from_checkpoint=resume_from_checkpoint,
        # )

        super(AlpaTrainer, self).__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def training_loop(self) -> None:
        self._train_loop(self._datasets, self._train_loop_config)
        
        
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
                updated_scaling_config = config.get("scaling_config", scaling_config)
                scaling_config_dataclass = (
                    trainer_cls._validate_and_get_scaling_config_data_class(
                        updated_scaling_config
                    )
                )
                return scaling_config_dataclass.as_placement_group_factory()

        return TrainTrainable
    
    
    


# @dataclass
# @PublicAPI(stability="alpha")
# class ScalingConfigDataClass:

#     trainer_resources: Optional[Dict] = None
#     num_workers: Optional[int] = None
#     use_gpu: bool = False
#     resources_per_worker: Optional[Dict] = None
#     placement_strategy: str = "PACK"

#     def __post_init__(self):
#         self.resources_per_worker = (
#             self.resources_per_worker if self.resources_per_worker else {}
#         )
#         if self.resources_per_worker:
#             if not self.use_gpu and self.num_gpus_per_worker > 0:
#                 raise ValueError(
#                     "`use_gpu` is False but `GPU` was found in "
#                     "`resources_per_worker`. Either set `use_gpu` to True or "
#                     "remove `GPU` from `resources_per_worker."
#                 )

#             if self.use_gpu and self.num_gpus_per_worker == 0:
#                 raise ValueError(
#                     "`use_gpu` is True but `GPU` is set to 0 in "
#                     "`resources_per_worker`. Either set `use_gpu` to False or "
#                     "request a positive number of `GPU` in "
#                     "`resources_per_worker."
#                 )

#     @property
#     def num_cpus_per_worker(self):
#         """The number of CPUs to set per worker."""
#         return self.resources_per_worker.get("CPU", 1)

#     @property
#     def num_gpus_per_worker(self):
#         """The number of GPUs to set per worker."""
#         return self.resources_per_worker.get("GPU", int(self.use_gpu))

#     @property
#     def additional_resources_per_worker(self):
#         """Resources per worker, not including CPU or GPU resources."""
#         return {
#             k: v
#             for k, v in self.resources_per_worker.items()
#             if k not in ["CPU", "GPU"]
#         }

#     def as_placement_group_factory(self) -> "PlacementGroupFactory":
#         """Returns a PlacementGroupFactory to specify resources for Tune."""
#         from ray.tune.execution.placement_groups import PlacementGroupFactory

#         trainer_resources = (
#             self.trainer_resources if self.trainer_resources else {"CPU": 1}
#         )
#         trainer_bundle = [trainer_resources]
#         worker_resources = {
#             "CPU": self.num_cpus_per_worker,
#             "GPU": self.num_gpus_per_worker,
#         }
#         worker_resources_extra = (
#             {} if self.resources_per_worker is None else self.resources_per_worker
#         )
#         worker_bundles = [
#             {**worker_resources, **worker_resources_extra}
#             for _ in range(self.num_workers if self.num_workers else 0)
#         ]
#         bundles = trainer_bundle + worker_bundles
#         return PlacementGroupFactory(bundles, strategy=self.placement_strategy)
