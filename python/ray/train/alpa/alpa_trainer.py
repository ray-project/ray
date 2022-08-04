import os
from typing import Optional, Dict, Type, Union, Callable, TYPE_CHECKING


from ray.air.checkpoint import Checkpoint
from ray.air.config import ScalingConfig, RunConfig
from ray.train.trainer import BaseTrainer, GenDataset

from ray.train.alpa.config import AlpaConfig

from ray.tune import Trainable
from ray.util import PublicAPI
from ray._private.dict import merge_dicts

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor
from ray.tune.trainable import wrap_function

import logging

import ray
from ray.train.constants import (
    TRAIN_DATASET_KEY,
    WILDCARD_KEY,
)

from ray.air.config import DatasetConfig


try:
    import alpa
    from alpa.device_mesh import VirtualPhysicalMesh, DeviceCluster
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "alpa isn't installed. To install alpa, run 'pip install " "alpa'."
    )

from ray.train.alpa.utils import (
    is_ray_node_resource,
    ScalingConfigWithIPs,
    update_jax_platform,
    get_bundle2ip, 
    AlpaManager
)
from icecream import ic

from ray.util.placement_group import get_current_placement_group, remove_placement_group

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
        # set up alpa cluster manager
        self.alpa_manager = AlpaManager(scaling_config) 
        self.host_ips = self.alpa_manager.host_ips

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
        # needs to add this to the head node
        # otherwise RuntimeError: Backend 'gpu' failed to initialize:
        # FAILED_PRECONDITION: No visible GPU devices.
        # os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        
        # intialize alpa cluster
        self.alpa_manager.init_global_cluster()

        if self._train_loop_config:
            self._train_loop(self._train_loop_config)
        else:
            self._train_loop()

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
                # adding the ip address of the head node to the config
                # `alpa` needs to allocate resources on the fixed node
                updated_scaling_config_dict["ips"] = self.host_ips

                assert updated_scaling_config_dict["num_workers"] <= len(
                    self.host_ips
                ), (
                    "The number of workers must not exceed the number of hosts. "
                    "Either decrease the number of workers or check whether "
                    "connected to the ray cluster via `ray.init('auto')`"
                )
                if isinstance(updated_scaling_config_dict, dict):
                    updated_scaling_config = ScalingConfigWithIPs(
                        **updated_scaling_config_dict
                    )

                # print(updated_scaling_config.as_placement_group_factory())
                # exit()
                return updated_scaling_config.as_placement_group_factory()

        return TrainTrainable
