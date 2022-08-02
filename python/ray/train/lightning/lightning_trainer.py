from typing import Type, Optional, Dict

import pytorch_lightning

from ray.util import PublicAPI
from ray.train.torch import TorchTrainer, TorchConfig
from ray.air.config import ScalingConfig, DatasetConfig, RunConfig
from ray.train.trainer import GenDataset
from ray.data.preprocessor import Preprocessor
from ray.air.checkpoint import Checkpoint


@PublicAPI(stability="alpha")
class LightningTrainer(TorchTrainer):
    def __init__(
        self,
        lightning_module: Type[pytorch_lightning.LightningModule],
        *,
        lightning_module_init_config: Optional[Dict] = None,
        trainer_init_config: Optional[Dict] = None,
        torch_config: Optional[TorchConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        # TODO (s10a): check PTL version

        # TODO (s10a): validate `lightning_module` is a class object, not instance

        trainer_init_config = trainer_init_config.copy() if trainer_init_config else {}
        if "_lightning_module" in trainer_init_config:
            raise ValueError(
                "'_lightning_module' is a reserved key in `trainer_init_config`."
            )
        trainer_init_config["_lightning_module"] = lightning_module
        if "_lightning_module_init_config" in trainer_init_config:
            raise ValueError(
                "'_lightning_module_init_config' is a reserved key in "
                "`trainer_init_config`."
            )
        trainer_init_config[
            "_lightning_module_init_config"
        ] = lightning_module_init_config

        super().__init__(
            train_loop_per_worker=_lightning_train_loop_per_worker,
            train_loop_config=trainer_init_config,
            torch_config=torch_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )


def _lightning_train_loop_per_worker(config):
    lightning_module = config.pop("_lightning_module")
    lightning_module_init_config = config.pop("_lightning_module_init_config")
    # TODO (s10a)
    # 1. Create a pytorch_lightning.Trainer, populating its args with the user
    #    provided scaling config and trainer_init_config
    # 2. Take the Dataset shard and convert to a PTL DataModule
    # 3. Call ptl_trainer.fit() with the user provided LightningModule and the
    #    DataModule that we created
    pass
