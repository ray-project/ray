from typing import Optional, Dict, Union, Callable, TYPE_CHECKING


from ray.air.checkpoint import Checkpoint
from ray.air.config import ScalingConfig, RunConfig
from ray.train.trainer import BaseTrainer, GenDataset

from ray.train.alpa.config import AlpaConfig

from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

import logging

from ray.train.constants import (
    TRAIN_DATASET_KEY,
    WILDCARD_KEY,
)

from ray.air.config import DatasetConfig

from ray.train.alpa.utils import AlpaManager

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class AlpaTrainer(BaseTrainer):
    """Alpa: Automating Parallelism trainer.

    References:
        Alpa: Automating Inter- and Intra-Operator
            Parallelism for Distributed Deep Learning
        https://arxiv.org/pdf/2201.12023.pdf
    """

    _scaling_config_allowed_keys = BaseTrainer._scaling_config_allowed_keys + [
        "num_workers",
        "resources_per_worker",
        "use_gpu",
        "placement_strategy",
    ]

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
        """Training loop for AlpaTrainer."""
        # intialize alpa cluster
        self.alpa_manager.init_global_cluster()

        if self._train_loop_config:
            self._train_loop(self._train_loop_config)
        else:
            self._train_loop()
