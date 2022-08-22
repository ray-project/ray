from typing import Optional, Dict, Union, Callable, TYPE_CHECKING

from ray.air._internal.config import ensure_only_allowed_dataclass_keys_updated

from ray.air.checkpoint import Checkpoint
from ray.air.config import ScalingConfig, RunConfig
from ray.train.trainer import BaseTrainer, GenDataset


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
        Alpa --- Automating Inter- and Intra-Operator
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

        # validate scaling config
        scaling_config = self._validate_scaling_config(scaling_config)

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

    @classmethod
    def _validate_scaling_config(cls, scaling_config: ScalingConfig) -> ScalingConfig:
        """Return scaling config dataclass after validating updated keys."""
        ensure_only_allowed_dataclass_keys_updated(
            dataclass=scaling_config,
            allowed_keys=cls._scaling_config_allowed_keys,
        )

        use_gpu = scaling_config.use_gpu

        # cpu or tpu parallelism are not supported yet in alpa
        if not use_gpu:
            raise ValueError(
                "cpu or tpu parallelism is not supported yet in alpa "
                "Please use distributed GPU training instead. "
            )

        # change the default value of `placment_strategy` to `SPREAD`.
        # using `SPREAD`: the alpatrainer will spread the workers across different nodes
        # between the nodes, the connections is to be established by NCCL;
        # within the nodes, alpa can already enable communicates automatically.
        # so, no end to use alpa manager to make the connections.
        # NOT `STRICT_SPREAD`: if the users use the trainer on one of GPU nodes instead
        # of on the extra cpu nodes, then `STRICT_SPREAD` will hang on for the resources
        if "PACK" in scaling_config.placement_strategy:
            scaling_config.placement_strategy = "SPREAD"
            logger.info(
                "In AlpaTrainer, the `placement_stategy` need to be `SPREAD`."
                " Placement strategy is now changed to `SPREAD`"
            )

        return scaling_config
