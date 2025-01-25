from typing import Any, Callable, Dict, Optional, Union

from ray.air.config import RunConfig, ScalingConfig
from ray.train import Checkpoint, DataConfig
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.train.horovod.config import HorovodConfig
from ray.train.trainer import GenDataset
from ray.util.annotations import Deprecated


@Deprecated
class HorovodTrainer(DataParallelTrainer):
    """A Trainer for data parallel Horovod training. HorovodTrainer is deprecated."""

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        horovod_config: Optional[HorovodConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[DataConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        raise DeprecationWarning(
            "`HorovodTrainer` is not supported and is scheduled to be removed "
            "in the future. "
            "Please consider using `TorchTrainer` or `TensorflowTrainer`, "
            "fall back to the old implementation with `RAY_TRAIN_V2_ENABLED=0`, "
            "or file an issue on Github describing your use case."
        )
