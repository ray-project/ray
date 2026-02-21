import logging
from typing import Callable, Dict, Optional

from ray.train import Result
from ray.train.trainer import GenDataset
from ray.train.v2._internal.execution.train_fn_utils import (
    LocalTrainFnUtils,
    get_train_fn_utils,
    set_train_fn_utils,
)

logger = logging.getLogger(__name__)


class LocalController:
    def __init__(
        self, experiment_name: str, datasets: Optional[Dict[str, GenDataset]] = None
    ):
        if datasets is not None:
            datasets = {k: v() if callable(v) else v for k, v in datasets.items()}

        self.datasets = datasets
        self.experiment_name = experiment_name

    def run(self, train_func: Callable[[], None]) -> Result:
        set_train_fn_utils(
            LocalTrainFnUtils(
                experiment_name=self.experiment_name,
                dataset_shards=self.datasets,
            )
        )
        train_func()
        train_fn_utils = get_train_fn_utils()
        assert isinstance(train_fn_utils, LocalTrainFnUtils)
        return Result(
            metrics=train_fn_utils._get_last_metrics(),
            checkpoint=train_fn_utils.get_checkpoint(),
            path=None,
            error=None,
        )
