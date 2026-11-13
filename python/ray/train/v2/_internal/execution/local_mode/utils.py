import logging
from typing import Any, Callable, Dict, Optional

import pandas as pd

from ray.train import Result
from ray.train.trainer import GenDataset
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.train_fn_utils import (
    LocalTrainFnUtils,
    get_train_fn_utils,
    set_train_fn_utils,
)

logger = logging.getLogger(__name__)


class LocalController:
    def __init__(
        self,
        experiment_name: str,
        datasets: Optional[Dict[str, GenDataset]] = None,
        storage_context: Optional[StorageContext] = None,
    ):
        if datasets is not None:
            datasets = {k: v() if callable(v) else v for k, v in datasets.items()}

        self.datasets = datasets
        self.experiment_name = experiment_name
        self.storage_context = storage_context

    def run(self, train_func: Callable[[], Any]) -> Result:
        set_train_fn_utils(
            LocalTrainFnUtils(
                experiment_name=self.experiment_name,
                dataset_shards=self.datasets,
                storage_context=self.storage_context,
            )
        )
        return self._build_result(train_func())

    def _build_result(self, return_value: Any) -> Result:
        train_fn_utils = get_train_fn_utils()
        assert isinstance(train_fn_utils, LocalTrainFnUtils)
        best_checkpoints = train_fn_utils._get_reported_checkpoints()
        storage = self.storage_context
        return Result(
            metrics=train_fn_utils._get_last_metrics(),
            checkpoint=train_fn_utils.get_checkpoint(),
            path=storage.experiment_fs_path if storage else None,
            error=None,
            best_checkpoints=best_checkpoints,
            metrics_dataframe=(
                pd.DataFrame([m for _, m in best_checkpoints])
                if best_checkpoints
                else None
            ),
            _storage_filesystem=storage.storage_filesystem if storage else None,
            return_value=return_value,
        )
