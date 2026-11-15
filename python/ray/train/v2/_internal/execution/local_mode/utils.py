import logging
import os
from typing import Any, Callable, Dict, Optional, Union

import pyarrow.fs

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
        storage_path: Optional[Union[str, os.PathLike]] = None,
        storage_filesystem: Optional[pyarrow.fs.FileSystem] = None,
    ):
        if datasets is not None:
            datasets = {k: v() if callable(v) else v for k, v in datasets.items()}

        self.datasets = datasets
        self.experiment_name = experiment_name

        # When a storage path is configured, checkpoints reported in local mode
        # are persisted to it so they survive the (often temporary) directory
        # they were originally created in.
        self.storage_context: Optional[StorageContext] = None
        if storage_path is not None:
            self.storage_context = StorageContext(
                storage_path=storage_path,
                experiment_dir_name=experiment_name,
                storage_filesystem=storage_filesystem,
            )

    def run(self, train_func: Callable[[], Any]) -> Result:
        set_train_fn_utils(
            LocalTrainFnUtils(
                experiment_name=self.experiment_name,
                dataset_shards=self.datasets,
                storage_context=self.storage_context,
            )
        )
        result = train_func()
        train_fn_utils = get_train_fn_utils()
        assert isinstance(train_fn_utils, LocalTrainFnUtils)
        return Result(
            metrics=train_fn_utils._get_last_metrics(),
            checkpoint=train_fn_utils.get_checkpoint(),
            path=(
                self.storage_context.experiment_fs_path
                if self.storage_context is not None
                else None
            ),
            error=None,
            return_value=result,
        )
