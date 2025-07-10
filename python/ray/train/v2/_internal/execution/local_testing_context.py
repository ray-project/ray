import os
import threading
from typing import Any, Dict, Optional

import ray.data
from ray.data import DataIterator

_local_testing_context_lock = threading.Lock()
_local_test_configs = {}


def set_local_test_config(key: str, value: Any) -> None:
    with _local_testing_context_lock:
        _local_test_configs[key] = value


LOCAL_CONFIG_DATASET_NAME = "dataset_shard"
ALLOW_LOCAL_TRAIN_FUNCTION_RUN_ENV_VAR = "ALLOW_LOCAL_TRAIN_FUNCTION_RUN"


def is_local_train_function_run_allowed() -> bool:
    return os.environ.get(ALLOW_LOCAL_TRAIN_FUNCTION_RUN_ENV_VAR, "0") == "1"


class LocalTestingTrainContext:
    def get_world_rank(self) -> int:
        return 0

    def get_world_size(self) -> int:
        return 1

    def get_local_rank(self) -> int:
        return 0

    def get_local_world_size(self) -> int:
        return 1

    def get_node_rank(self) -> int:
        return 0

    def get_dataset_shard(self, dataset_name: str) -> DataIterator:
        dataset_shards = _local_test_configs.get(LOCAL_CONFIG_DATASET_NAME, {})
        return dataset_shards.get(dataset_name, ray.data.from_items([]))

    def report(
        self,
        metrics: Dict[str, Any],
        checkpoint: Optional[Any] = None,
        checkpoint_dir_name: Optional[str] = None,
    ):
        print(f"Local testing report metrics: {metrics}")


_local_testing_train_context = LocalTestingTrainContext()
