from ray.rllib.offline.dataset_reader import (
    get_resource_bundles as dataset_reader_get_resource_bundles,
)
from ray.rllib.utils.typing import PartialTrainerConfigDict
from typing import Dict, List


def get_offline_io_resource_bundles(
    config: PartialTrainerConfigDict,
) -> List[Dict[str, float]]:
    # DatasetReader is the only offline I/O component today that
    # requires compute resources.
    if config["input"] == "dataset":
        return dataset_reader_get_resource_bundles(config["input_config"])
    else:
        return []
