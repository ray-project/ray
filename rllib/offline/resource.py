from typing import Dict, List, TYPE_CHECKING
from ray.rllib.utils.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

DEFAULT_NUM_CPUS_PER_TASK = 0.5


@PublicAPI
def get_offline_io_resource_bundles(
    config: "AlgorithmConfig",
) -> List[Dict[str, float]]:
    # DatasetReader is the only offline I/O component today that
    # requires compute resources.
    if config.input_ == "dataset":
        input_config = config.input_config
        # TODO (Kourosh): parallelism is use for reading the dataset, which defaults to
        # num_workers. This logic here relies on the information that dataset reader
        # will have the same logic. So to remove the information leakage, inside
        # Algorithm config, we should set parallelism to num_workers if not specified
        # and only deal with parallelism here or in dataset_reader.py. same thing is
        # true with cpus_per_task.
        parallelism = input_config.get("parallelism", config.get("num_workers", 1))
        cpus_per_task = input_config.get(
            "num_cpus_per_read_task", DEFAULT_NUM_CPUS_PER_TASK
        )
        return [{"CPU": cpus_per_task} for _ in range(parallelism)]
    else:
        return []
