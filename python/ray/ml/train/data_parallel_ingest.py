from typing import Dict, List, Union, TYPE_CHECKING

from ray.actor import ActorHandle
from ray.ml.config import DatasetConfig
from ray.ml.preprocessor import Preprocessor

if TYPE_CHECKING:
    from ray.data import Dataset, DatasetPipeline


class _DataParallelIngestSpec:
    def __init__(
        self, datasets: Dict[str, "Dataset"], dataset_config: Dict[str, DatasetConfig]
    ):
        self.datasets = datasets
        self.dataset_config = dataset_config

    def preprocess_datasets(self, prep: Preprocessor, datasets: Dict[str, "Dataset"]):
        if not prep:
            return datasets
        ds_to_fit = None
        for k, v in self.dataset_config.items():
            if v.fit:
                ds_to_fit = self.datasets[k]
        if ds_to_fit:
            prep.fit(ds_to_fit)
        new_datasets = {}

        for key, dataset in self.datasets.items():
            if self._config(key).no_transform:
                new_datasets[key] = dataset
            else:
                new_datasets[key] = prep.transform(dataset)
        return new_datasets

    def get_dataset_shards(
        self, training_worker_handles: List[ActorHandle]
    ) -> List[Dict[str, Union["Dataset", "DatasetPipeline"]]]:
        dataset_dict_splits = [{} for _ in range(len(training_worker_handles))]

        for key, dataset in self.datasets.items():
            config = self._config(key)

            if config.streamable:
                # TODO(ekl) implement and test this option.
                # dataset = dataset.repeat()
                raise NotImplementedError

            if config.split:
                dataset_splits = dataset.split(
                    len(training_worker_handles),
                    equal=True,
                    locality_hints=training_worker_handles,
                )
            else:
                # TODO(ekl) implement and test this option.
                # dataset_splits = [dataset] * len(training_worker_handles)
                raise NotImplementedError

            for i in range(len(dataset_splits)):
                dataset_dict_splits[i][key] = dataset_splits[i]

        return dataset_dict_splits

    def _config(self, key: str) -> "DatasetConfig":
        if key in self.dataset_config:
            return self.dataset_config[key]
        return self.dataset_config["*"]
