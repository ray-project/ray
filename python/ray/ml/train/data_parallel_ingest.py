from typing import Dict, List, Union, TYPE_CHECKING

from ray.actor import ActorHandle
from ray.ml.config import DatasetConfig
from ray.ml.preprocessor import Preprocessor

if TYPE_CHECKING:
    from ray.data import Dataset, DatasetPipeline


class _DataParallelIngestSpec:
    def __init__(self, dataset_config: Dict[str, DatasetConfig]):
        self.dataset_config = dataset_config
        self.preprocessed_datasets = None

    def preprocess_datasets(self, prep: Preprocessor, datasets: Dict[str, "Dataset"]):
        if prep:
            ds_to_fit = None
            for k, v in self.dataset_config.items():
                if v.fit:
                    ds_to_fit = datasets[k]
            if ds_to_fit:
                prep.fit(ds_to_fit)
            new_datasets = {}

            for key, dataset in datasets.items():
                if self._config(key).transform:
                    new_datasets[key] = prep.transform(dataset)
                else:
                    new_datasets[key] = dataset
        else:
            new_datasets = datasets
        self.preprocessed_datasets = new_datasets
        return new_datasets

    def get_dataset_shards(
        self, training_worker_handles: List[ActorHandle]
    ) -> List[Dict[str, Union["Dataset", "DatasetPipeline"]]]:
        """Note: this has to match the signature of DatasetSpec in legacy train."""
        dataset_dict_splits = [{} for _ in range(len(training_worker_handles))]

        for key, dataset in self.preprocessed_datasets.items():
            config = self._config(key)

            if config.streamable:
                dataset = dataset.repeat()

            if config.split:
                dataset_splits = dataset.split(
                    len(training_worker_handles),
                    equal=True,
                    locality_hints=training_worker_handles,
                )
            else:
                dataset_splits = [dataset] * len(training_worker_handles)

            for i in range(len(dataset_splits)):
                dataset_dict_splits[i][key] = dataset_splits[i]

        return dataset_dict_splits

    def _config(self, key: str) -> "DatasetConfig":
        if key in self.dataset_config:
            return self.dataset_config[key]
        return self.dataset_config["*"]
