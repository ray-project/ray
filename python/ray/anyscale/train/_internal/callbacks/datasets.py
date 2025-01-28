from typing import Dict

import ray.train
from ray.train.v2._internal.callbacks.datasets import DatasetsSetupCallback


class AnyscaleDatasetsSetupCallback(DatasetsSetupCallback):
    def get_train_total_resources(
        self, scaling_config: ray.train.ScalingConfig
    ) -> Dict[str, float]:
        if scaling_config.elasticity_enabled:
            # If Train is running with a variable number of workers,
            # we can't provide a fixed number of resources to exclude.
            # Instead, Anyscale's implementation of Data+Train uses a shared
            # `AutoscalingCoordinator` component to allocate resources dynamically
            # across Train and multiple Datasets.
            return {}

        return super().get_train_total_resources(scaling_config)
