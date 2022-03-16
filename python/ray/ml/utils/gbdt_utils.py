from typing import Dict, Type, Any
import warnings

from ray import tune
from ray.util.annotations import DeveloperAPI
from ray.ml.trainer import Trainer
from ray.ml.checkpoint import Checkpoint
from ray.tune import Trainable
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.ml.constants import PREPROCESSOR_KEY

import xgboost_ray

PARAMS_KEY = "params"
DMATRIX_PARAMS_KEY = "dmatrix_params"


@DeveloperAPI
class GBDTTrainer(Trainer):
    """Common logic for XGBoost-Ray and LightGBM-Ray."""

    _use_lightgbm: bool = False

    def _validate_dmatrix_params(self, config: Dict[str, Any], datasets: Dict) -> None:
        if DMATRIX_PARAMS_KEY in config:
            for key in config[DMATRIX_PARAMS_KEY]:
                if key not in datasets:
                    raise KeyError(
                        f"'{DMATRIX_PARAMS_KEY}' dict contains key '{key}' "
                        f"which is not present in `datasets`."
                    )

    def _get_dmatrices(
        self, dmatrix_params: Dict[str, Any]
    ) -> Dict[str, xgboost_ray.RayDMatrix]:
        dmatrix_class = xgboost_ray.RayDMatrix
        if self._use_lightgbm:
            import lightgbm_ray

            dmatrix_class = lightgbm_ray.RayDMatrix
        return {
            k: dmatrix_class(v, label=self.label_column, **dmatrix_params.get(k, {}))
            for k, v in self.datasets.items()
        }

    def _convert_pgf_to_ray_params(
        self,
        pgf: PlacementGroupFactory,
    ) -> "xgboost_ray.RayParams":
        extra_params = {}
        if self._use_lightgbm:
            import lightgbm_ray

            ray_params_cls = lightgbm_ray.RayParams
            extra_params["allow_less_than_two_cpus"] = True
        else:
            ray_params_cls = xgboost_ray.RayParams

        resources_per_worker = pgf.bundles[-1]
        num_workers = len(pgf.bundles) - int(not pgf.head_bundle_is_empty)
        cpus_per_worker = resources_per_worker.pop("CPU", 1)
        gpus_per_worker = resources_per_worker.pop("GPU", 0)

        ray_params = ray_params_cls(
            num_actors=int(num_workers),
            cpus_per_actor=int(cpus_per_worker),
            gpus_per_actor=int(gpus_per_worker),
            resources_per_actor=resources_per_worker,
            **extra_params,
        )

        return ray_params

    def setup(self) -> None:
        super().setup()
        resources_per_trial: PlacementGroupFactory = tune.get_trial_resources()
        self._ray_params = self._convert_pgf_to_ray_params(resources_per_trial)

    def preprocess_datasets(self) -> None:
        super().preprocess_datasets()

        # XGBoost/LightGBM-Ray requires each dataset to have at least as many
        # blocks as there are workers.
        # Alternatively, we can just throw an exception here?
        for dataset_key, dataset in self.datasets.items():
            if dataset.num_blocks() < self._ray_params.num_actors:
                warnings.warn(
                    f"Dataset '{dataset_key}' has {dataset.num_blocks()} blocks, "
                    f"which is less than the `num_workers` "
                    f"{self._ray_params.num_actors}. "
                    f"This dataset will be automatically repartitioned to "
                    f"{self._ray_params.num_actors} blocks."
                )
                self.datasets[dataset_key] = dataset.repartition(
                    self._ray_params.num_actors
                )

    def as_trainable(self) -> Type[Trainable]:
        trainable_cls = super().as_trainable()
        # scaling_config = self.scaling_config

        class GBDTTrainable(trainable_cls):
            # TODO: Discuss if trainer should have 0 CPUs by default
            # @classmethod
            # def default_resource_request(
            #     cls, config: Dict[str, Any]
            # ) -> PlacementGroupFactory:
            #     config = config.copy()
            #     config.setdefault("scaling_config", scaling_config)
            #     config["scaling_config"].copy()
            #     config["scaling_config"].setdefault("trainer_resources", {"CPU": 0})
            #     return super().default_resource_request(config)

            def _postprocess_checkpoint(self, checkpoint_path: str):
                preprocessor = self._merged_config.get("preprocessor", None)
                if not checkpoint_path or preprocessor is None:
                    return
                checkpoint_obj = Checkpoint.from_dict({PREPROCESSOR_KEY: preprocessor})
                checkpoint_obj.to_directory(path=checkpoint_path)

        return GBDTTrainable
