import os
import logging
import tempfile
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Type

from ray import train, tune
from ray.train import Checkpoint, RunConfig, ScalingConfig
from ray.train.constants import MODEL_KEY, TRAIN_DATASET_KEY
from ray.train.trainer import BaseTrainer, GenDataset
from ray.tune import Trainable
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.util.annotations import DeveloperAPI
from ray._private.dict import flatten_dict

if TYPE_CHECKING:
    import xgboost_ray

    from ray.data.preprocessor import Preprocessor

_WARN_REPARTITION_THRESHOLD = 10 * 1024**3
_DEFAULT_NUM_ITERATIONS = 10

logger = logging.getLogger(__name__)


def _convert_scaling_config_to_ray_params(
    scaling_config: ScalingConfig,
    ray_params_cls: Type["xgboost_ray.RayParams"],
    default_ray_params: Optional[Dict[str, Any]] = None,
) -> "xgboost_ray.RayParams":
    """Scaling config parameters have precedence over default ray params.

    Default ray params are defined in the trainers (xgboost/lightgbm),
    but if the user requests something else, that should be respected.
    """
    resources = (scaling_config.resources_per_worker or {}).copy()

    cpus_per_actor = resources.pop("CPU", 0)
    if not cpus_per_actor:
        cpus_per_actor = default_ray_params.get("cpus_per_actor", 0)

    gpus_per_actor = resources.pop("GPU", int(scaling_config.use_gpu))
    if not gpus_per_actor:
        gpus_per_actor = default_ray_params.get("gpus_per_actor", 0)

    resources_per_actor = resources
    if not resources_per_actor:
        resources_per_actor = default_ray_params.get("resources_per_actor", None)

    num_actors = scaling_config.num_workers
    if not num_actors:
        num_actors = default_ray_params.get("num_actors", 0)

    ray_params_kwargs = default_ray_params.copy() or {}

    ray_params_kwargs.update(
        {
            "cpus_per_actor": int(cpus_per_actor),
            "gpus_per_actor": int(gpus_per_actor),
            "resources_per_actor": resources_per_actor,
            "num_actors": int(num_actors),
        }
    )

    # This should be upstreamed to xgboost_ray,
    # but also left here for backwards compatibility.
    if not hasattr(ray_params_cls, "placement_options"):

        @dataclass
        class RayParamsFromScalingConfig(ray_params_cls):
            # Passed as kwargs to PlacementGroupFactory
            placement_options: Dict[str, Any] = None

            def get_tune_resources(self) -> PlacementGroupFactory:
                pgf = super().get_tune_resources()
                placement_options = self.placement_options.copy()
                extended_pgf = PlacementGroupFactory(
                    pgf.bundles,
                    **placement_options,
                )
                extended_pgf._head_bundle_is_empty = pgf._head_bundle_is_empty
                return extended_pgf

        ray_params_cls_extended = RayParamsFromScalingConfig
    else:
        ray_params_cls_extended = ray_params_cls

    placement_options = {
        "strategy": scaling_config.placement_strategy,
    }
    ray_params = ray_params_cls_extended(
        placement_options=placement_options,
        **ray_params_kwargs,
    )

    return ray_params


@DeveloperAPI
class GBDTTrainer(BaseTrainer):
    """Abstract class for scaling gradient-boosting decision tree (GBDT) frameworks.

    Inherited by XGBoostTrainer and LightGBMTrainer.

    Args:
        datasets: Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset.
            All non-training datasets will be used as separate
            validation sets, each reporting a separate metric.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        params: Framework specific training parameters.
        dmatrix_params: Dict of ``dataset name:dict of kwargs`` passed to respective
            :class:`xgboost_ray.RayDMatrix` initializations.
        num_boost_round: Target number of boosting iterations (trees in the model).
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        resume_from_checkpoint: A checkpoint to resume training from.
        metadata: Dict that should be made available in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        **train_kwargs: Additional kwargs passed to framework ``train()`` function.
    """

    _scaling_config_allowed_keys = BaseTrainer._scaling_config_allowed_keys + [
        "num_workers",
        "resources_per_worker",
        "use_gpu",
        "placement_strategy",
    ]
    _handles_checkpoint_freq = True
    _handles_checkpoint_at_end = True

    _dmatrix_cls: type
    _ray_params_cls: type
    _tune_callback_checkpoint_cls: type
    _default_ray_params: Dict[str, Any] = {
        "checkpoint_frequency": 1,
        "checkpoint_at_end": True,
    }
    _init_model_arg_name: str
    _num_iterations_argument: str = "num_boost_round"
    _default_num_iterations: int = _DEFAULT_NUM_ITERATIONS

    def __init__(
        self,
        *,
        datasets: Dict[str, GenDataset],
        label_column: str,
        params: Dict[str, Any],
        dmatrix_params: Optional[Dict[str, Dict[str, Any]]] = None,
        num_boost_round: int = _DEFAULT_NUM_ITERATIONS,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        preprocessor: Optional["Preprocessor"] = None,  # Deprecated
        resume_from_checkpoint: Optional[Checkpoint] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **train_kwargs,
    ):
        self.label_column = label_column
        self.params = params

        self.num_boost_round = num_boost_round
        self.train_kwargs = train_kwargs
        self.dmatrix_params = dmatrix_params or {}

        super().__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )

        # Datasets should always use distributed loading.
        for dataset_name in self.datasets.keys():
            dataset_params = self.dmatrix_params.get(dataset_name, {})
            dataset_params["distributed"] = True
            self.dmatrix_params[dataset_name] = dataset_params

    def _validate_attributes(self):
        super()._validate_attributes()
        self._validate_config_and_datasets()

    def _validate_config_and_datasets(self) -> None:
        if TRAIN_DATASET_KEY not in self.datasets:
            raise KeyError(
                f"'{TRAIN_DATASET_KEY}' key must be preset in `datasets`. "
                f"Got {list(self.datasets.keys())}"
            )
        if self.dmatrix_params:
            for key in self.dmatrix_params:
                if key not in self.datasets:
                    raise KeyError(
                        f"`dmatrix_params` dict contains key '{key}' "
                        f"which is not present in `datasets`."
                    )

    def _get_dmatrices(
        self, dmatrix_params: Dict[str, Any]
    ) -> Dict[str, "xgboost_ray.RayDMatrix"]:
        return {
            k: self._dmatrix_cls(
                v, label=self.label_column, **dmatrix_params.get(k, {})
            )
            for k, v in self.datasets.items()
        }

    def _load_checkpoint(
        self,
        checkpoint: Checkpoint,
    ) -> Any:
        raise NotImplementedError

    def _train(self, **kwargs):
        raise NotImplementedError

    def _save_model(self, model: Any, path: str):
        raise NotImplementedError

    def _model_iteration(self, model: Any) -> int:
        raise NotImplementedError

    @property
    def _ray_params(self) -> "xgboost_ray.RayParams":
        scaling_config_dataclass = self._validate_scaling_config(self.scaling_config)
        return _convert_scaling_config_to_ray_params(
            scaling_config_dataclass, self._ray_params_cls, self._default_ray_params
        )

    def _repartition_datasets_to_match_num_actors(self):
        # XGBoost/LightGBM-Ray requires each dataset to have at least as many
        # blocks as there are workers.
        # This is only applicable for xgboost-ray<0.1.16. The version check
        # is done in subclasses to ensure that xgboost-ray doesn't need to be
        # imported here.
        for dataset_key, dataset in self.datasets.items():
            if dataset.num_blocks() < self._ray_params.num_actors:
                if dataset.size_bytes() > _WARN_REPARTITION_THRESHOLD:
                    warnings.warn(
                        f"Dataset '{dataset_key}' has {dataset.num_blocks()} blocks, "
                        f"which is less than the `num_workers` "
                        f"{self._ray_params.num_actors}. "
                        f"This dataset will be automatically repartitioned to "
                        f"{self._ray_params.num_actors} blocks. You can disable "
                        "this error message by partitioning the dataset "
                        "to have blocks >= number of workers via "
                        "`dataset.repartition(num_workers)`."
                    )
                self.datasets[dataset_key] = dataset.repartition(
                    self._ray_params.num_actors
                )

    def _checkpoint_at_end(self, model, evals_result: dict) -> None:
        # We need to call session.report to save checkpoints, so we report
        # the last received metrics (possibly again).
        result_dict = flatten_dict(evals_result, delimiter="-")
        for k in list(result_dict):
            result_dict[k] = result_dict[k][-1]

        if getattr(self._tune_callback_checkpoint_cls, "_report_callbacks_cls", None):
            # Deprecate: Remove in Ray 2.8
            with tune.checkpoint_dir(step=self._model_iteration(model)) as cp_dir:
                self._save_model(model, path=os.path.join(cp_dir, MODEL_KEY))
            tune.report(**result_dict)
        else:
            with tempfile.TemporaryDirectory() as checkpoint_dir:
                self._save_model(model, path=checkpoint_dir)
                checkpoint = Checkpoint.from_directory(checkpoint_dir)
                train.report(result_dict, checkpoint=checkpoint)

    def training_loop(self) -> None:
        config = self.train_kwargs.copy()
        config[self._num_iterations_argument] = self.num_boost_round

        dmatrices = self._get_dmatrices(
            dmatrix_params=self.dmatrix_params,
        )
        train_dmatrix = dmatrices[TRAIN_DATASET_KEY]
        evals_result = {}

        init_model = None
        if self.starting_checkpoint:
            init_model = self._load_checkpoint(self.starting_checkpoint)

        config.setdefault("verbose_eval", False)
        config.setdefault("callbacks", [])

        if not any(
            isinstance(cb, self._tune_callback_checkpoint_cls)
            for cb in config["callbacks"]
        ):
            # Only add our own callback if it hasn't been added before
            checkpoint_frequency = (
                self.run_config.checkpoint_config.checkpoint_frequency
            )
            callback = self._tune_callback_checkpoint_cls(
                filename=MODEL_KEY, frequency=checkpoint_frequency
            )

            config["callbacks"] += [callback]

        config[self._init_model_arg_name] = init_model

        if init_model:
            # If restoring, make sure that we only create num_boosting_round trees,
            # and not init_model_trees + num_boosting_round trees
            last_iteration = self._model_iteration(init_model)
            num_iterations = config.get(
                self._num_iterations_argument, self._default_num_iterations
            )
            new_iterations = num_iterations - last_iteration
            config[self._num_iterations_argument] = new_iterations
            logger.warning(
                f"Model loaded from checkpoint will train for "
                f"additional {new_iterations} iterations (trees) in order "
                "to achieve the target number of iterations "
                f"({self._num_iterations_argument}={num_iterations})."
            )

        model = self._train(
            params=self.params,
            dtrain=train_dmatrix,
            evals_result=evals_result,
            evals=[(dmatrix, k) for k, dmatrix in dmatrices.items()],
            ray_params=self._ray_params,
            **config,
        )

        checkpoint_at_end = self.run_config.checkpoint_config.checkpoint_at_end
        if checkpoint_at_end is None:
            checkpoint_at_end = True

        if checkpoint_at_end:
            self._checkpoint_at_end(model, evals_result)

    def _generate_trainable_cls(self) -> Type["Trainable"]:
        trainable_cls = super()._generate_trainable_cls()
        trainer_cls = self.__class__
        scaling_config = self.scaling_config
        ray_params_cls = self._ray_params_cls
        default_ray_params = self._default_ray_params

        class GBDTTrainable(trainable_cls):
            @classmethod
            def default_resource_request(cls, config):
                # `config["scaling_config"] is a dataclass when passed via the
                # `scaling_config` argument in `Trainer` and is a dict when passed
                # via the `scaling_config` key of `param_spec`.
                updated_scaling_config = config.get("scaling_config", scaling_config)
                if isinstance(updated_scaling_config, dict):
                    updated_scaling_config = ScalingConfig(**updated_scaling_config)
                validated_scaling_config = trainer_cls._validate_scaling_config(
                    updated_scaling_config
                )
                return _convert_scaling_config_to_ray_params(
                    validated_scaling_config, ray_params_cls, default_ray_params
                ).get_tune_resources()

        return GBDTTrainable
