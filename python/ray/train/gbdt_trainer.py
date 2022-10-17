import os
import warnings
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Type

from ray import tune
from ray.air._internal.checkpointing import save_preprocessor_to_dir
from ray.air.checkpoint import Checkpoint
from ray.air.config import RunConfig, ScalingConfig
from ray.train.constants import MODEL_KEY, TRAIN_DATASET_KEY
from ray.train.trainer import BaseTrainer, GenDataset
from ray.tune import Trainable
from ray.tune.trainable.util import TrainableUtil
from ray.util.annotations import DeveloperAPI
from ray._private.dict import flatten_dict

if TYPE_CHECKING:
    import xgboost_ray

    from ray.data.preprocessor import Preprocessor

_WARN_REPARTITION_THRESHOLD = 10 * 1024 ** 3


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
    ray_params = ray_params_cls(
        **ray_params_kwargs,
    )

    return ray_params


@DeveloperAPI
class GBDTTrainer(BaseTrainer):
    """Abstract class for scaling gradient-boosting decision tree (GBDT) frameworks.

    Inherited by XGBoostTrainer and LightGBMTrainer.

    Args:
        datasets: Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. If a ``preprocessor``
            is provided and has not already been fit, it will be fit on the training
            dataset. All datasets will be transformed by the ``preprocessor`` if
            one is provided. All non-training datasets will be used as separate
            validation sets, each reporting a separate metric.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        params: Framework specific training parameters.
        dmatrix_params: Dict of ``dataset name:dict of kwargs`` passed to respective
            :class:`xgboost_ray.RayDMatrix` initializations.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.data.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
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
    _tune_callback_report_cls: type
    _tune_callback_checkpoint_cls: type
    _default_ray_params: Dict[str, Any] = {"checkpoint_frequency": 1}
    _init_model_arg_name: str

    def __init__(
        self,
        *,
        datasets: Dict[str, GenDataset],
        label_column: str,
        params: Dict[str, Any],
        dmatrix_params: Optional[Dict[str, Dict[str, Any]]] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        **train_kwargs,
    ):
        self.label_column = label_column
        self.params = params
        self.dmatrix_params = dmatrix_params or {}
        self.train_kwargs = train_kwargs
        super().__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

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
    ) -> Tuple[Any, Optional["Preprocessor"]]:
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

    def preprocess_datasets(self) -> None:
        super().preprocess_datasets()

        # XGBoost/LightGBM-Ray requires each dataset to have at least as many
        # blocks as there are workers.
        # TODO: Move this logic to the respective libraries
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

        with tune.checkpoint_dir(step=self._model_iteration(model)) as cp_dir:
            self._save_model(model, path=os.path.join(cp_dir, MODEL_KEY))
        tune.report(**result_dict)

    def training_loop(self) -> None:
        config = self.train_kwargs.copy()

        dmatrices = self._get_dmatrices(
            dmatrix_params=self.dmatrix_params,
        )
        train_dmatrix = dmatrices[TRAIN_DATASET_KEY]
        evals_result = {}

        init_model = None
        if self.resume_from_checkpoint:
            init_model, _ = self._load_checkpoint(self.resume_from_checkpoint)

        config.setdefault("verbose_eval", False)
        config.setdefault("callbacks", [])

        if not any(
            isinstance(
                cb, (self._tune_callback_report_cls, self._tune_callback_checkpoint_cls)
            )
            for cb in config["callbacks"]
        ):
            # Only add our own callback if it hasn't been added before
            checkpoint_frequency = (
                self.run_config.checkpoint_config.checkpoint_frequency
            )
            if checkpoint_frequency > 0:
                callback = self._tune_callback_checkpoint_cls(
                    filename=MODEL_KEY, frequency=checkpoint_frequency
                )
            else:
                callback = self._tune_callback_report_cls()

            config["callbacks"] += [callback]

        config[self._init_model_arg_name] = init_model

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
            # Workaround for actor name not being logged correctly
            # if __repr__ is not directly defined in a class.
            def __repr__(self):
                return super().__repr__()

            def save_checkpoint(self, tmp_checkpoint_dir: str = ""):
                checkpoint_path = super().save_checkpoint()
                parent_dir = TrainableUtil.find_checkpoint_dir(checkpoint_path)

                preprocessor = self._merged_config.get("preprocessor", None)
                if parent_dir and preprocessor:
                    save_preprocessor_to_dir(preprocessor, parent_dir)
                return checkpoint_path

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
