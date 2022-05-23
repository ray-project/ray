from typing import TYPE_CHECKING, Dict, Tuple, Type, Any, Optional
import warnings

from ray.ml.trainer import GenDataset
from ray.ml.config import ScalingConfig, RunConfig, ScalingConfigDataClass
from ray.ml.preprocessor import Preprocessor
from ray.ml.utils.checkpointing import save_preprocessor_to_dir
from ray.tune.utils.trainable import TrainableUtil
from ray.util.annotations import DeveloperAPI
from ray.ml.trainer import Trainer
from ray.ml.checkpoint import Checkpoint
from ray.tune import Trainable
from ray.ml.constants import MODEL_KEY, TRAIN_DATASET_KEY

if TYPE_CHECKING:
    import xgboost_ray


def _convert_scaling_config_to_ray_params(
    scaling_config: ScalingConfig,
    ray_params_cls: Type["xgboost_ray.RayParams"],
    default_ray_params: Optional[Dict[str, Any]] = None,
) -> "xgboost_ray.RayParams":
    default_ray_params = default_ray_params or {}
    scaling_config_dataclass = ScalingConfigDataClass(**scaling_config)
    resources_per_worker = scaling_config_dataclass.additional_resources_per_worker
    num_workers = scaling_config_dataclass.num_workers
    cpus_per_worker = scaling_config_dataclass.num_cpus_per_worker
    gpus_per_worker = scaling_config_dataclass.num_gpus_per_worker

    ray_params = ray_params_cls(
        num_actors=int(num_workers),
        cpus_per_actor=int(cpus_per_worker),
        gpus_per_actor=int(gpus_per_worker),
        resources_per_actor=resources_per_worker,
        **default_ray_params,
    )

    return ray_params


@DeveloperAPI
class GBDTTrainer(Trainer):
    """Common logic for gradient-boosting decision tree (GBDT) frameworks
    like XGBoost-Ray and LightGBM-Ray.


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
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
        **train_kwargs: Additional kwargs passed to framework ``train()`` function.
    """

    _scaling_config_allowed_keys = Trainer._scaling_config_allowed_keys + [
        "num_workers",
        "num_cpus_per_worker",
        "num_gpus_per_worker",
        "additional_resources_per_worker",
        "use_gpu",
    ]
    _dmatrix_cls: type
    _ray_params_cls: type
    _tune_callback_cls: type
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
        preprocessor: Optional[Preprocessor] = None,
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
    ) -> Tuple[Any, Optional[Preprocessor]]:
        raise NotImplementedError

    def _train(self, **kwargs):
        raise NotImplementedError

    @property
    def _ray_params(self) -> "xgboost_ray.RayParams":
        return _convert_scaling_config_to_ray_params(
            self.scaling_config, self._ray_params_cls, self._default_ray_params
        )

    def preprocess_datasets(self) -> None:
        super().preprocess_datasets()

        # XGBoost/LightGBM-Ray requires each dataset to have at least as many
        # blocks as there are workers.
        # TODO: Move this logic to the respective libraries
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
        config["callbacks"] += [
            self._tune_callback_cls(filename=MODEL_KEY, frequency=1)
        ]
        config[self._init_model_arg_name] = init_model

        self._train(
            params=self.params,
            dtrain=train_dmatrix,
            evals_result=evals_result,
            evals=[(dmatrix, k) for k, dmatrix in dmatrices.items()],
            ray_params=self._ray_params,
            **config,
        )

    def as_trainable(self) -> Type[Trainable]:
        trainable_cls = super().as_trainable()
        scaling_config = self.scaling_config
        ray_params_cls = self._ray_params_cls
        default_ray_params = self._default_ray_params

        class GBDTTrainable(trainable_cls):
            def save_checkpoint(self, tmp_checkpoint_dir: str = ""):
                checkpoint_path = super().save_checkpoint()
                parent_dir = TrainableUtil.find_checkpoint_dir(checkpoint_path)

                preprocessor = self._merged_config.get("preprocessor", None)
                if parent_dir and preprocessor:
                    save_preprocessor_to_dir(preprocessor, parent_dir)
                return checkpoint_path

            @classmethod
            def default_resource_request(cls, config):
                updated_scaling_config = config.get("scaling_config", scaling_config)
                return _convert_scaling_config_to_ray_params(
                    updated_scaling_config, ray_params_cls, default_ray_params
                ).get_tune_resources()

        return GBDTTrainable
