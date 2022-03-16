from typing import Optional, Dict, Any
import os

from ray.ml.trainer import GenDataset
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.utils.gbdt_utils import GBDTTrainer, PARAMS_KEY, DMATRIX_PARAMS_KEY
from ray.util.annotations import PublicAPI
from ray.ml.constants import MODEL_KEY, TRAIN_DATASET_KEY

import lightgbm
import lightgbm_ray
from lightgbm_ray.tune import TuneReportCheckpointCallback


@PublicAPI(stability="alpha")
class LightGBMTrainer(GBDTTrainer):
    """A Trainer for data parallel LightGBM training.

    This Trainer runs the LightGBM training loop in a distributed manner
    using multiple Ray Actors.

    Example:
        .. code-block:: python

            import ray

            from ray.ml.train.integrations.lightgbm import LightGBMTrainer

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = LightGBMTrainer(
                label_column="y",
                lightgbm_config={"objective": "regression"},
                scaling_config={"num_workers": 3},
                datasets={"train": train_dataset}
            )
            result = trainer.fit()


    Args:
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        datasets: Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. If a ``preprocessor``
            is provided and has not already been fit, it will be fit on the training
            dataset. All datasets will be transformed by the ``preprocessor`` if
            one is provided. All non-training datasets will be used as separate
            validation sets, each reporting a separate metric.
        lightgbm_config: LightGBM training parameters passed to ``lightgbm.train()``.
            Refer to `LightGBM documentation <https://lightgbm.readthedocs.io>`_
            for a list of possible parameters. Must contain a "params" key.
            You can also pass a "dmatrix_params" key, which should be a dict
            of ``dataset name:dict of kwargs`` passed to respective
            :class:`xgboost_ray.RayDMatrix` initializations.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    _use_lightgbm: bool = True

    def __init__(
        self,
        label_column: str,
        datasets: Dict[str, GenDataset],
        lightgbm_config: Dict[str, Any],
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if TRAIN_DATASET_KEY not in datasets:
            raise KeyError(
                f"'{TRAIN_DATASET_KEY}' key must be preset in `datasets`. "
                f"Got {list(datasets.keys())}"
            )
        if PARAMS_KEY not in lightgbm_config:
            raise KeyError(
                f"`lightgbm_config` must contain a '{PARAMS_KEY}' key. "
                f"Got {list(lightgbm_config.keys())}"
            )
        self._validate_dmatrix_params(lightgbm_config, datasets)
        self.label_column = label_column
        self.lightgbm_config = lightgbm_config
        super().__init__(
            datasets=datasets,
            scaling_config=scaling_config,
            run_config=run_config,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def training_loop(self) -> None:
        lightgbm_config = self.lightgbm_config.copy()

        dmatrices = self._get_dmatrices(
            dmatrix_params=lightgbm_config.pop(DMATRIX_PARAMS_KEY, {})
        )
        train_dmatrix = dmatrices[TRAIN_DATASET_KEY]
        evals_result = {}

        init_model = None
        if self.resume_from_checkpoint:
            init_model_path = self.resume_from_checkpoint.to_directory()
            init_model = lightgbm.Booster(
                model_file=os.path.join(init_model_path, MODEL_KEY)
            )

        lightgbm_config.setdefault("callbacks", [])
        lightgbm_config["callbacks"] += [
            TuneReportCheckpointCallback(filename=MODEL_KEY, frequency=1)
        ]

        lightgbm_ray.train(
            dtrain=train_dmatrix,
            evals_result=evals_result,
            evals=[(dmatrix, k) for k, dmatrix in dmatrices.items()],
            ray_params=self._ray_params,
            init_model=init_model,
            **lightgbm_config,
        )
