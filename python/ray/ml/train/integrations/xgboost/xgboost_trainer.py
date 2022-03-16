from typing import Optional, Dict, Any
import os

from ray.ml.trainer import GenDataset
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.checkpoint import Checkpoint
from ray.ml.utils.gbdt_utils import DMATRIX_PARAMS_KEY, PARAMS_KEY, GBDTTrainer
from ray.util.annotations import PublicAPI
from ray.ml.constants import MODEL_KEY, TRAIN_DATASET_KEY

import xgboost
import xgboost_ray
from xgboost_ray.tune import TuneReportCheckpointCallback


@PublicAPI(stability="alpha")
class XGBoostTrainer(GBDTTrainer):
    """A Trainer for data parallel XGBoost training.

    This Trainer runs the XGBoost training loop in a distributed manner
    using multiple Ray Actors.

    Example:
        .. code-block:: python

            import ray

            from ray.ml.train.integrations.xgboost import XGBoostTrainer

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = XGBoostTrainer(
                label_column="y",
                xgboost_config={
                    "params": {"objective": "reg:squarederror"}
                },
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
        xgboost_config: XGBoost training parameters passed to ``xgboost.train()``.
            Refer to `XGBoost documentation <https://xgboost.readthedocs.io>`_
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

    _use_lightgbm: bool = False

    def __init__(
        self,
        label_column: str,
        datasets: Dict[str, GenDataset],
        xgboost_config: Dict[str, Any],
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
        if PARAMS_KEY not in xgboost_config:
            raise KeyError(
                f"`xgboost_config` must contain a '{PARAMS_KEY}' key. "
                f"Got {list(xgboost_config.keys())}"
            )
        self._validate_dmatrix_params(xgboost_config, datasets)
        self.label_column = label_column
        self.xgboost_config = xgboost_config
        super().__init__(
            datasets=datasets,
            scaling_config=scaling_config,
            run_config=run_config,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def training_loop(self) -> None:
        xgboost_config = self.xgboost_config.copy()

        dmatrices = self._get_dmatrices(
            dmatrix_params=xgboost_config.pop(DMATRIX_PARAMS_KEY, {})
        )
        train_dmatrix = dmatrices[TRAIN_DATASET_KEY]
        evals_result = {}

        xgb_model = None
        if self.resume_from_checkpoint:
            xgb_model_path = self.resume_from_checkpoint.to_directory()
            xgb_model = xgboost.Booster()
            xgb_model.load_model(os.path.join(xgb_model_path, MODEL_KEY))

        xgboost_config.setdefault("verbose_eval", False)
        xgboost_config.setdefault("callbacks", [])
        xgboost_config["callbacks"] += [
            TuneReportCheckpointCallback(filename=MODEL_KEY, frequency=1)
        ]

        xgboost_ray.train(
            dtrain=train_dmatrix,
            evals_result=evals_result,
            evals=[(dmatrix, k) for k, dmatrix in dmatrices.items()],
            ray_params=self._ray_params,
            xgb_model=xgb_model,
            **xgboost_config,
        )
