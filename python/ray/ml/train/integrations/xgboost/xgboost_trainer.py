import os

from ray.ml.train.gbdt_trainer import GBDTTrainer
from ray.util.annotations import PublicAPI
from ray.ml.constants import MODEL_KEY

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
                params={"objective": "reg:squarederror"},
                scaling_config={"num_workers": 3},
                datasets={"train": train_dataset}
            )
            result = trainer.fit()

    Args:
        datasets: Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. If a ``preprocessor``
            is provided and has not already been fit, it will be fit on the training
            dataset. All datasets will be transformed by the ``preprocessor`` if
            one is provided. All non-training datasets will be used as separate
            validation sets, each reporting a separate metric.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        params: XGBoost training parameters.
            Refer to `XGBoost documentation <https://xgboost.readthedocs.io/>`_
            for a list of possible parameters.
        dmatrix_params: Dict of ``dataset name:dict of kwargs`` passed to respective
            :class:`xgboost_ray.RayDMatrix` initializations, which in turn are passed
            to ``xgboost.DMatrix`` objects created on each worker. For example, this can
            be used to add sample weights with the ``weights`` parameter.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
        **train_kwargs: Additional kwargs passed to ``xgboost.train()`` function.
    """

    _dmatrix_cls: type = xgboost_ray.RayDMatrix
    _ray_params_cls: type = xgboost_ray.RayParams
    _tune_callback_cls: type = TuneReportCheckpointCallback
    _init_model_arg_name: str = "xgb_model"

    def _load_model_from_checkpoint(self):
        xgb_model_path = self.resume_from_checkpoint.to_directory()
        xgb_model = xgboost.Booster()
        xgb_model.load_model(os.path.join(xgb_model_path, MODEL_KEY))
        return xgb_model

    def _train(self, **kwargs):
        return xgboost_ray.train(**kwargs)
