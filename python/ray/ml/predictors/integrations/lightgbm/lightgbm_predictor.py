from typing import Optional, List, Union

import lightgbm

from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.preprocessor import Preprocessor


class LightGBMPredictor(Predictor):
    """A predictor for LightGBM models.

    Args:
        model: The LightGBM booster to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(self, model: lightgbm.Booster, preprocessor: Preprocessor):
        raise NotImplementedError

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint) -> "LightGBMPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``LightGBMTrainer``.

        Args:
            checkpoint (Checkpoint): The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``LightGBMTrainer`` run.

        """
        raise NotImplementedError

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        **lgbm_dataset_kwargs,
    ) -> DataBatchType:
        """Run inference on data batch.

        The data is converted into a LightGBM Dataset before being inputted to
        the model.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, then use
                all columns in ``data``.
            **lgbm_dataset_kwargs: Keyword arguments passed to
                ``lightgbm.Dataset``.

        Examples:

        .. code-block:: python

            import numpy as np
            import lightgbm as lgbm
            from ray.ml.predictors.lightgbm import LightGBMPredictor

            train_X = np.array([[1, 2], [3, 4]])
            train_y = np.array([0, 1])

            model = lgbm.LGBMClassifier().fit(train_X, train_y)
            predictor = LightGBMPredictor(model=model.booster_)

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data)

            # Only use first and second column as the feature
            data = np.array([[1, 2, 8], [3, 4, 9]])
            predictions = predictor.predict(data, feature_columns=[0, 1])

        .. code-block:: python

            import pandas as pd
            import lightgbm as lgbm
            from ray.ml.predictors.lightgbm import LightGBMPredictor

            train_X = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
            train_y = pd.Series([0, 1])

            model = lgbm.LGBMClassifier().fit(train_X, train_y)
            predictor = LightGBMPredictor(model=model.booster_)

            # Pandas dataframe.
            data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
            predictions = predictor.predict(data)

            # Only use first and second column as the feature
            data = pd.DataFrame([[1, 2, 8], [3, 4, 9]], columns=["A", "B", "C"])
            predictions = predictor.predict(data, feature_columns=["A", "B"])


        Returns:
            DataBatchType: Prediction result.

        """
        raise NotImplementedError
