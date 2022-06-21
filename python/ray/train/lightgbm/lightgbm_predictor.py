from typing import TYPE_CHECKING, List, Optional, Union

import lightgbm
import pandas as pd

from ray.air.checkpoint import Checkpoint
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.train.lightgbm.utils import load_checkpoint
from ray.train.predictor import Predictor

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


class LightGBMPredictor(Predictor):
    """A predictor for LightGBM models.

    Args:
        model: The LightGBM booster to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self, model: lightgbm.Booster, preprocessor: Optional["Preprocessor"] = None
    ):
        self.model = model
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint) -> "LightGBMPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``LightGBMTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``LightGBMTrainer`` run.

        """
        bst, preprocessor = load_checkpoint(checkpoint)
        return LightGBMPredictor(model=bst, preprocessor=preprocessor)

    def _predict_pandas(
        self,
        data: "pd.DataFrame",
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        **predict_kwargs,
    ) -> pd.DataFrame:
        """Run inference on data batch.

        Args:
            data: A batch of input data.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, then use
                all columns in ``data``.
            **predict_kwargs: Keyword arguments passed to
                ``lightgbm.Booster.predict``.

        Examples:

        .. code-block:: python

            import numpy as np
            import lightgbm as lgbm
            from ray.train.predictors.lightgbm import LightGBMPredictor

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
            from ray.train.predictors.lightgbm import LightGBMPredictor

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
            Prediction result.

        """
        if TENSOR_COLUMN_NAME in data:
            data = data[TENSOR_COLUMN_NAME].to_numpy()

            if feature_columns:
                data = data[:, feature_columns]
        elif feature_columns:
            data = data[feature_columns]

        df = pd.DataFrame(self.model.predict(data, **predict_kwargs))
        df.columns = (
            ["predictions"]
            if len(df.columns) == 1
            else [f"predictions_{i}" for i in range(len(df.columns))]
        )
        return df
