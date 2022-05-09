from typing import Optional, List, Union
import numpy as np
import pandas as pd

import lightgbm

from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.preprocessor import Preprocessor
from ray.ml.train.integrations.lightgbm import load_checkpoint


class LightGBMPredictor(Predictor):
    """A predictor for LightGBM models.

    Args:
        model: The LightGBM booster to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self, model: lightgbm.Booster, preprocessor: Optional[Preprocessor] = None
    ):
        self.model = model
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint) -> "LightGBMPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``LightGBMTrainer``.

        Args:
            checkpoint (Checkpoint): The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``LightGBMTrainer`` run.

        """
        bst, preprocessor = load_checkpoint(checkpoint)
        return LightGBMPredictor(model=bst, preprocessor=preprocessor)

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        **predict_kwargs,
    ) -> pd.DataFrame:
        """Run inference on data batch.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, then use
                all columns in ``data``.
            **predict_kwargs: Keyword arguments passed to
                ``lightgbm.Booster.predict``.

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
            pd.DataFrame: Prediction result.

        """

        if self.preprocessor:
            data = self.preprocessor.transform_batch(data)

        if feature_columns:
            if isinstance(data, np.ndarray):
                data = data[:, feature_columns]
            else:
                data = data[feature_columns]
        df = pd.DataFrame(self.model.predict(data, **predict_kwargs))
        df.columns = (
            ["predictions"]
            if len(df.columns) == 1
            else [f"predictions_{i}" for i in range(len(df.columns))]
        )
        return df
