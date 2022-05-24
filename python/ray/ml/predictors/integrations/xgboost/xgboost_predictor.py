from typing import Optional, List, Union, Dict, Any
import numpy as np
import pandas as pd

import xgboost

from ray.ml.checkpoint import Checkpoint
from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.preprocessor import Preprocessor
from ray.ml.train.integrations.xgboost import load_checkpoint


class XGBoostPredictor(Predictor):
    """A predictor for XGBoost models.

    Args:
        model: The XGBoost booster to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self, model: xgboost.Booster, preprocessor: Optional[Preprocessor] = None
    ):
        self.model = model
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint) -> "XGBoostPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``XGBoostTrainer``.

        Args:
            checkpoint (Checkpoint): The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``XGBoostTrainer`` run.

        """
        bst, preprocessor = load_checkpoint(checkpoint)
        return XGBoostPredictor(model=bst, preprocessor=preprocessor)

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        dmatrix_kwargs: Optional[Dict[str, Any]] = None,
        **predict_kwargs,
    ) -> pd.DataFrame:
        """Run inference on data batch.

        The data is converted into an XGBoost DMatrix before being inputted to
        the model.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, then use
                all columns in ``data``.
            dmatrix_kwargs: Dict of keyword arguments passed to ``xgboost.DMatrix``.
            **predict_kwargs: Keyword arguments passed to ``xgboost.Booster.predict``.

        Examples:

        .. code-block:: python

            import numpy as np
            import xgboost as xgb
            from ray.ml.predictors.xgboost import XGBoostPredictor

            train_X = np.array([[1, 2], [3, 4]])
            train_y = np.array([0, 1])

            model = xgb.XGBClassifier().fit(train_X, train_y)
            predictor = XGBoostPredictor(model=model.get_booster())

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data)

            # Only use first and second column as the feature
            data = np.array([[1, 2, 8], [3, 4, 9]])
            predictions = predictor.predict(data, feature_columns=[0, 1])

        .. code-block:: python

            import pandas as pd
            import xgboost as xgb
            from ray.ml.predictors.xgboost import XGBoostPredictor

            train_X = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
            train_y = pd.Series([0, 1])

            model = xgb.XGBClassifier().fit(train_X, train_y)
            predictor = XGBoostPredictor(model=model.get_booster())

            # Pandas dataframe.
            data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
            predictions = predictor.predict(data)

            # Only use first and second column as the feature
            data = pd.DataFrame([[1, 2, 8], [3, 4, 9]], columns=["A", "B", "C"])
            predictions = predictor.predict(data, feature_columns=["A", "B"])


        Returns:
            pd.DataFrame: Prediction result.

        """
        dmatrix_kwargs = dmatrix_kwargs or {}

        if self.preprocessor:
            data = self.preprocessor.transform_batch(data)

        if feature_columns:
            if isinstance(data, np.ndarray):
                data = data[:, feature_columns]
            else:
                data = data[feature_columns]
        matrix = xgboost.DMatrix(data, **dmatrix_kwargs)
        df = pd.DataFrame(self.model.predict(matrix, **predict_kwargs))
        df.columns = (
            ["predictions"]
            if len(df.columns) == 1
            else [f"predictions_{i}" for i in range(len(df.columns))]
        )
        return df
