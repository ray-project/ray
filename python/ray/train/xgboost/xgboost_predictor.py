from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import pandas as pd
import xgboost

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.data_batch_type import DataBatchType
from ray.air.util.data_batch_conversion import _unwrap_ndarray_object_type_if_needed
from ray.train.predictor import Predictor
from ray.train.xgboost import XGBoostCheckpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
class XGBoostPredictor(Predictor):
    """A predictor for XGBoost models.

    Args:
        model: The XGBoost booster to use for predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self, model: xgboost.Booster, preprocessor: Optional["Preprocessor"] = None
    ):
        self.model = model
        super().__init__(preprocessor)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(model={self.model!r}, "
            f"preprocessor={self._preprocessor!r})"
        )

    @classmethod
    def from_checkpoint(cls, checkpoint: XGBoostCheckpoint) -> "XGBoostPredictor":
        """Instantiate the predictor from a Checkpoint.

        This is a helper constructor that instantiates the predictor from a
        framework-specific XGBoost checkpoint.

        Args:
            checkpoint: The checkpoint to load the model and preprocessor from.

        """
        model = checkpoint.get_model()
        preprocessor = checkpoint.get_preprocessor()
        return cls(model=model, preprocessor=preprocessor)

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        dmatrix_kwargs: Optional[Dict[str, Any]] = None,
        **predict_kwargs,
    ) -> DataBatchType:
        """Run inference on data batch.

        The data is converted into an XGBoost DMatrix before being inputted to
        the model.

        Args:
            data: A batch of input data.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, then use
                all columns in ``data``.
            dmatrix_kwargs: Dict of keyword arguments passed to ``xgboost.DMatrix``.
            **predict_kwargs: Keyword arguments passed to ``xgboost.Booster.predict``.


        Examples:

        .. testcode::

            import numpy as np
            import xgboost as xgb
            from ray.train.xgboost import XGBoostPredictor
            train_X = np.array([[1, 2], [3, 4]])
            train_y = np.array([0, 1])
            model = xgb.XGBClassifier().fit(train_X, train_y)
            predictor = XGBoostPredictor(model=model.get_booster())
            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data)
            # Only use first and second column as the feature
            data = np.array([[1, 2, 8], [3, 4, 9]])
            predictions = predictor.predict(data, feature_columns=[0, 1])

        .. testcode::

            import pandas as pd
            import xgboost as xgb
            from ray.train.xgboost import XGBoostPredictor
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
            Prediction result.

        """
        return Predictor.predict(
            self,
            data,
            feature_columns=feature_columns,
            dmatrix_kwargs=dmatrix_kwargs,
            **predict_kwargs,
        )

    def _predict_pandas(
        self,
        data: "pd.DataFrame",
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        dmatrix_kwargs: Optional[Dict[str, Any]] = None,
        **predict_kwargs,
    ) -> "pd.DataFrame":
        dmatrix_kwargs = dmatrix_kwargs or {}

        feature_names = None
        if TENSOR_COLUMN_NAME in data:
            data = data[TENSOR_COLUMN_NAME].to_numpy()
            data = _unwrap_ndarray_object_type_if_needed(data)
            if feature_columns:
                # In this case feature_columns is a list of integers
                data = data[:, feature_columns]
        elif feature_columns:
            # feature_columns is a list of integers or strings
            data = data[feature_columns].to_numpy()
            # Only set the feature names if they are strings
            if all(isinstance(fc, str) for fc in feature_columns):
                feature_names = feature_columns
        else:
            feature_columns = data.columns.tolist()
            data = data.to_numpy()

            if all(isinstance(fc, str) for fc in feature_columns):
                feature_names = feature_columns

        if feature_names:
            dmatrix_kwargs["feature_names"] = feature_names

        matrix = xgboost.DMatrix(data, **dmatrix_kwargs)
        df = pd.DataFrame(self.model.predict(matrix, **predict_kwargs))
        df.columns = (
            ["predictions"]
            if len(df.columns) == 1
            else [f"predictions_{i}" for i in range(len(df.columns))]
        )
        return df
