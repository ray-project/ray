import numpy as np
from typing import TYPE_CHECKING, List, Optional, Union

import lightgbm
import pandas as pd

from ray.air.checkpoint import Checkpoint
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.data_batch_conversion import _unwrap_ndarray_object_type_if_needed
from ray.train.lightgbm.lightgbm_checkpoint import LightGBMCheckpoint
from ray.train.predictor import Predictor
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="beta")
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
        super().__init__(preprocessor)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(model={self.model!r}, "
            f"preprocessor={self._preprocessor!r})"
        )

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint) -> "LightGBMPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``LightGBMTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``LightGBMTrainer`` run.

        """
        checkpoint = LightGBMCheckpoint.from_checkpoint(checkpoint)
        model = checkpoint.get_model()
        preprocessor = checkpoint.get_preprocessor()
        return cls(model=model, preprocessor=preprocessor)

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

        # Turn into dataframe to make dtype resolution easy
        data = pd.DataFrame(data, columns=feature_names)
        data = data.infer_objects()

        # Pandas does not detect categorical dtypes. Any remaining object
        # dtypes are probably categories, so convert them.
        update_dtypes = {}
        for column in data.columns:
            dtype = data.dtypes[column]
            if dtype == np.object:
                update_dtypes[column] = pd.CategoricalDtype()

        if update_dtypes:
            data = data.astype(update_dtypes, copy=False)

        df = pd.DataFrame(self.model.predict(data, **predict_kwargs))
        df.columns = (
            ["predictions"]
            if len(df.columns) == 1
            else [f"predictions_{i}" for i in range(len(df.columns))]
        )
        return df
