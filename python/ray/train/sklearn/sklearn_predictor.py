from typing import TYPE_CHECKING, List, Optional, Union

import pandas as pd
from joblib import parallel_backend
from sklearn.base import BaseEstimator

from ray.air.checkpoint import Checkpoint
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.train.predictor import Predictor
from ray.train.sklearn._sklearn_utils import _set_cpu_params
from ray.train.sklearn.utils import load_checkpoint
from ray.util.joblib import register_ray

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


class SklearnPredictor(Predictor):
    """A predictor for scikit-learn compatible estimators.

    Args:
        estimator: The fitted scikit-learn compatible estimator to use for
            predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self, estimator: BaseEstimator, preprocessor: Optional["Preprocessor"] = None
    ):
        self.estimator = estimator
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint) -> "SklearnPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``SklearnTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``SklearnTrainer`` run.

        """
        estimator, preprocessor = load_checkpoint(checkpoint)
        return SklearnPredictor(estimator=estimator, preprocessor=preprocessor)

    def _predict_pandas(
        self,
        data: "pd.DataFrame",
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        num_estimator_cpus: Optional[int] = 1,
        **predict_kwargs,
    ) -> "pd.DataFrame":
        """Run inference on data batch.

        Args:
            data: A batch of input data. Either a pandas DataFrame or numpy
                array.
            feature_columns: The names or indices of the columns in the
                data to use as features to predict on. If None, then use
                all columns in ``data``.
            num_estimator_cpus: If set to a value other than None, will set
                the values of all ``n_jobs`` and ``thread_count`` parameters
                in the estimator (including in nested objects) to the given value.
            **predict_kwargs: Keyword arguments passed to ``estimator.predict``.

        Examples:

        .. code-block:: python

            import numpy as np
            from sklearn.ensemble import RandomForestClassifier
            from ray.train.predictors.sklearn import SklearnPredictor

            train_X = np.array([[1, 2], [3, 4]])
            train_y = np.array([0, 1])

            model = RandomForestClassifier().fit(train_X, train_y)
            predictor = SklearnPredictor(model=model)

            data = np.array([[1, 2], [3, 4]])
            predictions = predictor.predict(data)

            # Only use first and second column as the feature
            data = np.array([[1, 2, 8], [3, 4, 9]])
            predictions = predictor.predict(data, feature_columns=[0, 1])

        .. code-block:: python

            import pandas as pd
            from sklearn.ensemble import RandomForestClassifier
            from ray.train.predictors.sklearn import SklearnPredictor

            train_X = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
            train_y = pd.Series([0, 1])

            model = RandomForestClassifier().fit(train_X, train_y)
            predictor = SklearnPredictor(model=model)

            # Pandas dataframe.
            data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
            predictions = predictor.predict(data)

            # Only use first and second column as the feature
            data = pd.DataFrame([[1, 2, 8], [3, 4, 9]], columns=["A", "B", "C"])
            predictions = predictor.predict(data, feature_columns=["A", "B"])


        Returns:
            Prediction result.

        """
        register_ray()

        if num_estimator_cpus:
            _set_cpu_params(self.estimator, num_estimator_cpus)

        if TENSOR_COLUMN_NAME in data:
            data = data[TENSOR_COLUMN_NAME].to_numpy()
            if feature_columns:
                data = data[:, feature_columns]
        elif feature_columns:
            data = data[feature_columns]

        with parallel_backend("ray", n_jobs=num_estimator_cpus):
            df = pd.DataFrame(self.estimator.predict(data, **predict_kwargs))
        df.columns = (
            ["predictions"]
            if len(df.columns) == 1
            else [f"predictions_{i}" for i in range(len(df.columns))]
        )
        return df
