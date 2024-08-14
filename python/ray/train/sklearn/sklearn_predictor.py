from typing import TYPE_CHECKING, List, Optional, Union

import pandas as pd
from joblib import parallel_backend
from sklearn.base import BaseEstimator

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.data_batch_type import DataBatchType
from ray.air.util.data_batch_conversion import _unwrap_ndarray_object_type_if_needed
from ray.train.predictor import Predictor
from ray.train.sklearn import SklearnCheckpoint
from ray.util.annotations import PublicAPI
from ray.util.joblib import register_ray

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


# thread_count is a catboost parameter
SKLEARN_CPU_PARAM_NAMES = ["n_jobs", "thread_count"]


def _set_cpu_params(estimator: BaseEstimator, num_cpus: int) -> None:
    """Sets all CPU-related params to num_cpus (incl. nested)."""
    cpu_params = {
        param: num_cpus
        for param in estimator.get_params(deep=True)
        if any(
            param.endswith(cpu_param_name) for cpu_param_name in SKLEARN_CPU_PARAM_NAMES
        )
    }
    estimator.set_params(**cpu_params)


@PublicAPI(stability="alpha")
class SklearnPredictor(Predictor):
    """A predictor for scikit-learn compatible estimators.

    Args:
        estimator: The fitted scikit-learn compatible estimator to use for
            predictions.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self,
        estimator: BaseEstimator,
        preprocessor: Optional["Preprocessor"] = None,
    ):
        self.estimator = estimator
        super().__init__(preprocessor)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(estimator={self.estimator!r}, "
            f"preprocessor={self._preprocessor!r})"
        )

    @classmethod
    def from_checkpoint(cls, checkpoint: SklearnCheckpoint) -> "SklearnPredictor":
        """Instantiate the predictor from a Checkpoint.

        The checkpoint is expected to be a result of ``SklearnTrainer``.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from. It is expected to be from the result of a
                ``SklearnTrainer`` run.
        """
        estimator = checkpoint.get_estimator()
        preprocessor = checkpoint.get_preprocessor()
        return cls(estimator=estimator, preprocessor=preprocessor)

    def predict(
        self,
        data: DataBatchType,
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        num_estimator_cpus: Optional[int] = None,
        **predict_kwargs,
    ) -> DataBatchType:
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
            >>> import numpy as np
            >>> from sklearn.ensemble import RandomForestClassifier
            >>> from ray.train.sklearn import SklearnPredictor
            >>>
            >>> train_X = np.array([[1, 2], [3, 4]])
            >>> train_y = np.array([0, 1])
            >>>
            >>> model = RandomForestClassifier().fit(train_X, train_y)
            >>> predictor = SklearnPredictor(estimator=model)
            >>>
            >>> data = np.array([[1, 2], [3, 4]])
            >>> predictions = predictor.predict(data)
            >>>
            >>> # Only use first and second column as the feature
            >>> data = np.array([[1, 2, 8], [3, 4, 9]])
            >>> predictions = predictor.predict(data, feature_columns=[0, 1])

            >>> import pandas as pd
            >>> from sklearn.ensemble import RandomForestClassifier
            >>> from ray.train.sklearn import SklearnPredictor
            >>>
            >>> train_X = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
            >>> train_y = pd.Series([0, 1])
            >>>
            >>> model = RandomForestClassifier().fit(train_X, train_y)
            >>> predictor = SklearnPredictor(estimator=model)
            >>>
            >>> # Pandas dataframe.
            >>> data = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
            >>> predictions = predictor.predict(data)
            >>>
            >>> # Only use first and second column as the feature
            >>> data = pd.DataFrame([[1, 2, 8], [3, 4, 9]], columns=["A", "B", "C"])
            >>> predictions = predictor.predict(data, feature_columns=["A", "B"])


        Returns:
            Prediction result.

        """

        return Predictor.predict(
            self,
            data,
            feature_columns=feature_columns,
            num_estimator_cpus=num_estimator_cpus,
            **predict_kwargs,
        )

    def _predict_pandas(
        self,
        data: "pd.DataFrame",
        feature_columns: Optional[Union[List[str], List[int]]] = None,
        num_estimator_cpus: Optional[int] = 1,
        **predict_kwargs,
    ) -> "pd.DataFrame":
        register_ray()

        if num_estimator_cpus:
            _set_cpu_params(self.estimator, num_estimator_cpus)

        if TENSOR_COLUMN_NAME in data:
            data = data[TENSOR_COLUMN_NAME].to_numpy()
            data = _unwrap_ndarray_object_type_if_needed(data)
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
