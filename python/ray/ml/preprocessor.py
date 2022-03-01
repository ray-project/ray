import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from ray.data import Dataset
from ray.ml.predictor import DataBatchType


class PreprocessorAlreadyFittedException(Exception):
    pass


class PreprocessorNotFittedException(Exception):
    pass


class Preprocessor(abc.ABC):
    """Preprocessor interface for transforming Datasets."""

    # Preprocessors that do not need to be fitted must override this.
    is_fittable = True

    def __init__(self):
        self.is_fitted = False

    def fit(self, dataset: Dataset) -> "Preprocessor":
        """Fit this Preprocessor to the Dataset.

        Fitted state attributes will be directly set in the Preprocessor.

        Returns:
            Preprocessor: The fitted Preprocessor with state attributes.
        """
        if self.is_fitted:
            raise PreprocessorAlreadyFittedException(
                "`fit` cannot be called multiple times. "
                "Create a new Preprocessor to fit a new Dataset."
            )

        result = self._fit(dataset)
        self.is_fitted = True
        return result

    def _fit(self, dataset: Dataset) -> "Preprocessor":
        raise NotImplementedError()

    def fit_transform(self, dataset: Dataset) -> Dataset:
        """Fit this Preprocessor to the Dataset and then transform the Dataset.

        Args:
            dataset (Dataset): Input Dataset.
        Returns:
            Dataset: The transformed Dataset.
        """
        self.fit(dataset)
        return self.transform(dataset)

    def transform(self, dataset: Dataset) -> Dataset:
        """Transform the given dataset.

        Args:
            dataset (Dataset): Input Dataset.
        Returns:
            Dataset: The transformed Dataset.
        """
        if self.is_fittable and not self.is_fitted:
            raise PreprocessorNotFittedException(
                "`fit` must be called before `transform`."
            )
        return self._transform(dataset)

    def _transform(self, dataset: Dataset) -> Dataset:
        return dataset.map_batches(self._transform_pandas, batch_format="pandas")

    def transform_batch(self, df: DataBatchType) -> DataBatchType:
        """Transform a single batch of data.

        Args:
            df (DataBatchType): Input data batch.
        Returns:
            DataBatchType: The transformed data batch.
        """

        if self.is_fittable and not self.is_fitted:
            raise PreprocessorNotFittedException(
                "`fit` must be called before `transform_batch`."
            )
        return self._transform_batch(df)

    def _transform_batch(self, df: DataBatchType) -> DataBatchType:
        if not isinstance(df, pd.DataFrame):
            raise NotImplementedError(
                "`transform_batch` is currently only implemented for Pandas DataFrames."
            )
        return self._transform_pandas(df)

    def _transform_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()
