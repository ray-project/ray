import abc

from ray.data import Dataset
from ray.ml.predictor import DataBatchType


class Preprocessor(abc.ABC):
    """Preprocessor interface"""

    def fit_transform(self, ds: Dataset) -> Dataset:
        """Fit this preprocessor to the given dataset.

        Returns the fitted dataset.

        Args:
            ds (Dataset): Input dataset.

        Returns:
            Dataset: Transformed dataset.

        """
        raise NotImplementedError

    def transform(self, ds: Dataset) -> Dataset:
        """Transforms the given dataset.

        The preprocessor must be fitted first.

        Args:
            ds (Dataset): Input dataset.

        Returns:
            Dataset: Transformed dataset.

        """
        raise NotImplementedError

    def transform_batch(self, df: DataBatchType) -> DataBatchType:
        """Transform a single batch of data.

        The preprocessor must be fitted first.

        Args:
            df (DataBatchType): Input data batch.

        Returns:
            Dataset: Transformed data batch.
        """
        raise NotImplementedError

    def __getstate__(self) -> dict:
        return self.__dict__

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
