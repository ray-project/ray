import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from ray.data import Dataset
from ray.ml.predictor import DataBatchType


class PreprocessorAlreadyFittedException(RuntimeError):
    """Error raised when the preprocessor cannot be fitted again."""

    pass


class PreprocessorNotFittedException(RuntimeError):
    """Error raised when the preprocessor needs to be fitted first."""

    pass


class Preprocessor(abc.ABC):
    """Implements an ML preprocessing operation.

    Preprocessors are stateful objects that can be fitted against a Dataset and used
    to transform both local data batches and distributed datasets. For example, a
    Normalization preprocessor may calculate the mean and stdev of a field during
    fitting, and uses these attributes to implement its normalization transform.
    """

    # Preprocessors that do not need to be fitted must override this.
    _is_fittable = True

    def fit(self, dataset: Dataset) -> "Preprocessor":
        """Fit this Preprocessor to the Dataset.

        Fitted state attributes will be directly set in the Preprocessor.

        Args:
            dataset: Input dataset.

        Returns:
            Preprocessor: The fitted Preprocessor with state attributes.
        """
        if self.check_is_fitted():
            raise PreprocessorAlreadyFittedException(
                "`fit` cannot be called multiple times. "
                "Create a new Preprocessor to fit a new Dataset."
            )

        return self._fit(dataset)

    def fit_transform(self, dataset: Dataset) -> Dataset:
        """Fit this Preprocessor to the Dataset and then transform the Dataset.

        Args:
            dataset: Input Dataset.

        Returns:
            ray.data.Dataset: The transformed Dataset.
        """
        self.fit(dataset)
        return self.transform(dataset)

    def transform(self, dataset: Dataset) -> Dataset:
        """Transform the given dataset.

        Args:
            dataset: Input Dataset.

        Returns:
            ray.data.Dataset: The transformed Dataset.
        """
        if self._is_fittable and not self.check_is_fitted():
            raise PreprocessorNotFittedException(
                "`fit` must be called before `transform`."
            )
        return self._transform(dataset)

    def transform_batch(self, df: DataBatchType) -> DataBatchType:
        """Transform a single batch of data.

        Args:
            df (DataBatchType): Input data batch.

        Returns:
            DataBatchType: The transformed data batch.
        """
        if self._is_fittable and not self.check_is_fitted():
            raise PreprocessorNotFittedException(
                "`fit` must be called before `transform_batch`."
            )
        return self._transform_batch(df)

    def check_is_fitted(self) -> bool:
        """Returns whether this preprocessor is fitted.

        We use the convention that attributes with a trailing ``_`` are set after
        fitting is complete.
        """
        fitted_vars = [v for v in vars(self) if v.endswith("_")]
        return bool(fitted_vars)

    def _fit(self, dataset: Dataset) -> "Preprocessor":
        """Sub-classes should override this instead of fit()."""
        raise NotImplementedError()

    def _transform(self, dataset: Dataset) -> Dataset:
        # TODO(matt): Expose `batch_size` or similar configurability.
        # The default may be too small for some datasets and too large for others.
        return dataset.map_batches(self._transform_pandas, batch_format="pandas")

    def _transform_batch(self, df: DataBatchType) -> DataBatchType:
        import pandas as pd

        # TODO(matt): Add `_transform_arrow` to use based on input type.
        # Reduce conversion cost if input is in Arrow:  Arrow -> Pandas -> Arrow.
        if not isinstance(df, pd.DataFrame):
            raise NotImplementedError(
                "`transform_batch` is currently only implemented for Pandas DataFrames."
            )
        return self._transform_pandas(df)

    def _transform_pandas(self, df: "pd.DataFrame") -> "pd.DataFrame":
        raise NotImplementedError()
