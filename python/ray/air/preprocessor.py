import abc
import warnings
from enum import Enum
from typing import Optional, TYPE_CHECKING
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pandas as pd

from ray.data import Dataset
from ray.air.predictor import DataBatchType


@PublicAPI(stability="alpha")
class PreprocessorNotFittedException(RuntimeError):
    """Error raised when the preprocessor needs to be fitted first."""

    pass


@PublicAPI(stability="alpha")
class Preprocessor(abc.ABC):
    """Implements an ML preprocessing operation.

    Preprocessors are stateful objects that can be fitted against a Dataset and used
    to transform both local data batches and distributed datasets. For example, a
    Normalization preprocessor may calculate the mean and stdev of a field during
    fitting, and uses these attributes to implement its normalization transform.
    """

    class FitStatus(str, Enum):
        """The fit status of preprocessor."""

        NOT_FITTABLE = "NOT_FITTABLE"
        NOT_FITTED = "NOT_FITTED"
        # Only meaningful for Chain preprocessors.
        # At least one contained preprocessor in the chain preprocessor
        # is fitted and at least one that can be fitted is not fitted yet.
        # This is a state that show up if caller only interacts
        # with the chain preprocessor through intended Preprocessor APIs.
        PARTIALLY_FITTED = "PARTIALLY_FITTED"
        FITTED = "FITTED"

    # Preprocessors that do not need to be fitted must override this.
    _is_fittable = True

    def fit_status(self) -> "Preprocessor.FitStatus":
        if not self._is_fittable:
            return Preprocessor.FitStatus.NOT_FITTABLE
        elif self._check_is_fitted():
            return Preprocessor.FitStatus.FITTED
        else:
            return Preprocessor.FitStatus.NOT_FITTED

    def transform_stats(self) -> Optional[str]:
        """Return Dataset stats for the most recent transform call, if any."""
        if not hasattr(self, "_transform_stats"):
            return None
        return self._transform_stats

    def fit(self, dataset: Dataset) -> "Preprocessor":
        """Fit this Preprocessor to the Dataset.

        Fitted state attributes will be directly set in the Preprocessor.

        Calling it more than once will overwrite all previously fitted state:
        ``preprocessor.fit(A).fit(B)`` is equivalent to ``preprocessor.fit(B)``.

        Args:
            dataset: Input dataset.

        Returns:
            Preprocessor: The fitted Preprocessor with state attributes.
        """
        fit_status = self.fit_status()
        if fit_status == Preprocessor.FitStatus.NOT_FITTABLE:
            # No-op as there is no state to be fitted.
            return self

        if fit_status in (
            Preprocessor.FitStatus.FITTED,
            Preprocessor.FitStatus.PARTIALLY_FITTED,
        ):
            warnings.warn(
                "`fit` has already been called on the preprocessor (or at least one "
                "contained preprocessors if this is a chain). "
                "All previously fitted state will be overwritten!"
            )

        return self._fit(dataset)

    def fit_transform(self, dataset: Dataset) -> Dataset:
        """Fit this Preprocessor to the Dataset and then transform the Dataset.

        Calling it more than once will overwrite all previously fitted state:
        ``preprocessor.fit_transform(A).fit_transform(B)``
        is equivalent to ``preprocessor.fit_transform(B)``.

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

        Raises:
            PreprocessorNotFittedException, if ``fit`` is not called yet.
        """
        fit_status = self.fit_status()
        if fit_status in (
            Preprocessor.FitStatus.PARTIALLY_FITTED,
            Preprocessor.FitStatus.NOT_FITTED,
        ):
            raise PreprocessorNotFittedException(
                "`fit` must be called before `transform`, "
                "or simply use fit_transform() to run both steps"
            )
        transformed_ds = self._transform(dataset)
        self._transform_stats = transformed_ds.stats()
        return transformed_ds

    def transform_batch(self, df: DataBatchType) -> DataBatchType:
        """Transform a single batch of data.

        Args:
            df: Input data batch.

        Returns:
            DataBatchType: The transformed data batch.
        """
        fit_status = self.fit_status()
        if fit_status in (
            Preprocessor.FitStatus.PARTIALLY_FITTED,
            Preprocessor.FitStatus.NOT_FITTED,
        ):
            raise PreprocessorNotFittedException(
                "`fit` must be called before `transform_batch`."
            )
        return self._transform_batch(df)

    def _check_is_fitted(self) -> bool:
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
