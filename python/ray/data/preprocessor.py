import abc
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Optional

from ray.data import Dataset
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow

    from ray.air.data_batch_type import DataBatchType


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

    Preprocessors can also be stateless and transform data without needed to be fitted.
    For example, a preprocessor may simply remove a column, which does not require
    any state to be fitted.

    If you are implementing your own Preprocessor sub-class, you should override the
    following:
    * ``_fit`` - if your preprocessor is stateful. Otherwise, set
    ``_is_fittable=False``.
    * ``_transform_pandas`` and/or ``_transform_arrow`` - for best performance,
    implement both. Otherwise, the data will be converted to the match the
    implemented method.
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

    def transform_batch(self, df: "DataBatchType") -> "DataBatchType":
        """Transform a single batch of data.

        The data will be converted to the format supported by the Preprocessor,
        based on which ``_transform_*`` method(s) are implemented.

        Args:
            df: Input data batch.

        Returns:
            DataBatchType: The transformed data batch. This may differ
                from the input type depending on which ``_transform_*`` method(s)
                are implemented.
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

    @DeveloperAPI
    def _fit(self, dataset: Dataset) -> "Preprocessor":
        """Sub-classes should override this instead of fit()."""
        raise NotImplementedError()

    def _determine_transform_to_use(self, data_format: str) -> str:
        """Determine which transform to use based on data format and implementation.

        * If only _transform_arrow is implemented, will convert the data to arrow.
        * If only _transform_pandas is implemented, will convert the data to pandas.
        If both are implemented, will pick the method corresponding to the format
        for best performance.
        * Implementation is defined as overriding the method in a sub-class.
        """

        assert data_format in ("pandas", "arrow")
        has_transform_arrow = (
            self.__class__._transform_arrow != Preprocessor._transform_arrow
        )
        has_transform_pandas = (
            self.__class__._transform_pandas != Preprocessor._transform_pandas
        )

        if has_transform_arrow and has_transform_pandas:
            # Both transforms available, so we delegate based on the dataset format to
            # ensure minimal data conversions.
            if data_format == "pandas":
                transform_type = "pandas"
            elif data_format == "arrow":
                transform_type = "arrow"
        elif has_transform_pandas:
            transform_type = "pandas"
        elif has_transform_arrow:
            transform_type = "arrow"
        else:
            raise NotImplementedError(
                "Neither `_transform_arrow` nor `_transform_pandas` are implemented."
            )
        return transform_type

    def _transform(self, dataset: Dataset) -> Dataset:
        # TODO(matt): Expose `batch_size` or similar configurability.
        # The default may be too small for some datasets and too large for others.

        dataset_format = dataset._dataset_format()
        if dataset_format not in ("pandas", "arrow"):
            raise ValueError(
                f"Unsupported Dataset format: '{dataset_format}'. Only 'pandas' and "
                "'arrow' Dataset formats are supported."
            )

        transform_type = self._determine_transform_to_use(dataset_format)

        if transform_type == "pandas":
            return dataset.map_batches(self._transform_pandas, batch_format="pandas")
        elif transform_type == "arrow":
            return dataset.map_batches(self._transform_arrow, batch_format="pyarrow")
        else:
            raise ValueError(
                "Invalid transform type returned from _determine_transform_to_use; "
                f'"pandas" and "arrow" allowed, but got: {transform_type}'
            )

    def _transform_batch(self, df: "DataBatchType") -> "DataBatchType":
        import pandas as pd

        try:
            import pyarrow
        except ImportError:
            pyarrow = None

        if isinstance(df, pd.DataFrame):
            data_format = "pandas"
        elif pyarrow is not None and isinstance(df, pyarrow.Table):
            data_format = "arrow"
        else:
            raise NotImplementedError(
                "`transform_batch` is currently only implemented for Pandas DataFrames "
                f"and PyArrow Tables. Got {type(df)}."
            )

        transform_type = self._determine_transform_to_use(data_format)

        if transform_type == "pandas":
            if data_format == "pandas":
                return self._transform_pandas(df)
            else:
                return self._transform_pandas(df.to_pandas())
        elif transform_type == "arrow":
            if data_format == "arrow":
                return self._transform_arrow(df)
            else:
                return self._transform_arrow(pyarrow.Table.from_pandas(df))

    @DeveloperAPI
    def _transform_pandas(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Run the transformation on a data batch in a Pandas DataFrame format."""
        raise NotImplementedError()

    @DeveloperAPI
    def _transform_arrow(self, table: "pyarrow.Table") -> "pyarrow.Table":
        """Run the transformation on a data batch in a PyArrow Table format."""
        raise NotImplementedError()
