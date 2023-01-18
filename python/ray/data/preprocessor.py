import abc
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Optional, Union, Dict, Any

from ray.air.util.data_batch_conversion import BatchFormat, BlockFormat
from ray.data import Dataset, DatasetPipeline
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import pandas as pd
    import numpy as np
    from ray.air.data_batch_type import DataBatchType


@PublicAPI(stability="beta")
class PreprocessorNotFittedException(RuntimeError):
    """Error raised when the preprocessor needs to be fitted first."""

    pass


@PublicAPI(stability="beta")
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

    * ``_fit`` if your preprocessor is stateful. Otherwise, set
      ``_is_fittable=False``.
    * ``_transform_pandas`` and/or ``_transform_numpy`` for best performance,
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
            PreprocessorNotFittedException: if ``fit`` is not called yet.
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

    def transform_batch(self, data: "DataBatchType") -> "DataBatchType":
        """Transform a single batch of data.

        The data will be converted to the format supported by the Preprocessor,
        based on which ``_transform_*`` methods are implemented.

        Args:
            data: Input data batch.

        Returns:
            DataBatchType:
                The transformed data batch. This may differ
                from the input type depending on which ``_transform_*`` methods
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
        return self._transform_batch(data)

    def _transform_pipeline(self, pipeline: DatasetPipeline) -> DatasetPipeline:
        """Transform the given DatasetPipeline.

        Args:
            pipeline: The pipeline to transform.

        Returns:
            A DatasetPipeline with this preprocessor's transformation added as an
                operation to the pipeline.
        """

        fit_status = self.fit_status()
        if fit_status != Preprocessor.FitStatus.NOT_FITTABLE:
            raise RuntimeError(
                "Streaming/pipelined ingest only works with Preprocessors that are not fittable. It is not possible to fit on Datasets in a streaming fashion."
            )

        return self._transform(pipeline)

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

    def _determine_transform_to_use(self, data_format: BlockFormat) -> BatchFormat:
        """Determine which transform to use based on data format and implementation.

        We will infer and pick the best transform to use:
            * ``pandas`` data format prioritizes ``pandas`` transform if available.
            * ``arrow`` and ``numpy`` data format prioritizes ``numpy`` transform if available. # noqa: E501
            * Fall back to what's available if no preferred path found.
        """

        assert data_format in (
            "pandas",
            "arrow",
            "numpy",
        ), f"Unsupported data format: {data_format}"

        has_transform_pandas = (
            self.__class__._transform_pandas != Preprocessor._transform_pandas
        )
        has_transform_numpy = (
            self.__class__._transform_numpy != Preprocessor._transform_numpy
        )

        # Infer transform type by prioritizing native transformation to minimize
        # data conversion cost.
        if data_format == BlockFormat.PANDAS:
            # Perform native pandas transformation if possible.
            if has_transform_pandas:
                transform_type = BatchFormat.PANDAS
            elif has_transform_numpy:
                transform_type = BatchFormat.NUMPY
            else:
                raise NotImplementedError(
                    "None of `_transform_numpy` or `_transform_pandas` "
                    f"are implemented for dataset format `{data_format}`."
                )
        elif data_format == BlockFormat.ARROW or data_format == "numpy":
            # Arrow -> Numpy is more efficient
            if has_transform_numpy:
                transform_type = BatchFormat.NUMPY
            elif has_transform_pandas:
                transform_type = BatchFormat.PANDAS
            else:
                raise NotImplementedError(
                    "None of `_transform_numpy` or `_transform_pandas` "
                    f"are implemented for dataset format `{data_format}`."
                )

        return transform_type

    def _transform(
        self, dataset: Union[Dataset, DatasetPipeline]
    ) -> Union[Dataset, DatasetPipeline]:
        # TODO(matt): Expose `batch_size` or similar configurability.
        # The default may be too small for some datasets and too large for others.

        dataset_format = dataset.dataset_format()
        if dataset_format not in (BlockFormat.PANDAS, BlockFormat.ARROW):
            raise ValueError(
                f"Unsupported Dataset format: '{dataset_format}'. Only 'pandas' "
                "and 'arrow' Dataset formats are supported."
            )

        transform_type = self._determine_transform_to_use(dataset_format)

        # Our user-facing batch format should only be pandas or NumPy, other
        # formats {arrow, simple} are internal.
        kwargs = self._get_transform_config()
        if transform_type == BatchFormat.PANDAS:
            return dataset.map_batches(
                self._transform_pandas, batch_format=BatchFormat.PANDAS, **kwargs
            )
        elif transform_type == BatchFormat.NUMPY:
            return dataset.map_batches(
                self._transform_numpy, batch_format=BatchFormat.NUMPY, **kwargs
            )
        else:
            raise ValueError(
                "Invalid transform type returned from _determine_transform_to_use; "
                f'"pandas" and "numpy" allowed, but got: {transform_type}'
            )

    def _get_transform_config(self) -> Dict[str, Any]:
        """Returns kwargs to be passed to :meth:`ray.data.Dataset.map_batches`.

        This can be implemented by subclassing preprocessors.
        """
        return {}

    def _transform_batch(self, data: "DataBatchType") -> "DataBatchType":
        # For minimal install to locally import air modules
        import pandas as pd
        import numpy as np
        from ray.air.util.data_batch_conversion import (
            convert_batch_type_to_pandas,
            _convert_batch_type_to_numpy,
        )

        try:
            import pyarrow
        except ImportError:
            pyarrow = None

        if isinstance(data, pd.DataFrame):
            data_format = BlockFormat.PANDAS
        elif pyarrow is not None and isinstance(data, pyarrow.Table):
            data_format = BlockFormat.ARROW
        elif isinstance(data, (dict, np.ndarray)):
            data_format = "numpy"
        else:
            raise NotImplementedError(
                "`transform_batch` is currently only implemented for Pandas "
                "DataFrames, pyarrow Tables, NumPy ndarray and dictionary of "
                f"ndarray. Got {type(data)}."
            )

        transform_type = self._determine_transform_to_use(data_format)

        if transform_type == BatchFormat.PANDAS:
            return self._transform_pandas(convert_batch_type_to_pandas(data))
        elif transform_type == BatchFormat.NUMPY:
            return self._transform_numpy(_convert_batch_type_to_numpy(data))

    @DeveloperAPI
    def _transform_pandas(self, df: "pd.DataFrame") -> "pd.DataFrame":
        """Run the transformation on a data batch in a Pandas DataFrame format."""
        raise NotImplementedError()

    @DeveloperAPI
    def _transform_numpy(
        self, np_data: Union["np.ndarray", Dict[str, "np.ndarray"]]
    ) -> Union["np.ndarray", Dict[str, "np.ndarray"]]:
        """Run the transformation on a data batch in a NumPy ndarray format."""
        raise NotImplementedError()
