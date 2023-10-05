import abc
import base64
import collections
import pickle
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from ray.air.util.data_batch_conversion import BatchFormat
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

    from ray.air.data_batch_type import DataBatchType
    from ray.data import Dataset, DatasetPipeline


@PublicAPI(stability="beta")
class PreprocessorNotFittedException(RuntimeError):
    """Error raised when the preprocessor needs to be fitted first."""

    pass


@PublicAPI(stability="beta")
class Preprocessor(abc.ABC):
    """Implements an ML preprocessing operation.

    Preprocessors are stateful objects that can be fitted against a Dataset and used
    to transform both local data batches and distributed data. For example, a
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

    def _check_has_fitted_state(self):
        """Checks if the Preprocessor has fitted state.

        This is also used as an indiciation if the Preprocessor has been fit, following
        convention from Ray versions prior to 2.6.
        This allows preprocessors that have been fit in older versions of Ray to be
        used to transform data in newer versions.
        """

        fitted_vars = [v for v in vars(self) if v.endswith("_")]
        return bool(fitted_vars)

    def fit_status(self) -> "Preprocessor.FitStatus":
        if not self._is_fittable:
            return Preprocessor.FitStatus.NOT_FITTABLE
        elif (
            hasattr(self, "_fitted") and self._fitted
        ) or self._check_has_fitted_state():
            return Preprocessor.FitStatus.FITTED
        else:
            return Preprocessor.FitStatus.NOT_FITTED

    @Deprecated
    def transform_stats(self) -> Optional[str]:
        """Return Dataset stats for the most recent transform call, if any."""

        raise DeprecationWarning(
            "`preprocessor.transform_stats()` is no longer supported in Ray 2.4. "
            "With Dataset now lazy by default, the stats are only populated "
            "after execution. Once the dataset transform is executed, the "
            "stats can be accessed directly from the transformed dataset "
            "(`ds.stats()`), or can be viewed in the ray-data.log "
            "file saved in the Ray logs directory "
            "(defaults to /tmp/ray/session_{SESSION_ID}/logs/)."
        )

    def fit(self, ds: "Dataset") -> "Preprocessor":
        """Fit this Preprocessor to the Dataset.

        Fitted state attributes will be directly set in the Preprocessor.

        Calling it more than once will overwrite all previously fitted state:
        ``preprocessor.fit(A).fit(B)`` is equivalent to ``preprocessor.fit(B)``.

        Args:
            ds: Input dataset.

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

        fitted_ds = self._fit(ds)
        self._fitted = True
        return fitted_ds

    def fit_transform(self, ds: "Dataset") -> "Dataset":
        """Fit this Preprocessor to the Dataset and then transform the Dataset.

        Calling it more than once will overwrite all previously fitted state:
        ``preprocessor.fit_transform(A).fit_transform(B)``
        is equivalent to ``preprocessor.fit_transform(B)``.

        Args:
            ds: Input Dataset.

        Returns:
            ray.data.Dataset: The transformed Dataset.
        """
        self.fit(ds)
        return self.transform(ds)

    def transform(self, ds: "Dataset") -> "Dataset":
        """Transform the given dataset.

        Args:
            ds: Input Dataset.

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
        transformed_ds = self._transform(ds)
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

    def _transform_pipeline(self, pipeline: "DatasetPipeline") -> "DatasetPipeline":
        """Transform the given DatasetPipeline.

        Args:
            pipeline: The pipeline to transform.

        Returns:
            A DatasetPipeline with this preprocessor's transformation added as an
                operation to the pipeline.
        """

        fit_status = self.fit_status()
        if fit_status not in (
            Preprocessor.FitStatus.NOT_FITTABLE,
            Preprocessor.FitStatus.FITTED,
        ):
            raise RuntimeError(
                "Streaming/pipelined ingest only works with "
                "Preprocessors that do not need to be fit on the entire dataset. "
                "It is not possible to fit on Datasets "
                "in a streaming fashion."
            )

        return self._transform(pipeline)

    @DeveloperAPI
    def _fit(self, ds: "Dataset") -> "Preprocessor":
        """Sub-classes should override this instead of fit()."""
        raise NotImplementedError()

    def _determine_transform_to_use(self) -> BatchFormat:
        """Determine which batch format to use based on Preprocessor implementation.

        * If only `_transform_pandas` is implemented, then use ``pandas`` batch format.
        * If only `_transform_numpy` is implemented, then use ``numpy`` batch format.
        * If both are implemented, then use the Preprocessor defined preferred batch
        format.
        """

        has_transform_pandas = (
            self.__class__._transform_pandas != Preprocessor._transform_pandas
        )
        has_transform_numpy = (
            self.__class__._transform_numpy != Preprocessor._transform_numpy
        )

        if has_transform_numpy and has_transform_pandas:
            return self.preferred_batch_format()
        elif has_transform_numpy:
            return BatchFormat.NUMPY
        elif has_transform_pandas:
            return BatchFormat.PANDAS
        else:
            raise NotImplementedError(
                "None of `_transform_numpy` or `_transform_pandas` are implemented. "
                "At least one of these transform functions must be implemented "
                "for Preprocessor transforms."
            )

    def _transform(
        self, ds: Union["Dataset", "DatasetPipeline"]
    ) -> Union["Dataset", "DatasetPipeline"]:
        # TODO(matt): Expose `batch_size` or similar configurability.
        # The default may be too small for some datasets and too large for others.
        transform_type = self._determine_transform_to_use()

        # Our user-facing batch format should only be pandas or NumPy, other
        # formats {arrow, simple} are internal.
        kwargs = self._get_transform_config()
        if transform_type == BatchFormat.PANDAS:
            return ds.map_batches(
                self._transform_pandas, batch_format=BatchFormat.PANDAS, **kwargs
            )
        elif transform_type == BatchFormat.NUMPY:
            return ds.map_batches(
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
        import numpy as np
        import pandas as pd

        from ray.air.util.data_batch_conversion import (
            _convert_batch_type_to_numpy,
            _convert_batch_type_to_pandas,
        )

        try:
            import pyarrow
        except ImportError:
            pyarrow = None

        if not isinstance(
            data, (pd.DataFrame, pyarrow.Table, collections.abc.Mapping, np.ndarray)
        ):
            raise ValueError(
                "`transform_batch` is currently only implemented for Pandas "
                "DataFrames, pyarrow Tables, NumPy ndarray and dictionary of "
                f"ndarray. Got {type(data)}."
            )

        transform_type = self._determine_transform_to_use()

        if transform_type == BatchFormat.PANDAS:
            return self._transform_pandas(_convert_batch_type_to_pandas(data))
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

    @classmethod
    @DeveloperAPI
    def preferred_batch_format(cls) -> BatchFormat:
        """Batch format hint for upstream producers to try yielding best block format.

        The preferred batch format to use if both `_transform_pandas` and
        `_transform_numpy` are implemented. Defaults to Pandas.

        Can be overriden by Preprocessor classes depending on which transform
        path is the most optimal.
        """
        return BatchFormat.PANDAS

    @DeveloperAPI
    def serialize(self) -> str:
        """Return this preprocessor serialized as a string.
        Note: this is not a stable serialization format as it uses `pickle`.
        """
        # Convert it to a plain string so that it can be included as JSON metadata
        # in Trainer checkpoints.
        return base64.b64encode(pickle.dumps(self)).decode("ascii")

    @staticmethod
    @DeveloperAPI
    def deserialize(serialized: str) -> "Preprocessor":
        """Load the original preprocessor serialized via `self.serialize()`."""
        return pickle.loads(base64.b64decode(serialized))
