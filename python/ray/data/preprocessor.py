import abc
import base64
import collections
import logging
import pickle
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union, final

from ray.air.util.data_batch_conversion import BatchFormat
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    import numpy as np
    import pandas as pd

    from ray.air.data_batch_type import DataBatchType
    from ray.data.dataset import Dataset


logger = logging.getLogger(__name__)


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

    def __init__(self):
        from ray.data.preprocessors.utils import StatComputationPlan

        self.stat_computation_plan = StatComputationPlan()
        self.stats_ = {}

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

        fitted_vars = [v for v in vars(self) if v.endswith("_") and getattr(self, v)]
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

        self.stat_computation_plan.reset()
        fitted_ds = self._fit(ds)._fit_execute(ds)
        self._fitted = True
        return fitted_ds

    def _fit_execute(self, dataset: "Dataset"):
        self.stats_ |= self.stat_computation_plan.compute(dataset)
        return self

    def has_stats(self) -> bool:
        return hasattr(self, "stats_") and len(self.stats_) > 0

    def fit_transform(
        self,
        ds: "Dataset",
        *,
        transform_num_cpus: Optional[float] = None,
        transform_memory: Optional[float] = None,
        transform_batch_size: Optional[int] = None,
        transform_concurrency: Optional[int] = None,
    ) -> "Dataset":
        """Fit this Preprocessor to the Dataset and then transform the Dataset.

        Calling it more than once will overwrite all previously fitted state:
        ``preprocessor.fit_transform(A).fit_transform(B)``
        is equivalent to ``preprocessor.fit_transform(B)``.

        Args:
            ds: Input Dataset.
            transform_num_cpus: [experimental] The number of CPUs to reserve for each parallel map worker.
            transform_memory: [experimental] The heap memory in bytes to reserve for each parallel map worker.
            transform_batch_size: [experimental] The maximum number of rows to return.
            transform_concurrency: [experimental] The maximum number of Ray workers to use concurrently.

        Returns:
            ray.data.Dataset: The transformed Dataset.
        """
        self.fit(ds)
        return self.transform(
            ds,
            num_cpus=transform_num_cpus,
            memory=transform_memory,
            batch_size=transform_batch_size,
            concurrency=transform_concurrency,
        )

    def transform(
        self,
        ds: "Dataset",
        *,
        batch_size: Optional[int] = None,
        num_cpus: Optional[float] = None,
        memory: Optional[float] = None,
        concurrency: Optional[int] = None,
    ) -> "Dataset":
        """Transform the given dataset.

        Args:
            ds: Input Dataset.
            batch_size: [experimental] Advanced configuration for adjusting input size for each worker.
            num_cpus: [experimental] The number of CPUs to reserve for each parallel map worker.
            memory: [experimental] The heap memory in bytes to reserve for each parallel map worker.
            concurrency: [experimental] The maximum number of Ray workers to use concurrently.

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
        transformed_ds = self._transform(
            ds,
            batch_size=batch_size,
            num_cpus=num_cpus,
            memory=memory,
            concurrency=concurrency,
        )
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
        self,
        ds: "Dataset",
        batch_size: Optional[int],
        num_cpus: Optional[float] = None,
        memory: Optional[float] = None,
        concurrency: Optional[int] = None,
    ) -> "Dataset":
        transform_type = self._determine_transform_to_use()

        # Our user-facing batch format should only be pandas or NumPy, other
        # formats {arrow, simple} are internal.
        kwargs = self._get_transform_config()
        if num_cpus is not None:
            kwargs["num_cpus"] = num_cpus
        if memory is not None:
            kwargs["memory"] = memory
        if batch_size is not None:
            kwargs["batch_size"] = batch_size
        if concurrency is not None:
            kwargs["concurrency"] = concurrency

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

    @classmethod
    def _derive_and_validate_output_columns(
        cls, columns: List[str], output_columns: Optional[List[str]]
    ) -> List[str]:
        """Returns the output columns after validation.

        Checks if the columns are explicitly set, otherwise defaulting to
        the input columns.

        Raises:
            ValueError: If the length of the output columns does not match the
                length of the input columns.
        """

        if output_columns and len(columns) != len(output_columns):
            raise ValueError(
                "Invalid output_columns: Got len(columns) != len(output_columns). "
                "The length of columns and output_columns must match."
            )
        return output_columns or columns

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

    def get_input_columns(self) -> List[str]:
        return getattr(self, "columns", [])

    def get_output_columns(self) -> List[str]:
        return getattr(self, "output_columns", [])

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        # Exclude unpicklable attributes
        state.pop("stat_computation_plan", None)
        return state

    def __setstate__(self, state: Dict[str, Any]):
        from ray.data.preprocessors.utils import StatComputationPlan

        self.__dict__.update(state)
        self.stat_computation_plan = StatComputationPlan()

    @DeveloperAPI
    def serialize(self) -> str:
        """Return this preprocessor serialized as a string.
        Note: This is not a stable serialization format as it uses `pickle`.
        """
        # Convert it to a plain string so that it can be included as JSON metadata
        # in Trainer checkpoints.
        return base64.b64encode(pickle.dumps(self)).decode("ascii")

    @staticmethod
    @DeveloperAPI
    def deserialize(serialized: str) -> "Preprocessor":
        """Load the original preprocessor serialized via `self.serialize()`."""
        return pickle.loads(base64.b64decode(serialized))


@DeveloperAPI
class SerializablePreprocessorBase(Preprocessor, abc.ABC):
    """Abstract base class for serializable preprocessors.

    This class defines the serialization interface that all preprocessors must implement
    to support saving and loading their state. The serialization system uses CloudPickle
    as the primary format.

    **Architecture Overview:**

    The serialization system is built around two types of methods:

    1. **Final Methods (DO NOT OVERRIDE):**
       - ``serialize()``: Orchestrates the serialization process
       - ``deserialize()``: Orchestrates the deserialization process

       These methods are marked as ``@final`` and should never be overridden by
       subclasses. They handle format detection, factory coordination, and error handling.

    2. **Abstract Methods (MUST IMPLEMENT):**
       - ``_get_serializable_fields()``: Extract instance fields for serialization
       - ``_set_serializable_fields()``: Restore instance fields from deserialization
       - ``_get_stats()``: Extract computed statistics for serialization
       - ``_set_stats()``: Restore computed statistics from deserialization

       These methods must be implemented by each preprocessor subclass to define
       their specific serialization behavior.

    **Format Support:**

    - **CloudPickle** (default):
    - **Pickle** (legacy): Backward compatibility for existing serialized data

    **Important Notes:**

    - Never override ``serialize()`` or ``deserialize()`` in subclasses
    - Always call ``super().__init__()`` in subclass constructors
    - Use ``_fitted`` attribute to track fitting state
    - Store computed statistics in ``stats_`` dictionary
    - Handle version migration and backwards compatibility in ``_set_serializable_fields()`` if needed
    """

    @DeveloperAPI
    class SerializationFormat(Enum):
        CLOUDPICKLE = "cloudpickle"
        PICKLE = "pickle"  # legacy

    MAGIC_CLOUDPICKLE = b"CPKL:"
    SERIALIZER_FORMAT_VERSION = 1

    @abc.abstractmethod
    def _get_serializable_fields(self) -> Dict[str, Any]:
        """Extract instance fields that should be serialized.

        This method should return a dictionary containing all instance attributes
        that are necessary to restore the preprocessor's configuration state.
        This typically includes constructor parameters and internal state flags.

        Returns:
            Dictionary mapping field names to their values
        """
        pass

    @abc.abstractmethod
    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        """Restore instance fields from deserialized data.

        This method should restore the preprocessor's configuration state from
        the provided fields' dictionary. It's called during deserialization to
        recreate the instance state.

        **Version Migration:**

        If the serialized version differs from the current ``VERSION``,
        implement migration logic to handle schema changes:

        .. testcode::

            def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
                # Handle version migration
                if version == 1 and self.VERSION == 2:
                    # Migrate from version 1 to 2
                    if "old_field" in fields:
                        fields["new_field"] = migrate_old_field(fields.pop("old_field"))

                # Set all fields
                for key, value in fields.items():
                    setattr(self, key, value)

                # Reinitialize derived state
                self.stat_computation_plan = StatComputationPlan()

        Args:
            fields: Dictionary of field names to values
            version: Version of the serialized data
        """
        pass

    def _get_stats(self) -> Dict[str, Any]:
        """Extract computed statistics that should be serialized.

        This method should return the computed statistics that were generated
        during the ``fit()`` process. These statistics are typically stored in
        the ``stats_`` attribute and contain the learned parameters needed for
        transformation.

        Returns:
            Dictionary containing computed statistics
        """
        return getattr(self, "stats_", {})

    def _set_stats(self, stats: Dict[str, Any]):
        """Restore computed statistics from deserialized data.

        This method should restore the preprocessor's computed statistics from
        the provided stats dictionary. These statistics are typically stored in
        the ``stats_`` attribute and contain learned parameters from fitting.

        Args:
            stats: Dictionary containing computed statistics
        """
        self.stats_ = stats

    @classmethod
    def get_preprocessor_class_id(cls) -> str:
        """Get the preprocessor class identifier for this preprocessor class.

        Returns:
            The preprocessor class identifier string used to identify this preprocessor
            type in serialized data.
        """
        return cls.__PREPROCESSOR_CLASS_ID

    @classmethod
    def set_preprocessor_class_id(cls, identifier: str) -> None:
        """Set the preprocessor class identifier for this preprocessor class.

        Args:
            identifier: The preprocessor class identifier string to use.
        """
        cls.__PREPROCESSOR_CLASS_ID = identifier

    @classmethod
    def get_version(cls) -> int:
        """Get the version number for this preprocessor class.

        Returns:
            The version number for this preprocessor's serialization format.
        """
        return cls.__VERSION

    @classmethod
    def set_version(cls, version: int) -> None:
        """Set the version number for this preprocessor class.

        Args:
            version: The version number for this preprocessor's serialization format.
        """
        cls.__VERSION = version

    @final
    @DeveloperAPI
    def serialize(self) -> Union[str, bytes]:
        """Serialize this preprocessor to a string or bytes.

        **⚠️ DO NOT OVERRIDE THIS METHOD IN SUBCLASSES ⚠️**

        This method is marked as ``@final`` in the concrete implementation and handles
        the complete serialization orchestration. Subclasses should implement the
        abstract methods instead: ``_get_serializable_fields()`` and ``_get_stats()``.

        **Serialization Process:**

        1. Extracts fields via ``_get_serializable_fields()``
        2. Extracts statistics via ``_get_stats()``
        3. Packages data with metadata (type, version, format)
        4. Delegates to ``SerializationHandlerFactory`` for format-specific handling
        5. Returns serialized data with magic bytes for format identification

        **Supported Formats:**

        - **CloudPickle** (default):
        - **Pickle** (legacy): Backward compatibility for existing serialized data

        Returns:
            Serialized preprocessor data (bytes for CloudPickle, str for legacy Pickle)

        Raises:
            ValueError: If the serialization format is invalid or unsupported
        """
        from ray.data.preprocessors.serialization_handlers import (
            HandlerFormatName,
            SerializationHandlerFactory,
        )

        # Prepare data for CloudPickle format
        data = {
            "type": self.get_preprocessor_class_id(),
            "version": self.get_version(),
            "fields": self._get_serializable_fields(),
            "stats": self._get_stats(),
            # The `serializer_format_version` field is for versioning the structure of this
            # dictionary. It is separate from the preprocessor's own version and is not used currently.
            "serializer_format_version": self.SERIALIZER_FORMAT_VERSION,
        }

        return SerializationHandlerFactory.get_handler(
            format_identifier=HandlerFormatName.CLOUDPICKLE
        ).serialize(data)

    @final
    @staticmethod
    @DeveloperAPI
    def deserialize(serialized: Union[str, bytes]) -> "Preprocessor":
        """Deserialize a preprocessor from serialized data.

        **⚠️ DO NOT OVERRIDE THIS METHOD IN SUBCLASSES ⚠️**

        This method is marked as ``@final`` in the concrete implementation and handles
        the complete deserialization orchestration. Subclasses should implement the
        abstract methods instead: ``_set_serializable_fields()`` and ``_set_stats()``.

        **Deserialization Process:**

        1. Detects format from magic bytes in serialized data
        2. Delegates to ``SerializationHandlerFactory`` for format-specific parsing
        3. Extracts metadata (type, version, fields, stats)
        4. Looks up preprocessor class from registry
        5. Creates new instance and restores state via abstract methods
        6. Returns fully reconstructed preprocessor instance

        **Format Detection:**

        The method automatically detects the serialization format:
        - ``CPKL:`` → CloudPickle format
        - Base64 string → Legacy Pickle format

        **Error Handling:**

        Provides comprehensive error handling for:
        - Unknown serialization formats
        - Corrupted or invalid data
        - Missing preprocessor types
        - Version compatibility issues

        Args:
            serialized: Serialized preprocessor data (bytes or str)

        Returns:
            Reconstructed preprocessor instance

        Raises:
            ValueError: If the serialized data is corrupted or format is unrecognized
            UnknownPreprocessorError: If the preprocessor type is not registered
        """
        from ray.data.preprocessors.serialization_handlers import (
            PickleSerializationHandler,
            SerializationHandlerFactory,
        )
        from ray.data.preprocessors.version_support import (
            UnknownPreprocessorError,
            _lookup_class,
        )

        try:
            # Use factory to deserialize all formats (auto-detects format)
            handler = SerializationHandlerFactory.get_handler(data=serialized)
            meta = handler.deserialize(serialized)

            # Handle pickle specially - it returns the object directly
            if isinstance(handler, PickleSerializationHandler):
                return meta  # For pickle, meta is actually the deserialized object

            # Reconstruct the preprocessor object for structured formats
            cls = _lookup_class(meta["type"])

            # Validate metadata
            if meta["serializer_format_version"] != cls.SERIALIZER_FORMAT_VERSION:
                raise ValueError(
                    f"Unsupported serializer format version: {meta['serializer_format_version']}"
                )

            obj = cls.__new__(cls)

            # handle base class fields here
            from ray.data.preprocessors.utils import StatComputationPlan

            obj.stat_computation_plan = StatComputationPlan()

            obj._set_serializable_fields(fields=meta["fields"], version=meta["version"])

            obj._set_stats(stats=meta["stats"])
            return obj
        except UnknownPreprocessorError:
            # Let UnknownPreprocessorError pass through unchanged for specific error handling
            raise
        except Exception as e:
            # Provide more helpful error message for other exception types
            raise ValueError(
                f"Failed to deserialize preprocessor. Data preview: {serialized[:50]}..."
            ) from e


SerializationFormat = SerializablePreprocessorBase.SerializationFormat
