from typing import Dict, Callable, Optional, Union, TYPE_CHECKING
import warnings

import numpy as np

from ray.data.preprocessor import Preprocessor

if TYPE_CHECKING:
    import pandas


class BatchMapper(Preprocessor):
    """Apply an arbitrary operation to a dataset.

    :class:`BatchMapper` applies a user-defined function to batches of a dataset. A
    batch is a Pandas ``DataFrame`` that represents a small amount of data. By modifying
    batches instead of individual records, this class can efficiently transform a
    dataset with vectorized operations.

    Use this preprocessor to apply stateless operations that aren't already built-in.

    .. tip::
        :class:`BatchMapper` doesn't need to be fit. You can call
        ``transform`` without calling ``fit``.

    Examples:
        Use :class:`BatchMapper` to apply arbitrary operations like dropping a column.

        >>> import pandas as pd
        >>> import numpy as np
        >>> from typing import Dict
        >>> import ray
        >>> from ray.data.preprocessors import BatchMapper
        >>>
        >>> df = pd.DataFrame({"X": [0, 1, 2], "Y": [3, 4, 5]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>>
        >>> def fn(batch: pd.DataFrame) -> pd.DataFrame:
        ...     return batch.drop("Y", axis="columns")
        >>>
        >>> preprocessor = BatchMapper(fn)
        >>> preprocessor.transform(ds)  # doctest: +SKIP
        Dataset(num_blocks=1, num_rows=3, schema={X: int64})
        >>>
        >>> def fn_numpy(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        ...     return {"X": batch["X"]}
        >>> preprocessor = BatchMapper(fn_numpy, batch_format="numpy")
        >>> preprocessor.transform(ds)  # doctest: +SKIP
        Dataset(num_blocks=1, num_rows=3, schema={X: int64})

    Args:
        fn: The function to apply to data batches.
        batch_format: The preferred batch format to use in UDF. If not given,
            we will infer based on the input dataset data format.
    """

    _is_fittable = False

    def __init__(
        self,
        fn: Union[
            Callable[["pandas.DataFrame"], "pandas.DataFrame"],
            Callable[
                [Union[np.ndarray, Dict[str, np.ndarray]]],
                Union[np.ndarray, Dict[str, np.ndarray]],
            ],
        ],
        batch_format: Optional[str] = None,
        # TODO: Make batch_format required from user
        # TODO: Introduce a "zero_copy" format
        # TODO: We should reach consistency of args between BatchMapper and map_batches.
    ):
        if not batch_format:
            warnings.warn(
                "batch_format will be a required argument for BatchMapper in future "
                "releases. Defaulting to 'pandas' batch format.",
                DeprecationWarning,
            )
            batch_format = "pandas"
        if batch_format and batch_format not in [
            "pandas",
            "numpy",
        ]:
            raise ValueError("BatchMapper only supports pandas and numpy batch format.")

        self.batch_format = batch_format
        self.fn = fn

    def _transform_numpy(
        self, np_data: Union[np.ndarray, Dict[str, np.ndarray]]
    ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
        return self.fn(np_data)

    def _transform_pandas(self, df: "pandas.DataFrame") -> "pandas.DataFrame":
        return self.fn(df)

    def _determine_transform_to_use(self, data_format: str):
        if self.batch_format:
            return self.batch_format
        else:
            return super()._determine_transform_to_use(data_format)

    def __repr__(self):
        fn_name = getattr(self.fn, "__name__", self.fn)
        return f"{self.__class__.__name__}(fn={fn_name})"
