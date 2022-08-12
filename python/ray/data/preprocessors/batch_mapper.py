from typing import Callable, TYPE_CHECKING

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

    Args:
        fn: The function to apply to data batches.
    """

    _is_fittable = False

    def __init__(self, fn: Callable[["pandas.DataFrame"], "pandas.DataFrame"]):
        self.fn = fn

    def _transform_pandas(self, df: "pandas.DataFrame") -> "pandas.DataFrame":
        return self.fn(df)

    def __repr__(self):
        fn_name = getattr(self.fn, "__name__", self.fn)
        return f"{self.__class__.__name__}(fn={fn_name})"
