from typing import Callable, TYPE_CHECKING

from ray.data.preprocessor import Preprocessor

if TYPE_CHECKING:
    import pandas


class BatchMapper(Preprocessor):
    """Apply ``fn`` to batches of records of given dataset.

    This is meant to be generic and supports low level operation on records.
    One could easily leverage this preprocessor to achieve operations like
    adding a new column or modifying a column in place.

    Args:
        fn: The udf function for batch operation.
    """

    _is_fittable = False

    def __init__(self, fn: Callable[["pandas.DataFrame"], "pandas.DataFrame"]):
        self.fn = fn

    def _transform_pandas(self, df: "pandas.DataFrame") -> "pandas.DataFrame":
        return self.fn(df)

    def __repr__(self):
        fn_name = getattr(self.fn, "__name__", self.fn)
        return f"BatchMapper(fn={fn_name})"
