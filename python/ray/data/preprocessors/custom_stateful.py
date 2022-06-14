from typing import Callable, TYPE_CHECKING, Dict

from ray.data.preprocessor import Preprocessor
from ray.data import Dataset

if TYPE_CHECKING:
    import pandas


class CustomStatefulPreprocessor(Preprocessor):
    """Implements a user-defined stateful preprocessor that fits on a Dataset.

    This is meant to be generic and can be used to perform arbitrary stateful
    preprocessing that cannot already be done through existing preprocessors.
    Logic must be defined to perform fitting on a Ray Dataset and transforming
    pandas DataFrames.

    Example:

    .. code-block:: python

        import pandas as pd
        import ray.data
        from pandas import DataFrame
        from ray.data.preprocessors import CustomStatefulPreprocessor
        from ray.data import Dataset
        from ray.data.aggregate import Max

        items = [
            {"A": 1, "B": 10},
            {"A": 2, "B": 20},
            {"A": 3, "B": 30},
        ]

        ds = ray.data.from_items(items)


        def get_max_a(ds: Dataset):
            # Calculate max value for column A.
            max_a = ds.aggregate(Max("A"))
            # {'max(A)': 3}
            return max_a


        def subtract_max_a_from_a_and_add_max_a_to_b(df: DataFrame, stats: dict):
            # Subtract max A value from column A and subtract it from B.
            max_a = stats["max(A)"]
            df["A"] = df["A"] - max_a
            df["B"] = df["B"] + max_a
            return df


        preprocessor = CustomStatefulPreprocessor(
            get_max_a,
            subtract_max_a_from_a_and_add_max_a_to_b
        )
        preprocessor.fit(ds)
        transformed_ds = preprocessor.transform(ds)

        expected_items = [
            {"A": -2, "B": 13},
            {"A": -1, "B": 23},
            {"A": 0, "B": 33},
        ]
        expected_ds = ray.data.from_items(expected_items)
        assert transformed_ds.take(3) == expected_ds.take(3)

        batch = pd.DataFrame(
            {
                "A": [5, 6],
                "B": [10, 10]
            }
        )
        transformed_batch = preprocessor.transform_batch(batch)
        expected_batch = pd.DataFrame(
            {
                "A": [2, 3],
                "B": [13, 13],
            }
        )
        assert transformed_batch.equals(expected_batch)


    Args:
        fit_fn: A user defined function that computes state information about
            a :class:`ray.data.Dataset` and returns it in a :class:`dict`.
        transform_fn: A user defined function that takes in a
            :class:`pandas.DataFrame` and the :class:`dict` computed from
            ``fit_fn``, and returns a transformed :class:`pandas.DataFrame`.
    """

    _is_fittable = True

    def __init__(
        self,
        fit_fn: Callable[[Dataset], Dict],
        transform_fn: Callable[["pandas.DataFrame", Dict], "pandas.DataFrame"],
    ):
        self.fit_fn = fit_fn
        self.transform_fn = transform_fn

    def _fit(self, dataset: Dataset) -> "Preprocessor":
        self.stats_ = self.fit_fn(dataset)
        return self

    def _transform_pandas(self, df: "pandas.DataFrame") -> "pandas.DataFrame":
        return self.transform_fn(df, self.stats_)

    def __repr__(self):
        fit_fn_name = getattr(self.fit_fn, "__name__", str(self.fit_fn))
        transform_fn_name = getattr(
            self.transform_fn, "__name__", str(self.transform_fn)
        )
        stats = getattr(self, "stats_", None)
        return (
            f"CustomStatefulPreprocessor("
            f"fit_fn={fit_fn_name}, "
            f"transform_fn={transform_fn_name}, "
            f"stats={stats})"
        )
