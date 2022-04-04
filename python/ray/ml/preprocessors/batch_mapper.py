from typing import Callable

from ray.data import Dataset
from ray.ml.preprocessor import Preprocessor


class BatchMapper(Preprocessor):
    _is_fittable = False

    def __init__(self, fn: Callable[["pandas.DataFrame"], "pandas.DataFrame"]):
        self.fn = fn

    def _fit(self, dataset: Dataset) -> Preprocessor:
        return self

    def _transform_pandas(self, df: "pandas.DataFrame") -> "pandas.DataFrame":
        return df.transform(self.fn)

    def __repr__(self):
        return f"<BatchMapper udf={self.fn}>"
