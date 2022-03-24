from ray.data import Dataset
from ray.ml.preprocessor import Preprocessor, DataBatchType


class Chain(Preprocessor):
    """Chain multiple Preprocessors into a single Preprocessor.

    Calling ``fit`` will invoke ``fit_transform`` on the input preprocessors,
    so that one preprocessor can ``fit`` based on columns/values produced by
    the ``transform`` of a preceding preprocessor.

    Args:
        preprocessors: The preprocessors that should be executed sequentially.
    """

    _is_fittable = False

    def __init__(self, *preprocessors: Preprocessor):
        super().__init__()
        self.preprocessors = preprocessors

    def _fit(self, ds: Dataset) -> Preprocessor:
        for preprocessor in self.preprocessors[:-1]:
            ds = preprocessor.fit_transform(ds)
        self.preprocessors[-1].fit(ds)
        return self

    def fit_transform(self, ds: Dataset) -> Dataset:
        for preprocessor in self.preprocessors:
            ds = preprocessor.fit_transform(ds)
        return ds

    def _transform(self, ds: Dataset) -> Dataset:
        for preprocessor in self.preprocessors:
            ds = preprocessor.transform(ds)
        return ds

    def _transform_batch(self, df: DataBatchType) -> DataBatchType:
        for preprocessor in self.preprocessors:
            df = preprocessor.transform_batch(df)
        return df

    def check_is_fitted(self) -> bool:
        return all(p.check_is_fitted() for p in self.preprocessors)

    def __repr__(self):
        return f"<Chain preprocessors={self.preprocessors}>"
