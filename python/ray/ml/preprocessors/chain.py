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

    def fit_status(self):
        fittable_count = 0
        fitted_count = 0
        for p in self.preprocessors:
            # AIR does not support a chain of chained preprocessors at this point.
            # Assert to explicitly call this out.
            # This can be revisited if compelling use cases emerge.
            assert not isinstance(
                p, Chain
            ), "A chain preprocessor should not contain another chain preprocessor."
            if p.fit_status() == Preprocessor.FitStatus.FITTED:
                fittable_count += 1
                fitted_count += 1
            elif p.fit_status() == Preprocessor.FitStatus.NOT_FITTED:
                fittable_count += 1
            else:
                assert p.fit_status() == Preprocessor.FitStatus.NOT_FITTABLE
        if fittable_count > 0:
            if fitted_count == fittable_count:
                return Preprocessor.FitStatus.FITTED
            elif fitted_count > 0:
                return Preprocessor.FitStatus.PARTIALLY_FITTED
            else:
                return Preprocessor.FitStatus.NOT_FITTED
        else:
            return Preprocessor.FitStatus.NOT_FITTABLE

    def __init__(self, *preprocessors: Preprocessor):
        self.preprocessors = preprocessors

    def _fit(self, ds: Dataset) -> Preprocessor:
        for preprocessor in self.preprocessors[:-1]:
            ds = preprocessor.fit_transform(ds)
        self.preprocessors[-1].fit(ds)
        return self

    def fit_transform(self, ds: Dataset) -> Dataset:
        for preprocessor in self.preprocessors:
            ds = preprocessor.fit_transform(ds)
        self._transform_stats = preprocessor.transform_stats()
        return ds

    def _transform(self, ds: Dataset) -> Dataset:
        for preprocessor in self.preprocessors:
            ds = preprocessor.transform(ds)
        self._transform_stats = preprocessor.transform_stats()
        return ds

    def _transform_batch(self, df: DataBatchType) -> DataBatchType:
        for preprocessor in self.preprocessors:
            df = preprocessor.transform_batch(df)
        return df

    def __repr__(self):
        return f"Chain(preprocessors={self.preprocessors})"
