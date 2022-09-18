from typing import TYPE_CHECKING
from ray.data import Dataset
from ray.data.preprocessor import Preprocessor

if TYPE_CHECKING:
    from ray.air.data_batch_type import DataBatchType


class Chain(Preprocessor):
    """Combine multiple preprocessors into a single :py:class:`Preprocessor`.

    When you call ``fit``, each preprocessor is fit on the dataset produced by the
    preceeding preprocessor's ``fit_transform``.

    Example:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import *
        >>>
        >>> df = pd.DataFrame({
        ...     "X0": [0, 1, 2],
        ...     "X1": [3, 4, 5],
        ...     "Y": ["orange", "blue", "orange"],
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>>
        >>> preprocessor = Chain(
        ...     StandardScaler(columns=["X0", "X1"]),
        ...     Concatenator(include=["X0", "X1"], output_column_name="X"),
        ...     LabelEncoder(label_column="Y")
        ... )
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           Y                                         X
        0  1  [-1.224744871391589, -1.224744871391589]
        1  0                                [0.0, 0.0]
        2  1    [1.224744871391589, 1.224744871391589]

    Args:
        preprocessors: The preprocessors to sequentially compose.
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

    def _transform_batch(self, df: "DataBatchType") -> "DataBatchType":
        for preprocessor in self.preprocessors:
            df = preprocessor.transform_batch(df)
        return df

    def __repr__(self):
        arguments = ", ".join(repr(preprocessor) for preprocessor in self.preprocessors)
        return f"{self.__class__.__name__}({arguments})"
