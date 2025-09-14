from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pandas as pd
import ray

from ray.data.stats import feature_aggregators_for_dataset

if TYPE_CHECKING:
    from ray.data import Dataset


@dataclass
class CategoricalSummary:
    """
    A summary of a categorical column.
    """

    column: str
    count: int
    missing_pct: Optional[float]


@dataclass
class NumericalSummary:
    """
    A summary of a numerical column.
    """

    column: str
    count: int
    min: Optional[float]
    max: Optional[float]
    std: Optional[float]
    mean: Optional[float]
    missing_pct: Optional[float]
    zero_pct: Optional[float]


@dataclass
class VectorSummary:
    """
    A summary of a categorical column.
    """

    column: str
    count: int
    missing_pct: Optional[float]


@dataclass
class DatasetSummary:
    """
    A summary of a dataset.
    """

    categorical: List[CategoricalSummary]
    numerical: List[NumericalSummary]
    vector: List[VectorSummary]

    def to_dataframes(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Convert this summary into two pandas DataFrames: (numerical_df, categorical_df, vector_df).

        Returns:
            A tuple of (numerical_df, categorical_df)
        """

        numerical_records = [
            {
                "feature": n.column,
                "count": n.count,
                "min": n.min,
                "max": n.max,
                "std": n.std,
                "mean": n.mean,
                "missing_pct": n.missing_pct,
                "zero_pct": n.zero_pct,
            }
            for n in self.numerical
        ]
        categorical_records = [
            {
                "feature": c.column,
                "count": c.count,
                "missing_pct": c.missing_pct,
            }
            for c in self.categorical
        ]
        vector_records = [
            {
                "feature": v.column,
                "count": v.count,
                "missing_pct": v.missing_pct,
            }
            for v in self.vector
        ]

        numerical_df = pd.DataFrame(numerical_records)
        categorical_df = pd.DataFrame(categorical_records)
        vector_df = pd.DataFrame(vector_records)

        # Stable column order
        if not numerical_df.empty:
            numerical_df = numerical_df[
                [
                    "feature",
                    "count",
                    "min",
                    "max",
                    "std",
                    "mean",
                    "missing_pct",
                    "zero_pct",
                ]
            ]
        if not categorical_df.empty:
            # Reflect new CategoricalSummary fields including top percentage
            categorical_df = categorical_df[
                ["feature", "count", "missing_pct", "top", "top_freq", "top_pct"]
            ]

        if not vector_df.empty:
            vector_df = vector_df[["feature", "count", "missing_pct"]]

        return numerical_df, categorical_df, vector_df


def _convert_raw_statistics_to_summary(
    numerical_cols: List[str],
    categorical_cols: List[str],
    vector_cols: List[str],
    raw_statistics: Dict[str, Any],
) -> DatasetSummary:
    """
    Convert raw statistics to a DatasetSummary.
    """
    categorical_summaries: List[CategoricalSummary] = []
    numerical_summaries: List[NumericalSummary] = []
    vector_summaries: List[VectorSummary] = []

    for feature_name in categorical_cols:
        missing_pct = raw_statistics.get(f"missing_pct({feature_name})", None)
        count = raw_statistics.get(f"count({feature_name})", 0)

        categorical_summary = CategoricalSummary(
            feature_name,
            count,
            missing_pct,
        )
        categorical_summaries.append(categorical_summary)

    for feature_name in numerical_cols:
        zero_pct = raw_statistics.get(f"zero_pct({feature_name})", None)
        missing_pct = raw_statistics.get(f"missing_pct({feature_name})", None)

        numerical_summary = NumericalSummary(
            feature_name,
            raw_statistics.get(f"count({feature_name})", 0),
            raw_statistics.get(f"min({feature_name})", None),
            raw_statistics.get(f"max({feature_name})", None),
            raw_statistics.get(f"std({feature_name})", None),
            raw_statistics.get(f"mean({feature_name})", None),
            missing_pct,
            zero_pct,
        )
        numerical_summaries.append(numerical_summary)

    for feature_name in vector_cols:
        missing_pct = raw_statistics.get(f"missing_pct({feature_name})", None)

        vector_summary = VectorSummary(
            feature_name,
            raw_statistics.get(f"count({feature_name})", 0),
            missing_pct,
        )
        vector_summaries.append(vector_summary)

    return DatasetSummary(
        categorical=categorical_summaries,
        numerical=numerical_summaries,
        vector=vector_summaries,
    )


def dataset_summary(
    dataset: "Dataset", columns: Optional[List[str]] = None
) -> DatasetSummary:
    """
    Generate comprehensive statistical summaries for all column types in a Ray dataset.

    This function analyzes the dataset and produces tailored statistics based on column types:

    **Numerical columns**: count, min, max, std, median, mean, missing percentage, zero percentage
    **Categorical columns**: count, missing percentage, most frequent value (top), frequency and percentage of top value
    **Vector columns**: count, missing percentage

    The function automatically detects column types and applies appropriate aggregations using Ray's
    distributed computing capabilities for efficient processing of large datasets.

    Args:
        dataset: A Ray Dataset instance containing the data to summarize
        columns: Optional list of specific column names to include in the summary.
                If None (default), all columns in the dataset will be analyzed.

    Returns:
        DatasetSummary: A structured summary object containing separate lists for numerical,
                       categorical, and vector column statistics. The summary can be converted
                       to pandas DataFrames using the `.to_dataframes()` method or visualized
                       using the `.visualize()` method.

    Example:
        >>> import ray
        >>> dataset = ray.data.from_pandas(df)
        >>> summary = dataset_summary(dataset)
        >>> numerical_df, categorical_df, vector_df = summary.to_dataframes()
        >>> artifact = summary.visualize()
    """
    (
        numerical_cols,
        string_cols,
        vector_cols,
        feature_aggregators,
    ) = feature_aggregators_for_dataset(dataset, columns)
    raw_statistics = dataset.aggregate(*feature_aggregators)

    return _convert_raw_statistics_to_summary(
        numerical_cols, string_cols, vector_cols, raw_statistics
    )
