from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

from ray.data._internal.gpu_shuffle.hash_aggregate import (
    GPUAggregateFn,
    _cast_cudf_column_dtype,
)
from ray.data.block import Schema
from ray.data.datatype import DataType
from ray.data.preprocessors.gpu._runtime import _apply_gpu_preprocessors

if TYPE_CHECKING:
    import cudf


class GPUOrdinalValueCounter(GPUAggregateFn):
    """Count categorical values locally, then globally merge and threshold them."""

    def __init__(
        self,
        columns: Sequence[str],
        *,
        prefix: Sequence[Any],
        min_evidence: int,
        input_batch_rows: Optional[int] = None,
        local_combine_partials: Optional[int] = 4,
        alias_name: str = "count",
    ) -> None:
        if not columns:
            raise ValueError("columns must not be empty.")
        if min_evidence < 1:
            raise ValueError(f"min_evidence must be positive, got {min_evidence!r}.")
        self.columns = tuple(columns)
        self.prefix = tuple(prefix)
        self.min_evidence = min_evidence
        self.input_batch_rows = input_batch_rows
        self.local_combine_partials = local_combine_partials
        super().__init__(
            alias_name,
            on=None,
            ignore_nulls=True,
            accumulators=("count",),
        )

    def required_input_columns(self) -> Tuple[str, ...]:
        required = list(self.columns)
        for preprocessor in self.prefix:
            for column in preprocessor.get_input_columns():
                if column not in required:
                    required.append(column)
        return tuple(required)

    def generated_key_columns(self) -> Tuple[str, ...]:
        return ("column", "value")

    def supports_local_combine(self) -> bool:
        return True

    def preferred_input_batch_rows(self) -> Optional[int]:
        return self.input_batch_rows

    def max_local_combine_partials(self) -> Optional[int]:
        return self.local_combine_partials

    def combine_partial_aggregates(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
    ) -> cudf.DataFrame:
        acc_col = accumulator_columns[0]
        result = (
            df.groupby(list(key_columns), dropna=False)[acc_col].sum().reset_index()
        )
        _cast_cudf_column_dtype(result, acc_col, DataType.from_numpy("int64"))
        return result[list(key_columns) + [acc_col]]

    def partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        *,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        import cudf as cudf_module

        if tuple(key_columns) != self.generated_key_columns():
            raise ValueError(
                "GPUOrdinalValueCounter requires group keys ('column', 'value'); "
                f"got {key_columns!r}."
            )
        for preprocessor in self.prefix:
            preprocessor._prepare_gpu_state()
        df = _apply_gpu_preprocessors(df, self.prefix)

        acc_col = accumulator_columns[0]
        frames = []
        for column in self.columns:
            values = df[column]
            if bool(values.isnull().any()):
                raise ValueError(
                    "Unable to fit column because it contains null values. "
                    "Consider imputing missing values first."
                )
            counts = values.value_counts(dropna=False).reset_index()
            counts.columns = ["value", acc_col]
            counts.insert(0, "column", column)
            frames.append(counts[["column", "value", acc_col]])
        result = cudf_module.concat(frames, ignore_index=True)
        _cast_cudf_column_dtype(result, acc_col, DataType.from_numpy("int64"))
        return result

    def final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        output_name: str,
    ) -> cudf.DataFrame:
        acc_col = accumulator_columns[0]
        result = (
            df.groupby(list(key_columns), dropna=False)[acc_col].sum().reset_index()
        )
        result = result.rename(columns={result.columns[-1]: output_name})
        result = result[result[output_name] >= self.min_evidence]
        _cast_cudf_column_dtype(result, output_name, DataType.from_numpy("int64"))
        return result[list(key_columns) + [output_name]]

    def _partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        return {
            self._accumulator_columns(accumulator_prefix)[0]: DataType.from_numpy(
                "int64"
            )
        }

    def _final_cudf_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        return {output_name: DataType.from_numpy("int64")}


class GPUPreprocessorFitAggregate(GPUAggregateFn):
    """Fit numeric moments and categorical value counts in one GPU pass.

    The aggregate emits a common, tagged row representation so heterogeneous
    fit statistics can share one distributed hash aggregation. Numeric moments
    are represented as three metric rows (count, sum, and squared sum), while
    categorical values are represented as count rows. All values use one
    float64 accumulator, which exactly represents integer row counts below
    ``2**53``.
    """

    CATEGORY_KIND = 0
    MOMENT_KIND = 1
    COUNT_METRIC = 0
    SUM_METRIC = 1
    SUM_SQ_METRIC = 2
    KEY_COLUMNS = (
        "__fit_kind",
        "__fit_index",
        "column",
        "value",
        "__fit_metric",
    )

    def __init__(
        self,
        *,
        ordinal_entries: Sequence[Tuple[int, Sequence[str], Sequence[Any], int]],
        moment_entries: Sequence[Tuple[int, Sequence[str], Sequence[Any]]],
        input_batch_rows: Optional[int] = None,
        local_combine_partials: Optional[int] = 4,
        alias_name: str = "fit_value",
    ) -> None:
        if not ordinal_entries:
            raise ValueError("At least one ordinal fit entry is required.")
        self.ordinal_entries = tuple(
            (index, tuple(columns), tuple(prefix), min_evidence)
            for index, columns, prefix, min_evidence in ordinal_entries
        )
        self.moment_entries = tuple(
            (index, tuple(columns), tuple(prefix))
            for index, columns, prefix in moment_entries
        )
        self.input_batch_rows = input_batch_rows
        self.local_combine_partials = local_combine_partials
        super().__init__(
            alias_name,
            on=None,
            ignore_nulls=True,
            accumulators=("value",),
        )

    def required_input_columns(self) -> Tuple[str, ...]:
        required: List[str] = []
        entries = [
            (columns, prefix) for _, columns, prefix, _ in self.ordinal_entries
        ] + [(columns, prefix) for _, columns, prefix in self.moment_entries]
        for columns, prefix in entries:
            for column in columns:
                if column not in required:
                    required.append(column)
            for preprocessor in prefix:
                for column in preprocessor.get_input_columns():
                    if column not in required:
                        required.append(column)
        return tuple(required)

    def generated_key_columns(self) -> Tuple[str, ...]:
        return self.KEY_COLUMNS

    def supports_local_combine(self) -> bool:
        return True

    def preferred_input_batch_rows(self) -> Optional[int]:
        return self.input_batch_rows

    def max_local_combine_partials(self) -> Optional[int]:
        return self.local_combine_partials

    def combine_partial_aggregates(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
    ) -> cudf.DataFrame:
        acc_col = accumulator_columns[0]
        result = (
            df.groupby(list(key_columns), dropna=False)[acc_col].sum().reset_index()
        )
        _cast_cudf_column_dtype(result, acc_col, DataType.from_numpy("float64"))
        return result[list(key_columns) + [acc_col]]

    def partial_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        *,
        input_schema: Optional[Schema] = None,
    ) -> cudf.DataFrame:
        import cudf as cudf_module

        if tuple(key_columns) != self.KEY_COLUMNS:
            raise ValueError(
                "GPUPreprocessorFitAggregate requires generated fit keys; "
                f"got {key_columns!r}."
            )

        acc_col = accumulator_columns[0]
        frames: List[cudf.DataFrame] = []
        for fit_index, columns, prefix, _ in self.ordinal_entries:
            transformed = _apply_gpu_preprocessors(df, prefix)
            for column in columns:
                values = transformed[column]
                if bool(values.isnull().any()):
                    raise ValueError(
                        "Unable to fit column because it contains null values. "
                        "Consider imputing missing values first."
                    )
                counts = values.value_counts(dropna=False).reset_index()
                counts.columns = ["value", acc_col]
                counts.insert(0, "column", column)
                counts.insert(0, "__fit_index", fit_index)
                counts.insert(0, "__fit_kind", self.CATEGORY_KIND)
                counts["__fit_metric"] = self.COUNT_METRIC
                frames.append(counts[list(self.KEY_COLUMNS) + [acc_col]])

        for fit_index, columns, prefix in self.moment_entries:
            transformed = _apply_gpu_preprocessors(df, prefix)
            numeric = transformed[list(columns)].astype("float64")
            metrics = (
                (self.COUNT_METRIC, numeric.count()),
                (self.SUM_METRIC, numeric.sum()),
                (self.SUM_SQ_METRIC, (numeric * numeric).sum()),
            )
            for metric, values in metrics:
                stats = values.rename(acc_col).to_frame().reset_index()
                stats.columns = ["column", acc_col]
                stats.insert(0, "__fit_index", fit_index)
                stats.insert(0, "__fit_kind", self.MOMENT_KIND)
                stats["value"] = None
                stats["__fit_metric"] = metric
                frames.append(stats[list(self.KEY_COLUMNS) + [acc_col]])

        result = cudf_module.concat(frames, ignore_index=True)
        _cast_cudf_column_dtype(result, acc_col, DataType.from_numpy("float64"))
        return result

    def final_aggregate(
        self,
        df: cudf.DataFrame,
        key_columns: Tuple[str, ...],
        accumulator_columns: Tuple[str, ...],
        output_name: str,
    ) -> cudf.DataFrame:
        acc_col = accumulator_columns[0]
        result = (
            df.groupby(list(key_columns), dropna=False)[acc_col].sum().reset_index()
        )
        result = result.rename(columns={result.columns[-1]: output_name})
        keep = result["__fit_kind"] == self.MOMENT_KIND
        for fit_index, _, _, min_evidence in self.ordinal_entries:
            keep = keep | (
                (result["__fit_kind"] == self.CATEGORY_KIND)
                & (result["__fit_index"] == fit_index)
                & (result[output_name] >= min_evidence)
            )
        result = result[keep]
        _cast_cudf_column_dtype(result, output_name, DataType.from_numpy("float64"))
        return result[list(key_columns) + [output_name]]

    def _partial_accumulator_dtypes(
        self,
        df: cudf.DataFrame,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        return {
            self._accumulator_columns(accumulator_prefix)[0]: DataType.from_numpy(
                "float64"
            )
        }

    def _final_cudf_dtypes(
        self,
        df: cudf.DataFrame,
        output_name: str,
        accumulator_prefix: str,
        *,
        input_schema: Optional[Schema] = None,
    ) -> Dict[str, Optional[DataType]]:
        return {output_name: DataType.from_numpy("float64")}
