from __future__ import annotations

import math
from collections import Counter
from numbers import Number
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors.gpu._aggregates import GPUOrdinalValueCounter
from ray.data.preprocessors.gpu._runtime import _GPUTransformContext
from ray.data.preprocessors.gpu.base import _DEFAULT_GPU_BATCH_SIZE, GPUPreprocessor
from ray.data.preprocessors.scaler import _EPSILON
from ray.data.preprocessors.utils import _Computed, _PublicField, migrate_private_fields
from ray.data.preprocessors.version_support import SerializablePreprocessor
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import cudf

    from ray.data.dataset import Dataset

_DEFAULT_WORD_PATTERN = r"[A-Za-z0-9]+(?:[-'][A-Za-z0-9]+)?"
_DEFAULT_TOKEN_PATTERN = r"[A-Za-z]+|[0-9]+|[^A-Za-z0-9\s]"
_GPU_ORDINAL_FIT_NUM_PARTITIONS = 256


def _append_hashing_columns_from_tokens(
    df: cudf.DataFrame,
    tokens: cudf.Series,
    lengths: cudf.Series,
    output_col: str,
    num_features: int,
) -> cudf.DataFrame:
    """Append dense hashing-vectorizer columns from tokenized cuDF strings."""
    import cudf
    import cupy as cp

    lengths = lengths.fillna(0).astype("int32")
    lengths_gpu = lengths.to_cupy()
    dense = cp.zeros((len(df), num_features), dtype=cp.int32)
    total_tokens = int(lengths.sum())
    if total_tokens:
        leaves = tokens.list.leaves
        flat_tokens = leaves() if callable(leaves) else leaves
        try:
            hashed = flat_tokens.hash_values(method="murmur3")
        except TypeError:
            hashed = flat_tokens.hash_values()
        token_hashes = hashed.astype("uint64").to_cupy() % num_features
        offsets = cp.cumsum(lengths_gpu)
        token_offsets = cp.arange(total_tokens, dtype=cp.int64)
        row_ids = cp.searchsorted(offsets, token_offsets, side="right").astype(cp.int32)
        cp.add.at(dense, (row_ids, token_hashes.astype(cp.int32)), 1)
    for idx in range(num_features):
        df[f"{output_col}_{idx}"] = cudf.Series(dense[:, idx], index=df.index)
    return df


def _is_missing_value(value: Any) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    try:
        return bool(missing)
    except (TypeError, ValueError):
        return False


def _ordinal_map_from_stats(stats: Dict[str, Any], column: str) -> Dict[Any, int]:
    stat_value = stats[f"unique_values({column})"]
    if isinstance(stat_value, dict):
        return stat_value
    keys_array, values_array = stat_value
    return {key.as_py(): value.as_py() for key, value in zip(keys_array, values_array)}


def _str_count(series: cudf.Series, pattern: str) -> cudf.Series:
    try:
        return series.str.count(pattern)
    except AttributeError:
        return series.str.count_re(pattern)


@PublicAPI(stability="alpha")
@SerializablePreprocessor(
    version=1, identifier="io.ray.preprocessors.gpu_column_caster"
)
class GPUColumnCaster(GPUPreprocessor):
    """Cast one or more columns inside a fused GPU preprocessing stage."""

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        output_dtype: Any,
        *,
        output_columns: Optional[List[str]] = None,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )
        self._columns = columns
        if isinstance(output_dtype, list):
            if len(output_dtype) != len(columns):
                raise ValueError(
                    f"Expected {len(columns)} values, but got "
                    f"{len(output_dtype)} values: {output_dtype!r}."
                )
            self._output_dtypes = output_dtype
        else:
            self._output_dtypes = [output_dtype for _ in columns]
        self._output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def output_columns(self) -> List[str]:
        return self._output_columns

    @property
    def output_dtypes(self) -> List[Any]:
        return self._output_dtypes

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Cast configured columns to their requested cuDF dtypes."""
        cast_map = dict(zip(self._columns, self._output_dtypes))
        casted = df[self._columns].astype(cast_map)
        casted.columns = self._output_columns
        df[list(self._output_columns)] = casted
        return df

    def get_input_columns(self) -> List[str]:
        return list(self._columns)

    def get_output_columns(self) -> List[str]:
        return list(self._output_columns)

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            **super()._get_serializable_fields(),
            "columns": self._columns,
            "output_dtypes": self._output_dtypes,
            "output_columns": self._output_columns,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        super()._set_serializable_fields(fields, version)
        self._columns = fields["columns"]
        self._output_dtypes = fields["output_dtypes"]
        self._output_columns = fields["output_columns"]

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(columns={self._columns!r}, "
            f"output_dtypes={self._output_dtypes!r}, "
            f"output_columns={self._output_columns!r})"
        )


@PublicAPI(stability="alpha")
@SerializablePreprocessor(
    version=1, identifier="io.ray.preprocessors.gpu_column_dropper"
)
class GPUColumnDropper(GPUPreprocessor):
    """Drop columns inside a fused GPU preprocessing stage."""

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        *,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )
        self._columns = columns

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Drop configured columns that are present in the batch."""
        existing = [column for column in self._columns if column in df.columns]
        if not existing:
            return df
        return df.drop(columns=existing)

    def get_input_columns(self) -> List[str]:
        return list(self._columns)

    def get_output_columns(self) -> List[str]:
        return []

    def _gpu_modified_columns(self) -> List[str]:
        return list(self._columns)

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {**super()._get_serializable_fields(), "columns": self._columns}

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        super()._set_serializable_fields(fields, version)
        self._columns = fields["columns"]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(columns={self._columns!r})"


@PublicAPI(stability="alpha")
@SerializablePreprocessor(
    version=1, identifier="io.ray.preprocessors.gpu_hashing_vectorizer"
)
class GPUHashingVectorizer(GPUPreprocessor):
    """GPU hashing vectorizer that emits dense scalar feature columns."""

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        num_features: int,
        *,
        token_pattern: str = _DEFAULT_TOKEN_PATTERN,
        output_columns: Optional[List[str]] = None,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )
        if num_features <= 0:
            raise ValueError("num_features must be positive")
        self._columns = columns
        self._num_features = num_features
        self._token_pattern = token_pattern
        self._output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Tokenize string columns and append dense hashed-count features."""
        context = context or _GPUTransformContext()
        for input_col, output_col in zip(self._columns, self._output_columns):
            tokens, lengths = context.tokenized_text(df, input_col, self._token_pattern)
            df = _append_hashing_columns_from_tokens(
                df, tokens, lengths, output_col, self._num_features
            )
            if output_col == input_col and input_col in df.columns:
                df = df.drop(columns=[input_col])
        return df

    def _gpu_modified_columns(self) -> List[str]:
        modified = self.get_output_columns()
        modified.extend(
            input_col
            for input_col, output_col in zip(self._columns, self._output_columns)
            if input_col == output_col
        )
        return modified

    def get_input_columns(self) -> List[str]:
        return list(self._columns)

    def get_output_columns(self) -> List[str]:
        return [
            f"{output_col}_{idx}"
            for output_col in self._output_columns
            for idx in range(self._num_features)
        ]

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            **super()._get_serializable_fields(),
            "columns": self._columns,
            "num_features": self._num_features,
            "token_pattern": self._token_pattern,
            "output_columns": self._output_columns,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        super()._set_serializable_fields(fields, version)
        self._columns = fields["columns"]
        self._num_features = fields["num_features"]
        self._token_pattern = fields.get("token_pattern", _DEFAULT_TOKEN_PATTERN)
        self._output_columns = fields["output_columns"]

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(columns={self._columns!r}, "
            f"num_features={self._num_features!r}, "
            f"output_columns={self._output_columns!r})"
        )


@PublicAPI(stability="alpha")
@SerializablePreprocessor(
    version=1, identifier="io.ray.preprocessors.gpu_one_hot_encoder"
)
class GPUOneHotEncoder(GPUPreprocessor):
    """GPU-native one-hot encoder that emits dense scalar indicator columns."""

    _is_fittable = True

    def __init__(
        self,
        columns: List[str],
        *,
        max_categories: Optional[Dict[str, int]] = None,
        output_columns: Optional[List[str]] = None,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )
        self._columns = columns
        self._max_categories = max_categories or {}
        self._output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )
        self._gpu_maps: Dict[str, Dict[Any, int]] = {}

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def output_columns(self) -> List[str]:
        return self._output_columns

    def _supports_gpu_combined_fit(self) -> bool:
        return True

    def _gpu_fit_stats_cudf(self, df: cudf.DataFrame) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for column in self._columns:
            counts = df[column].value_counts(dropna=False)
            keys = counts.index.to_pandas().tolist()
            vals = counts.to_pandas().tolist()
            rows.extend(
                {"column": column, "value": key, "count": int(value)}
                for key, value in zip(keys, vals)
                if not _is_missing_value(key) and not _is_missing_value(value)
            )
        return pd.DataFrame(rows, columns=["column", "value", "count"])

    def _finalize_gpu_fit_stats(self, partials: pd.DataFrame) -> None:
        """Combine category counts into deterministic one-hot mappings."""
        counters: Dict[str, Counter] = {column: Counter() for column in self._columns}
        if not partials.empty:
            for column, value, count in partials[
                ["column", "value", "count"]
            ].itertuples(index=False, name=None):
                if column not in counters:
                    continue
                if _is_missing_value(value) or _is_missing_value(count):
                    continue
                counters[column][value] += int(count)

        self.stats_ = {}
        for column in self._columns:
            counter = counters[column]
            if column in self._max_categories:
                values = list(dict(counter.most_common(self._max_categories[column])))
            else:
                values = list(counter.keys())
            self.stats_[f"unique_values({column})"] = {
                value: index for index, value in enumerate(sorted(values))
            }
        self._gpu_maps = {}

    def _fit_gpu(
        self, dataset: "Dataset", prefix: Sequence[GPUPreprocessor]
    ) -> "GPUOneHotEncoder":
        return self._fit_gpu_with_stats(dataset, prefix)

    def _prepare_gpu_state(self) -> None:
        self._gpu_maps = {
            column: self.stats_.get(f"unique_values({column})", {})
            for column in self._columns
        }

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Expand categorical columns into one-hot indicator columns."""
        for input_col, output_col in zip(self._columns, self._output_columns):
            if bool(df[input_col].isnull().any()):
                raise ValueError(
                    f"Unable to transform column {input_col!r} because it contains "
                    "null values. Consider imputing missing values first."
                )
            mapping = (
                self._gpu_maps.get(input_col)
                or self.stats_[f"unique_values({input_col})"]
            )
            codes = df[input_col].map(mapping).fillna(-1).astype("int32")
            for idx in range(len(mapping)):
                df[f"{output_col}_{idx}"] = (codes == idx).astype("uint8")
            if output_col == input_col and input_col in df.columns:
                df = df.drop(columns=[input_col])
        return df

    def get_input_columns(self) -> List[str]:
        return list(self._columns)

    def get_output_columns(self) -> List[str]:
        outputs: List[str] = []
        for input_col, output_col in zip(self._columns, self._output_columns):
            mapping = self.stats_.get(f"unique_values({input_col})")
            if mapping is None:
                outputs.append(output_col)
            else:
                outputs.extend(f"{output_col}_{idx}" for idx in range(len(mapping)))
        return outputs

    def _gpu_modified_columns(self) -> List[str]:
        modified = self.get_output_columns()
        modified.extend(
            input_col
            for input_col, output_col in zip(self._columns, self._output_columns)
            if input_col == output_col
        )
        return modified

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            **super()._get_serializable_fields(),
            "columns": self._columns,
            "max_categories": self._max_categories,
            "output_columns": self._output_columns,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        super()._set_serializable_fields(fields, version)
        self._columns = fields["columns"]
        self._max_categories = fields.get("max_categories", {})
        self._output_columns = fields["output_columns"]
        self._gpu_maps = {}

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(columns={self._columns!r}, "
            f"output_columns={self._output_columns!r})"
        )


@PublicAPI(stability="alpha")
@SerializablePreprocessor(
    version=1, identifier="io.ray.preprocessors.gpu_ordinal_encoder"
)
class GPUOrdinalEncoder(GPUPreprocessor):
    """GPU-native variant of :class:`~ray.data.preprocessors.OrdinalEncoder` for scalar columns."""

    _is_fittable = True

    def __init__(
        self,
        columns: List[str],
        *,
        encode_lists: bool = True,
        output_columns: Optional[List[str]] = None,
        unknown_value: Optional[Number] = None,
        encoded_missing_value: Optional[Number] = None,
        output_dtype: Optional[Any] = None,
        encoded_value_offset: int = 0,
        min_evidence: int = 1,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )
        if min_evidence < 1:
            raise ValueError(
                f"`min_evidence` must be a positive integer, got {min_evidence!r}."
            )
        self._columns = columns
        self._encode_lists = encode_lists
        self._output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )
        self._unknown_value = unknown_value
        self._encoded_missing_value = encoded_missing_value
        self._output_dtype = output_dtype
        self._encoded_value_offset = encoded_value_offset
        self._min_evidence = min_evidence
        self._gpu_maps: Dict[str, Dict[Any, int]] = {}

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def encode_lists(self) -> bool:
        return self._encode_lists

    @property
    def output_columns(self) -> List[str]:
        return self._output_columns

    @property
    def unknown_value(self) -> Optional[Number]:
        return self._unknown_value

    @property
    def encoded_missing_value(self) -> Optional[Number]:
        return self._encoded_missing_value

    @property
    def output_dtype(self) -> Optional[Any]:
        return self._output_dtype

    @property
    def encoded_value_offset(self) -> int:
        return self._encoded_value_offset

    @property
    def min_evidence(self) -> int:
        return self._min_evidence

    def _supports_gpu_combined_fit(self) -> bool:
        # Category counts require a grouped GPU shuffle. They can't share the
        # serialized, driver-finalized statistics path used by scalar fit stats.
        return False

    def _fit_gpu(
        self, dataset: "Dataset", prefix: Sequence[GPUPreprocessor]
    ) -> "GPUOrdinalEncoder":
        from ray.data.context import ShuffleStrategy
        from ray.data.grouped_data import GroupedData

        if dataset.context.shuffle_strategy != ShuffleStrategy.GPU_SHUFFLE:
            raise ValueError(
                "GPUOrdinalEncoder fitting requires "
                "DataContext.shuffle_strategy=ShuffleStrategy.GPU_SHUFFLE so "
                "global category counts remain distributed on GPUs."
            )

        # These group keys are produced by GPUOrdinalValueCounter's local
        # aggregation, so they intentionally aren't columns in the input dataset.
        # Keep the shuffle fanout independent of the number of input blocks. The
        # production-cardinality dataset has thousands of 128 MiB input blocks;
        # using that count here makes RAPIDS MPF create millions of tiny packed
        # fragments. 256 partitions keep individual high-cardinality reducer
        # partitions bounded while avoiding that per-input fragmentation.
        retained = GroupedData(
            dataset,
            ["column", "value"],
            num_partitions=_GPU_ORDINAL_FIT_NUM_PARTITIONS,
        ).aggregate(
            GPUOrdinalValueCounter(
                self._columns,
                prefix=tuple(prefix),
                min_evidence=self._min_evidence,
                input_batch_rows=self._batch_size,
            )
        )

        retained_by_column: Dict[str, List[Any]] = {
            column: [] for column in self._columns
        }
        for batch in retained.iter_batches(batch_size=None, batch_format="pandas"):
            if batch.empty:
                continue
            for column, value in batch[["column", "value"]].itertuples(
                index=False, name=None
            ):
                if column in retained_by_column and not _is_missing_value(value):
                    retained_by_column[column].append(value)

        self.stats_ = {}
        for column in self._columns:
            self.stats_[f"unique_values({column})"] = {
                value: index + self._encoded_value_offset
                for index, value in enumerate(sorted(retained_by_column[column]))
            }
        self._gpu_maps = {}
        return self

    def _gpu_fit_stats_cudf(self, df: cudf.DataFrame) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for column in self._columns:
            if bool(df[column].isnull().any()) and self._encoded_missing_value is None:
                raise ValueError(
                    "Unable to fit column because it contains null values. "
                    "Consider imputing missing values first."
                )
            counts = df[column].dropna().value_counts(dropna=False)
            keys = counts.index.to_pandas().tolist()
            values = counts.to_pandas().tolist()
            rows.extend(
                {"column": column, "value": key, "count": int(value)}
                for key, value in zip(keys, values)
                if not _is_missing_value(key) and not _is_missing_value(value)
            )
        return pd.DataFrame(rows, columns=["column", "value", "count"])

    def _finalize_gpu_fit_stats(self, partials: pd.DataFrame) -> None:
        """Combine category counts into deterministic ordinal mappings."""
        if partials.empty:
            retained_by_column: Dict[str, List[Any]] = {}
        else:
            counts = partials[["column", "value", "count"]]
            counts = counts[
                counts["column"].isin(self._columns)
                & counts["value"].notna()
                & counts["count"].notna()
            ].copy()
            counts["count"] = counts["count"].astype("int64", copy=False)
            totals = counts.groupby(
                ["column", "value"],
                as_index=False,
                observed=True,
                sort=False,
            )["count"].sum()
            totals = totals[totals["count"] >= self._min_evidence]
            retained_by_column = {
                column: values.tolist()
                for column, values in totals.groupby(
                    "column", observed=True, sort=False
                )["value"]
            }

        self.stats_ = {}
        for column in self._columns:
            self.stats_[f"unique_values({column})"] = {
                value: index + self._encoded_value_offset
                for index, value in enumerate(
                    sorted(retained_by_column.get(column, ()))
                )
            }
        self._gpu_maps = {}

    def _prepare_gpu_state(self) -> None:
        self._gpu_maps = {
            column: _ordinal_map_from_stats(self.stats_, column)
            for column in self._columns
        }

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Map configured categorical columns to fitted ordinal codes."""
        if self._encode_lists:
            for column in self._columns:
                if hasattr(df[column].dtype, "element_type"):
                    raise ValueError(
                        "GPUOrdinalEncoder doesn't support list columns yet."
                    )

        output_columns: List[str] = []
        encoded_columns: List[Any] = []
        for input_col, output_col in zip(self._columns, self._output_columns):
            missing = df[input_col].isnull()
            if bool(missing.any()) and self._encoded_missing_value is None:
                raise ValueError(
                    f"Unable to transform column {input_col!r} because it contains "
                    "null values. Consider imputing missing values first."
                )

            mapping = self._gpu_maps.get(input_col) or _ordinal_map_from_stats(
                self.stats_, input_col
            )
            codes = df[input_col].map(mapping)
            unknown = codes.isnull() & ~missing
            if self._unknown_value is not None:
                codes = codes.mask(unknown, self._unknown_value)
            if self._encoded_missing_value is not None:
                codes = codes.mask(missing, self._encoded_missing_value)
            if self._output_dtype is not None:
                codes = codes.astype(self._output_dtype)
            output_columns.append(output_col)
            encoded_columns.append(codes)

        import cudf

        encoded = cudf.DataFrame(dict(zip(output_columns, encoded_columns)))
        encoded.index = df.index
        df[list(output_columns)] = encoded
        return df

    def get_input_columns(self) -> List[str]:
        return list(self._columns)

    def get_output_columns(self) -> List[str]:
        return list(self._output_columns)

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            **super()._get_serializable_fields(),
            "columns": self._columns,
            "encode_lists": self._encode_lists,
            "output_columns": self._output_columns,
            "unknown_value": self._unknown_value,
            "encoded_missing_value": self._encoded_missing_value,
            "output_dtype": self._output_dtype,
            "encoded_value_offset": self._encoded_value_offset,
            "min_evidence": self._min_evidence,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        super()._set_serializable_fields(fields, version)
        self._columns = fields["columns"]
        self._encode_lists = fields.get("encode_lists", True)
        self._output_columns = fields["output_columns"]
        self._unknown_value = fields.get("unknown_value")
        self._encoded_missing_value = fields.get("encoded_missing_value")
        self._output_dtype = fields.get("output_dtype")
        self._encoded_value_offset = fields.get("encoded_value_offset", 0)
        self._min_evidence = fields.get("min_evidence", 1)
        self._gpu_maps = {}

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(columns={self._columns!r}, "
            f"encode_lists={self._encode_lists!r}, "
            f"output_columns={self._output_columns!r}, "
            f"unknown_value={self._unknown_value!r}, "
            f"encoded_missing_value={self._encoded_missing_value!r}, "
            f"output_dtype={self._output_dtype!r}, "
            f"encoded_value_offset={self._encoded_value_offset!r}, "
            f"min_evidence={self._min_evidence!r})"
        )


def _power_transform_series(
    values: cudf.Series, power: float, method: str
) -> cudf.Series:
    """Apply a Yeo-Johnson or Box-Cox transform to a cuDF series."""
    if method == "yeo-johnson":
        positive = values >= 0
        if power != 0:
            positive_values = ((values + 1).pow(power) - 1) / power
        else:
            positive_values = np.log(values + 1)

        if power != 2:
            negative_values = -((-values + 1).pow(2 - power) - 1) / (2 - power)
        else:
            negative_values = -np.log(-values + 1)

        return positive_values.where(positive, negative_values)

    if power != 0:
        return (values.pow(power) - 1) / power
    return np.log(values)


def _power_transform_values(
    values: cudf.DataFrame,
    power: float,
    method: str,
) -> cudf.DataFrame:
    """Apply a Yeo-Johnson or Box-Cox transform to a numeric cuDF frame."""
    import cudf

    return cudf.DataFrame(
        {
            column: _power_transform_series(values[column], power, method)
            for column in values.columns
        },
        index=values.index,
    )


def _standard_scale_values(
    values: cudf.DataFrame,
    columns: Sequence[str],
    stats: Dict[str, Any],
) -> cudf.DataFrame:
    """Standardize columns with fitted mean/std statistics."""
    import cudf

    scaled = {}
    for column in columns:
        mean = stats.get(f"mean({column})")
        std = stats.get(f"std({column})")
        mean_value = float(mean) if mean is not None else float("nan")
        std_value = float(std) if std is not None and std >= _EPSILON else 1.0
        scaled[column] = (values[column] - mean_value) / std_value
    return cudf.DataFrame(scaled, index=values.index)


@PublicAPI(stability="alpha")
@SerializablePreprocessor(
    version=1, identifier="io.ray.preprocessors.gpu_power_transformer"
)
class GPUPowerTransformer(GPUPreprocessor):
    """GPU-native variant of :class:`~ray.data.preprocessors.PowerTransformer`."""

    _valid_methods = ["yeo-johnson", "box-cox"]
    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        power: float,
        method: str = "yeo-johnson",
        *,
        output_columns: Optional[List[str]] = None,
        output_dtype: Optional[Any] = None,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )
        if method not in self._valid_methods:
            raise ValueError(
                f"Method {method} is not supported."
                f"Supported values are: {self._valid_methods}"
            )
        self._columns = columns
        self._method = method
        self._power = power
        self._output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )
        self._output_dtype = output_dtype

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def method(self) -> str:
        return self._method

    @property
    def power(self) -> float:
        return self._power

    @property
    def output_columns(self) -> List[str]:
        return self._output_columns

    @property
    def output_dtype(self) -> Optional[Any]:
        return self._output_dtype

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Apply the configured Yeo-Johnson or Box-Cox power transform."""
        transformed = _power_transform_values(
            df[self._columns].astype("float64"), self._power, self._method
        )
        if self._output_dtype is not None:
            transformed = transformed.astype(self._output_dtype)
        output = transformed.copy(deep=False)
        output.columns = list(self._output_columns)
        df[list(self._output_columns)] = output
        return df

    def get_input_columns(self) -> List[str]:
        return list(self._columns)

    def get_output_columns(self) -> List[str]:
        return list(self._output_columns)

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            **super()._get_serializable_fields(),
            "columns": self._columns,
            "power": self._power,
            "method": self._method,
            "output_columns": self._output_columns,
            "output_dtype": self._output_dtype,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        super()._set_serializable_fields(fields, version)
        self._columns = fields["columns"]
        self._power = fields["power"]
        self._method = fields.get("method", "yeo-johnson")
        self._output_columns = fields["output_columns"]
        self._output_dtype = fields.get("output_dtype")

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(columns={self._columns!r}, "
            f"power={self._power!r}, method={self._method!r}, "
            f"output_columns={self._output_columns!r}, "
            f"output_dtype={self._output_dtype!r})"
        )


@PublicAPI(stability="alpha")
@SerializablePreprocessor(
    version=1, identifier="io.ray.preprocessors.gpu_simple_imputer"
)
class GPUSimpleImputer(GPUPreprocessor):
    """GPU-native variant of :class:`~ray.data.preprocessors.SimpleImputer`."""

    _valid_strategies = ["mean", "most_frequent", "constant"]

    def __init__(
        self,
        columns: List[str],
        strategy: str = "mean",
        fill_value: Optional[Number] = None,
        *,
        output_columns: Optional[List[str]] = None,
        output_dtype: Optional[Any] = None,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )
        if strategy not in self._valid_strategies:
            raise ValueError(
                f"Strategy {strategy} is not supported."
                f"Supported values are: {self._valid_strategies}"
            )
        if strategy == "constant" and fill_value is None:
            raise ValueError('`fill_value` must be set when using "constant" strategy.')
        self._is_fittable = strategy != "constant"
        self._columns = columns
        self._strategy = strategy
        self._fill_value = fill_value
        self._output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )
        self._output_dtype = output_dtype

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def strategy(self) -> str:
        return self._strategy

    @property
    def fill_value(self) -> Optional[Number]:
        return self._fill_value

    @property
    def output_columns(self) -> List[str]:
        return self._output_columns

    @property
    def output_dtype(self) -> Optional[Any]:
        return self._output_dtype

    def _supports_gpu_combined_fit(self) -> bool:
        return self._strategy in {"mean", "most_frequent"}

    def _gpu_fit_stats_cudf(self, df: cudf.DataFrame) -> pd.DataFrame:
        """Compute per-batch mean or most-frequent imputation statistics."""
        if self._strategy == "mean":
            numeric = df[self._columns].astype("float64")
            counts = numeric.count().to_pandas().to_dict()
            sums = numeric.sum().to_pandas().to_dict()
            rows = [
                {
                    "column": column,
                    "count": int(counts.get(column, 0) or 0),
                    "sum": float(sums.get(column, 0.0) or 0.0),
                }
                for column in self._columns
            ]
            return pd.DataFrame(rows, columns=["column", "count", "sum"])

        rows: List[Dict[str, Any]] = []
        for column in self._columns:
            counts = df[column].value_counts(dropna=False)
            keys = counts.index.to_pandas().tolist()
            values = counts.to_pandas().tolist()
            rows.extend(
                {"column": column, "value": key, "count": int(value)}
                for key, value in zip(keys, values)
                if not _is_missing_value(key) and not _is_missing_value(value)
            )
        return pd.DataFrame(rows, columns=["column", "value", "count"])

    def _finalize_gpu_fit_stats(self, partials: pd.DataFrame) -> None:
        """Combine partials into one fitted fill value per input column."""
        self.stats_ = {}
        if self._strategy == "mean":
            counts = {column: 0 for column in self._columns}
            sums = {column: 0.0 for column in self._columns}
            if not partials.empty:
                for column, count, col_sum in partials[
                    ["column", "count", "sum"]
                ].itertuples(index=False, name=None):
                    if column not in counts or _is_missing_value(count):
                        continue
                    counts[column] += int(count)
                    sums[column] += float(col_sum)
            for column in self._columns:
                count = counts[column]
                self.stats_[f"mean({column})"] = sums[column] / count if count else None
            return

        counters: Dict[str, Counter] = {column: Counter() for column in self._columns}
        if not partials.empty:
            for column, value, count in partials[
                ["column", "value", "count"]
            ].itertuples(index=False, name=None):
                if column in counters and not _is_missing_value(value):
                    counters[column][value] += int(count)
        for column in self._columns:
            self.stats_[f"most_frequent({column})"] = (
                counters[column].most_common(1)[0][0] if counters[column] else None
            )

    def _fit_gpu(
        self, dataset: "Dataset", prefix: Sequence[GPUPreprocessor]
    ) -> "GPUSimpleImputer":
        return self._fit_gpu_with_stats(dataset, prefix)

    def _get_fill_value(self, column: str) -> Any:
        if self._strategy == "mean":
            return self.stats_[f"mean({column})"]
        if self._strategy == "most_frequent":
            return self.stats_[f"most_frequent({column})"]
        if self._strategy == "constant":
            return self._fill_value
        raise ValueError(
            f"Strategy {self._strategy} is not supported. "
            f"Supported values are: {self._valid_strategies}"
        )

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Fill nulls in configured columns and write the requested outputs."""
        existing_columns: List[str] = []
        existing_outputs: List[str] = []
        fill_values: Dict[str, Any] = {}
        for column, output_column in zip(self._columns, self._output_columns):
            value = self._get_fill_value(column)
            if value is None:
                raise ValueError(
                    f"Column {column} has no fill value. "
                    "Check the data used to fit the SimpleImputer."
                )
            if column in df.columns:
                existing_columns.append(column)
                existing_outputs.append(output_column)
                fill_values[column] = value
            else:
                df[output_column] = value

        if existing_columns:
            filled = df[existing_columns].copy(deep=False).fillna(fill_values)
            if self._output_dtype is not None:
                filled = filled.astype(self._output_dtype)
            filled.columns = existing_outputs
            df[list(existing_outputs)] = filled
        return df

    def get_input_columns(self) -> List[str]:
        return list(self._columns)

    def get_output_columns(self) -> List[str]:
        return list(self._output_columns)

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            **super()._get_serializable_fields(),
            "columns": self._columns,
            "strategy": self._strategy,
            "fill_value": self._fill_value,
            "output_columns": self._output_columns,
            "output_dtype": self._output_dtype,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        super()._set_serializable_fields(fields, version)
        self._columns = fields["columns"]
        self._strategy = fields["strategy"]
        self._fill_value = fields.get("fill_value")
        self._output_columns = fields["output_columns"]
        self._output_dtype = fields.get("output_dtype")
        self._is_fittable = self._strategy != "constant"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(columns={self._columns!r}, "
            f"strategy={self._strategy!r}, fill_value={self._fill_value!r}, "
            f"output_columns={self._output_columns!r}, "
            f"output_dtype={self._output_dtype!r})"
        )


@PublicAPI(stability="alpha")
@SerializablePreprocessor(
    version=1, identifier="io.ray.preprocessors.gpu_standard_scaler"
)
class GPUStandardScaler(GPUPreprocessor):
    """GPU-native variant of :class:`~ray.data.preprocessors.StandardScaler`."""

    _is_fittable = True

    def __init__(
        self,
        columns: List[str],
        output_columns: Optional[List[str]] = None,
        *,
        output_dtype: Optional[Any] = None,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )
        self._columns = columns
        self._output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )
        self._output_dtype = output_dtype

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def output_columns(self) -> List[str]:
        return self._output_columns

    @property
    def output_dtype(self) -> Optional[Any]:
        return self._output_dtype

    def _supports_gpu_combined_fit(self) -> bool:
        return True

    def _gpu_fit_stats_cudf(self, df: cudf.DataFrame) -> pd.DataFrame:
        numeric = df[self._columns].astype("float64")
        counts = numeric.count().to_pandas().to_dict()
        sums = numeric.sum().to_pandas().to_dict()
        sum_sqs = (numeric * numeric).sum().to_pandas().to_dict()
        rows = [
            {
                "column": column,
                "count": int(counts.get(column, 0) or 0),
                "sum": float(sums.get(column, 0.0) or 0.0),
                "sum_sq": float(sum_sqs.get(column, 0.0) or 0.0),
            }
            for column in self._columns
        ]
        return pd.DataFrame(rows, columns=["column", "count", "sum", "sum_sq"])

    def _finalize_gpu_fit_stats(self, partials: pd.DataFrame) -> None:
        """Combine partial count, sum, and squared-sum statistics."""
        counts = {column: 0 for column in self._columns}
        sums = {column: 0.0 for column in self._columns}
        sum_sqs = {column: 0.0 for column in self._columns}
        if not partials.empty:
            for column, count, col_sum, col_sum_sq in partials[
                ["column", "count", "sum", "sum_sq"]
            ].itertuples(index=False, name=None):
                if column not in counts:
                    continue
                if _is_missing_value(count):
                    continue
                counts[column] += int(count)
                sums[column] += float(col_sum)
                sum_sqs[column] += float(col_sum_sq)

        self.stats_ = {}
        for column in self._columns:
            count = counts[column]
            if count == 0:
                self.stats_[f"mean({column})"] = None
                self.stats_[f"std({column})"] = None
                continue
            mean = sums[column] / count
            variance = max((sum_sqs[column] / count) - (mean * mean), 0.0)
            self.stats_[f"mean({column})"] = mean
            self.stats_[f"std({column})"] = math.sqrt(variance)

    def _fit_gpu(
        self, dataset: "Dataset", prefix: Sequence[GPUPreprocessor]
    ) -> "GPUStandardScaler":
        return self._fit_gpu_with_stats(dataset, prefix)

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Standardize configured columns using fitted GPU statistics."""
        scaled = _standard_scale_values(
            df[self._columns].astype("float64"), self._columns, self.stats_
        )
        if self._output_dtype is not None:
            scaled = scaled.astype(self._output_dtype)
        output = scaled.copy(deep=False)
        output.columns = list(self._output_columns)
        df[list(self._output_columns)] = output
        return df

    def get_input_columns(self) -> List[str]:
        return list(self._columns)

    def get_output_columns(self) -> List[str]:
        return list(self._output_columns)

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            **super()._get_serializable_fields(),
            "columns": self._columns,
            "output_columns": self._output_columns,
            "output_dtype": self._output_dtype,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        super()._set_serializable_fields(fields, version)
        self._columns = fields["columns"]
        self._output_columns = fields["output_columns"]
        self._output_dtype = fields.get("output_dtype")

    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state)
        migrate_private_fields(
            self,
            fields={
                "_columns": _PublicField(public_field="columns"),
                "_output_columns": _PublicField(
                    public_field="output_columns",
                    default=_Computed(lambda obj: obj._columns),
                ),
                "_output_dtype": _PublicField(
                    public_field="output_dtype",
                    default=None,
                ),
            },
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(columns={self._columns!r}, "
            f"output_columns={self._output_columns!r}, "
            f"output_dtype={self._output_dtype!r})"
        )


@PublicAPI(stability="alpha")
@SerializablePreprocessor(version=1, identifier="io.ray.preprocessors.gpu_text_stats")
class GPUTextStatsPreprocessor(GPUPreprocessor):
    """Append GPU-computed text count features for string columns."""

    _is_fittable = False

    def __init__(
        self,
        text_column: str,
        *,
        word_pattern: str = _DEFAULT_WORD_PATTERN,
        token_pattern: str = _DEFAULT_TOKEN_PATTERN,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__(
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )
        self._text_column = text_column
        self._word_pattern = word_pattern
        self._token_pattern = token_pattern

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Append word, line, and tokenizer-token counts for one text column."""
        context = context or _GPUTransformContext()
        text = context.string_column(df, self._text_column)
        _, token_lengths = context.tokenized_text(
            df, self._text_column, self._token_pattern
        )
        df["word_count"] = (
            _str_count(text, self._word_pattern).fillna(0).astype("int64")
        )
        newline_count = _str_count(text, r"\n").fillna(0).astype("int64")
        non_empty = text.str.len().fillna(0) > 0
        df["line_count"] = (newline_count + 1).where(non_empty, 0).astype("int64")
        df["tokenizer_token_count"] = token_lengths.astype("int64")
        return df

    def get_input_columns(self) -> List[str]:
        return [self._text_column]

    def get_output_columns(self) -> List[str]:
        return ["word_count", "line_count", "tokenizer_token_count"]

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            **super()._get_serializable_fields(),
            "text_column": self._text_column,
            "word_pattern": self._word_pattern,
            "token_pattern": self._token_pattern,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        super()._set_serializable_fields(fields, version)
        self._text_column = fields["text_column"]
        self._word_pattern = fields.get("word_pattern", _DEFAULT_WORD_PATTERN)
        self._token_pattern = fields.get("token_pattern", _DEFAULT_TOKEN_PATTERN)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(text_column={self._text_column!r})"
