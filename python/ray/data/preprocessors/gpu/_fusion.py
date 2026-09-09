from __future__ import annotations

from numbers import Number
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

from ray.data.preprocessors.gpu._runtime import (
    _apply_gpu_preprocessors,
    _GPUPhysicalOp,
    _GPUPreprocessorOp,
    _GPUTransformContext,
)
from ray.data.preprocessors.gpu.base import GPUPreprocessor
from ray.data.preprocessors.gpu.ops import (
    GPUColumnCaster,
    GPUOrdinalEncoder,
    GPUPowerTransformer,
    GPUSimpleImputer,
    GPUStandardScaler,
    _ordinal_map_from_stats,
)
from ray.data.preprocessors.scaler import _EPSILON

if TYPE_CHECKING:
    import cudf


class _FusedGPUNumericColumnOp(_GPUPhysicalOp):
    def __init__(self, preprocessors: Sequence["GPUPreprocessor"]):
        self._preprocessors = tuple(preprocessors)
        self._columns = self._preprocessors[0].get_input_columns()
        self._output_columns = self._preprocessors[-1].get_output_columns()

    def _prepare_gpu_state(self) -> None:
        for preprocessor in self._preprocessors:
            preprocessor._prepare_gpu_state()

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Apply compatible numeric transforms to one dense CuPy array."""
        import cudf
        import cupy as cp

        values = df[self._columns].astype("float64").to_cupy(na_value=cp.nan)

        for preprocessor in self._preprocessors:
            if isinstance(preprocessor, GPUPowerTransformer):
                power = preprocessor.power
                if preprocessor.method == "yeo-johnson":
                    positive = values >= 0
                    if power != 0:
                        positive_values = (cp.power(values + 1, power) - 1) / power
                    else:
                        positive_values = cp.log(values + 1)

                    if power != 2:
                        negative_values = -(cp.power(-values + 1, 2 - power) - 1) / (
                            2 - power
                        )
                    else:
                        negative_values = -cp.log(-values + 1)

                    values = cp.where(positive, positive_values, negative_values)
                elif power != 0:
                    values = (cp.power(values, power) - 1) / power
                else:
                    values = cp.log(values)
            elif isinstance(preprocessor, GPUStandardScaler):
                means = cp.asarray(
                    [
                        (
                            float(preprocessor.stats_.get(f"mean({column})"))
                            if preprocessor.stats_.get(f"mean({column})") is not None
                            else cp.nan
                        )
                        for column in preprocessor.columns
                    ],
                    dtype=cp.float64,
                )
                stds = cp.asarray(
                    [
                        (
                            float(preprocessor.stats_.get(f"std({column})"))
                            if preprocessor.stats_.get(f"std({column})") is not None
                            and preprocessor.stats_.get(f"std({column})") >= _EPSILON
                            else 1.0
                        )
                        for column in preprocessor.columns
                    ],
                    dtype=cp.float64,
                )
                values = (values - means) / stds
            elif isinstance(preprocessor, GPUSimpleImputer):
                fill_values: List[Any] = []
                for column in preprocessor.columns:
                    value = preprocessor._get_fill_value(column)
                    if value is None:
                        raise ValueError(
                            f"Column {column} has no fill value. "
                            "Check the data used to fit the SimpleImputer."
                        )
                    fill_values.append(value)
                fill_array = cp.asarray(fill_values, dtype=values.dtype).reshape(1, -1)
                values = cp.where(cp.isnan(values), fill_array, values)
            else:
                raise TypeError(
                    f"Unsupported fused GPU numeric transform: {preprocessor!r}."
                )

            output_dtype = getattr(preprocessor, "output_dtype", None)
            if output_dtype is not None:
                values = values.astype(output_dtype)

        output = cudf.DataFrame(values, columns=list(self._output_columns))
        output.index = df.index
        df[list(self._output_columns)] = output
        return df

    def _gpu_modified_columns(self) -> List[str]:
        return list(self._output_columns)


class _FusedGPUCategoricalOrdinalOp(_GPUPhysicalOp):
    def __init__(
        self,
        preprocessors: Sequence["GPUPreprocessor"],
        caster: Optional["GPUColumnCaster"],
        imputer: Optional["GPUSimpleImputer"],
        encoder: "GPUOrdinalEncoder",
    ):
        self._preprocessors = tuple(preprocessors)
        self._caster = caster
        self._imputer = imputer
        self._encoder = encoder
        self._columns = encoder.get_input_columns()
        self._output_columns = encoder.get_output_columns()
        self._gpu_maps: Dict[str, Dict[Any, int]] = {}

    def _prepare_gpu_state(self) -> None:
        for preprocessor in self._preprocessors:
            preprocessor._prepare_gpu_state()
        self._gpu_maps = {
            column: _ordinal_map_from_stats(self._encoder.stats_, column)
            for column in self._columns
        }

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Apply cast, imputation, and ordinal encoding to one working frame."""
        if any(column not in df.columns for column in self._columns):
            return _apply_gpu_preprocessors(df, self._preprocessors)

        import cudf

        working = df[self._columns].copy(deep=False)

        if self._caster is not None:
            cast_map = dict(zip(self._caster.columns, self._caster.output_dtypes))
            working = working.astype(cast_map)

        if self._imputer is not None:
            fill_values: Dict[str, Any] = {}
            for column in self._imputer.columns:
                value = self._imputer._get_fill_value(column)
                if value is None:
                    raise ValueError(
                        f"Column {column} has no fill value. "
                        "Check the data used to fit the SimpleImputer."
                    )
                fill_values[column] = value
            working = working.fillna(fill_values)
            if self._imputer.output_dtype is not None:
                working = working.astype(self._imputer.output_dtype)

        if self._encoder.encode_lists:
            for column in self._columns:
                if hasattr(working[column].dtype, "element_type"):
                    raise ValueError(
                        "GPUOrdinalEncoder doesn't support list columns yet."
                    )

        output_columns: List[str] = []
        encoded_columns: List[Any] = []
        for input_col, output_col in zip(
            self._encoder.columns, self._encoder.output_columns
        ):
            missing = working[input_col].isnull()
            if bool(missing.any()) and self._encoder.encoded_missing_value is None:
                raise ValueError(
                    f"Unable to transform column {input_col!r} because it contains "
                    "null values. Consider imputing missing values first."
                )

            mapping = self._gpu_maps.get(input_col) or _ordinal_map_from_stats(
                self._encoder.stats_, input_col
            )
            codes = working[input_col].map(mapping)
            unknown = codes.isnull() & ~missing
            if self._encoder.unknown_value is not None:
                codes = codes.mask(unknown, self._encoder.unknown_value)
            if self._encoder.encoded_missing_value is not None:
                codes = codes.mask(missing, self._encoder.encoded_missing_value)
            if self._encoder.output_dtype is not None:
                codes = codes.astype(self._encoder.output_dtype)
            output_columns.append(output_col)
            encoded_columns.append(codes)

        encoded = cudf.DataFrame(dict(zip(output_columns, encoded_columns)))
        encoded.index = df.index
        df[list(output_columns)] = encoded
        return df

    def _gpu_modified_columns(self) -> List[str]:
        return list(self._output_columns)


def _match_fused_numeric_ops(
    preprocessors: Sequence["GPUPreprocessor"],
    start: int,
) -> Optional[Tuple[_FusedGPUNumericColumnOp, int]]:
    """Match a contiguous run of compatible in-place numeric transforms."""
    ops: List[GPUPreprocessor] = []
    columns: Optional[List[str]] = None
    index = start
    while index < len(preprocessors):
        preprocessor = preprocessors[index]
        input_columns = preprocessor.get_input_columns()
        if input_columns != preprocessor.get_output_columns():
            break
        is_numeric_transform = isinstance(
            preprocessor, (GPUPowerTransformer, GPUStandardScaler)
        )
        is_constant_imputer = (
            isinstance(preprocessor, GPUSimpleImputer)
            and preprocessor.strategy == "constant"
            and isinstance(preprocessor.fill_value, Number)
        )
        if not is_numeric_transform and not is_constant_imputer:
            break
        if columns is None:
            columns = input_columns
        elif input_columns != columns:
            break
        ops.append(preprocessor)
        index += 1

    if len(ops) < 2:
        return None
    if not any(isinstance(op, (GPUPowerTransformer, GPUStandardScaler)) for op in ops):
        return None
    return _FusedGPUNumericColumnOp(ops), index


def _match_fused_categorical_ordinal_ops(
    preprocessors: Sequence["GPUPreprocessor"],
    start: int,
) -> Optional[Tuple[_FusedGPUCategoricalOrdinalOp, int]]:
    """Match an optional cast/impute prefix followed by ordinal encoding."""
    ops: List[GPUPreprocessor] = []
    index = start
    caster = None
    imputer = None

    if isinstance(preprocessors[index], GPUColumnCaster):
        caster = preprocessors[index]
        if caster.get_input_columns() != caster.get_output_columns():
            return None
        ops.append(caster)
        index += 1
        if index >= len(preprocessors):
            return None

    if isinstance(preprocessors[index], GPUSimpleImputer):
        imputer = preprocessors[index]
        if imputer.get_input_columns() != imputer.get_output_columns():
            return None
        ops.append(imputer)
        index += 1
        if index >= len(preprocessors):
            return None

    if not isinstance(preprocessors[index], GPUOrdinalEncoder):
        return None

    encoder = preprocessors[index]
    if encoder.get_input_columns() != encoder.get_output_columns():
        return None

    encoder_columns = encoder.get_input_columns()
    if caster is not None and caster.get_input_columns() != encoder_columns:
        return None
    if imputer is not None and imputer.get_input_columns() != encoder_columns:
        return None

    ops.append(encoder)
    if len(ops) < 2:
        return None
    return _FusedGPUCategoricalOrdinalOp(ops, caster, imputer, encoder), index + 1


def _plan_gpu_transform_ops(
    preprocessors: Sequence["GPUPreprocessor"],
) -> Tuple[_GPUPhysicalOp, ...]:
    """Plan logical GPU preprocessors into fused physical transform operations."""
    planned: List[_GPUPhysicalOp] = []
    index = 0
    while index < len(preprocessors):
        categorical_match = _match_fused_categorical_ordinal_ops(preprocessors, index)
        if categorical_match is not None:
            op, index = categorical_match
            planned.append(op)
            continue

        numeric_match = _match_fused_numeric_ops(preprocessors, index)
        if numeric_match is not None:
            op, index = numeric_match
            planned.append(op)
            continue

        planned.append(_GPUPreprocessorOp(preprocessors[index]))
        index += 1
    return tuple(planned)
