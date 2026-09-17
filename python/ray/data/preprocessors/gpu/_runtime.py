from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

if TYPE_CHECKING:
    import cudf

    from ray.data.preprocessors.gpu.base import GPUPreprocessor


class _GPUTransformContext:
    """Shared state for a fused GPU transform batch.

    GPU preprocessors in a fused chain often reuse intermediate string and token
    representations. The context memoizes those intermediates for one batch and
    invalidates cache entries when a preprocessor modifies their source columns.
    """

    def __init__(self) -> None:
        self._cache: Dict[Tuple[Any, ...], Any] = {}
        self._dependencies: Dict[Tuple[Any, ...], set[str]] = {}

    def get_or_compute(
        self,
        key: Tuple[Any, ...],
        dependencies: Sequence[str],
        compute: Callable[[], Any],
    ) -> Any:
        if key not in self._cache:
            self._cache[key] = compute()
            self._dependencies[key] = set(dependencies)
        return self._cache[key]

    def invalidate_columns(self, columns: Sequence[str]) -> None:
        modified = set(columns)
        if not modified:
            return
        stale_keys = [
            key
            for key, dependencies in self._dependencies.items()
            if dependencies.intersection(modified)
        ]
        for key in stale_keys:
            self._cache.pop(key, None)
            self._dependencies.pop(key, None)

    def string_column(self, df: cudf.DataFrame, column: str) -> cudf.Series:
        return self.get_or_compute(
            ("string_column", column),
            (column,),
            lambda: df[column].fillna("").astype("str"),
        )

    def lowercase_string_column(self, df: cudf.DataFrame, column: str) -> cudf.Series:
        return self.get_or_compute(
            ("lowercase_string_column", column),
            (column,),
            lambda: self.string_column(df, column).str.lower(),
        )

    def tokenized_text(
        self, df: cudf.DataFrame, column: str, pattern: str
    ) -> Tuple[cudf.Series, cudf.Series]:
        def compute() -> Tuple[cudf.Series, cudf.Series]:
            text_lower = self.lowercase_string_column(df, column)
            tokens = text_lower.str.findall(pattern)
            lengths = tokens.list.len
            lengths = lengths() if callable(lengths) else lengths
            return tokens, lengths.fillna(0).astype("int32")

        return self.get_or_compute(
            ("tokenized_text", column, pattern),
            (column,),
            compute,
        )


class _GPUPhysicalOp(ABC):
    """Base class for planned GPU transforms.

    Fused kernels and :class:`_GPUPreprocessorOp` adapters subclass this
    type. Logical GPU preprocessors must be wrapped or fused first.
    """

    @abstractmethod
    def _prepare_gpu_state(self) -> None:
        pass

    @abstractmethod
    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        pass

    @abstractmethod
    def _gpu_modified_columns(self) -> List[str]:
        pass


class _GPUPreprocessorOp(_GPUPhysicalOp):
    """Adapter that runs one logical GPU preprocessor as a physical op."""

    def __init__(self, preprocessor: "GPUPreprocessor") -> None:
        self._preprocessor = preprocessor

    def _prepare_gpu_state(self) -> None:
        self._preprocessor._prepare_gpu_state()

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        return self._preprocessor._transform_cudf(df, context)

    def _gpu_modified_columns(self) -> List[str]:
        return self._preprocessor._gpu_modified_columns()


def _apply_gpu_physical_ops(
    batch: cudf.DataFrame,
    ops: Sequence[_GPUPhysicalOp],
    *,
    prepare: bool = True,
) -> cudf.DataFrame:
    df = batch.copy(deep=False)
    context = _GPUTransformContext()
    for op in ops:
        if prepare:
            op._prepare_gpu_state()
        result = op._transform_cudf(df, context)
        if result is not None:
            df = result
        context.invalidate_columns(op._gpu_modified_columns())
    return df


def _apply_gpu_preprocessors(
    batch: cudf.DataFrame,
    preprocessors: Sequence["GPUPreprocessor"],
    *,
    prepare: bool = True,
) -> cudf.DataFrame:
    """Apply logical preprocessors in order, without any fusion planning."""
    return _apply_gpu_physical_ops(
        batch,
        tuple(_GPUPreprocessorOp(preprocessor) for preprocessor in preprocessors),
        prepare=prepare,
    )
