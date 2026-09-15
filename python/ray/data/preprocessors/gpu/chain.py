from __future__ import annotations

import logging
import time
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from ray.data.preprocessor import (
    Preprocessor,
    PreprocessorNotFittedException,
    SerializablePreprocessorBase,
)
from ray.data.preprocessors.gpu._aggregates import GPUPreprocessorFitAggregate
from ray.data.preprocessors.gpu._fusion import _plan_gpu_transform_ops
from ray.data.preprocessors.gpu._runtime import _apply_gpu_physical_ops
from ray.data.preprocessors.gpu.base import (
    _COMBINED_FIT_INDEX_COLUMN,
    _COMBINED_FIT_STATS_COLUMN,
    _DEFAULT_GPU_BATCH_SIZE,
    GPUPreprocessor,
    _gpu_actor_compute_strategy,
    _GPUFitStatsUDF,
)
from ray.data.preprocessors.gpu.ops import (
    _GPU_ORDINAL_FIT_NUM_PARTITIONS,
    GPUOrdinalEncoder,
    GPUPowerTransformer,
    GPUSimpleImputer,
    GPUStandardScaler,
    _is_missing_value,
)
from ray.data.preprocessors.version_support import SerializablePreprocessor
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import cudf

    from ray.data.dataset import Dataset
    from ray.data.preprocessors.chain import Chain

logger = logging.getLogger(__name__)


def use_gpu_accelerator(accelerator: Optional[str]) -> bool:
    """Return whether ``accelerator`` selects the GPU chain implementation."""
    if accelerator is None:
        return False
    if accelerator != "gpu":
        raise ValueError(
            f"Unsupported accelerator {accelerator!r}. Only 'gpu' is supported."
        )
    return True


def gpu_concurrency(
    *,
    num_gpus: Optional[int],
    concurrency: Optional[int],
) -> Optional[int]:
    """Resolve GPU worker concurrency from ``num_gpus`` and ``concurrency``."""
    if num_gpus is None:
        return concurrency
    if num_gpus <= 0:
        raise ValueError("num_gpus must be positive when accelerator='gpu'.")
    if concurrency is not None and concurrency != num_gpus:
        raise ValueError(
            "Specify either num_gpus or concurrency for GPU Chain preprocessing, "
            "or set them to the same value."
        )
    return num_gpus


def gpu_fit(
    chain: "Chain",
    ds: "Dataset",
    *,
    batch_size: Optional[int],
    num_gpus: Optional[int],
    num_gpus_per_worker: float,
    concurrency: Optional[int],
) -> "Chain":
    """Fit a :class:`~ray.data.preprocessors.Chain` through a fused GPU chain."""
    fit_status = chain.fit_status()
    if fit_status in (
        Preprocessor.FitStatus.FITTED,
        Preprocessor.FitStatus.PARTIALLY_FITTED,
    ):
        warnings.warn(
            "`fit` has already been called on the preprocessor (or at least one "
            "contained preprocessors if this is a chain). "
            "All previously fitted state will be overwritten!"
        )

    chain._stat_computation_plan.reset()
    chain.stats_ = {}
    gpu_chain = GPUChain.from_cpu_preprocessors(
        chain.preprocessors,
        batch_size=batch_size,
        num_gpus_per_worker=num_gpus_per_worker,
        concurrency=gpu_concurrency(
            num_gpus=num_gpus,
            concurrency=concurrency,
        ),
        copy_fitted_state=False,
    )
    gpu_chain.fit(ds)
    gpu_chain.copy_fitted_state_to(chain.preprocessors)
    chain._gpu_chain = gpu_chain
    chain._fitted = True
    return chain


def gpu_transform(
    chain: "Chain",
    ds: "Dataset",
    *,
    batch_size: Optional[int],
    num_cpus: Optional[float],
    memory: Optional[float],
    concurrency: Optional[int],
    num_gpus: Optional[int],
    num_gpus_per_worker: float,
) -> "Dataset":
    """Transform a dataset with a :class:`~ray.data.preprocessors.Chain` on GPU."""
    if num_cpus is not None:
        raise ValueError("GPU Chain preprocessing does not support num_cpus.")
    if memory is not None:
        raise ValueError("GPU Chain preprocessing does not support memory.")

    fit_status = chain.fit_status()
    if fit_status in (
        Preprocessor.FitStatus.PARTIALLY_FITTED,
        Preprocessor.FitStatus.NOT_FITTED,
    ):
        raise PreprocessorNotFittedException(
            "`fit` must be called before `transform`, "
            "or simply use fit_transform() to run both steps"
        )

    resolved_concurrency = gpu_concurrency(
        num_gpus=num_gpus,
        concurrency=concurrency,
    )
    gpu_chain = chain._gpu_chain
    if gpu_chain is None:
        gpu_chain = GPUChain.from_cpu_preprocessors(
            chain.preprocessors,
            batch_size=batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=resolved_concurrency,
            copy_fitted_state=True,
        )
    return gpu_chain.transform(
        ds,
        batch_size=batch_size,
        concurrency=resolved_concurrency,
    )


def gpu_fit_transform(
    chain: "Chain",
    ds: "Dataset",
    *,
    transform_num_cpus: Optional[float],
    transform_memory: Optional[float],
    transform_batch_size: Optional[int],
    transform_concurrency: Optional[int],
    num_gpus: Optional[int],
    num_gpus_per_worker: float,
) -> "Dataset":
    """Fit and transform a :class:`~ray.data.preprocessors.Chain` in one GPU pass."""
    if transform_num_cpus is not None:
        raise ValueError("GPU Chain preprocessing does not support num_cpus.")
    if transform_memory is not None:
        raise ValueError("GPU Chain preprocessing does not support memory.")

    gpu_fit(
        chain,
        ds,
        batch_size=transform_batch_size,
        num_gpus=num_gpus,
        num_gpus_per_worker=num_gpus_per_worker,
        concurrency=transform_concurrency,
    )
    return gpu_transform(
        chain,
        ds,
        batch_size=transform_batch_size,
        num_cpus=None,
        memory=None,
        concurrency=transform_concurrency,
        num_gpus=num_gpus,
        num_gpus_per_worker=num_gpus_per_worker,
    )


class _GPUChainTransformUDF:
    """Actor class for applying a fused GPU preprocessor chain to a cuDF batch."""

    def __init__(self, preprocessors: Sequence[GPUPreprocessor]):
        self._preprocessors = tuple(preprocessors)
        self._ops = _plan_gpu_transform_ops(self._preprocessors)
        for op in self._ops:
            op._prepare_gpu_state()

    def __call__(self, batch: cudf.DataFrame) -> cudf.DataFrame:
        return _apply_gpu_physical_ops(batch, self._ops, prepare=False)


@PublicAPI(stability="alpha")
@SerializablePreprocessor(version=1, identifier="io.ray.preprocessors.gpu_chain")
class GPUChain(SerializablePreprocessorBase):
    """Fuse GPU preprocessors into one cuDF ``map_batches`` stage."""

    def __init__(
        self,
        *preprocessors: GPUPreprocessor,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__()
        for preprocessor in preprocessors:
            if not isinstance(preprocessor, GPUPreprocessor):
                raise TypeError(
                    "GPUChain only supports GPUPreprocessor instances; got "
                    f"{type(preprocessor)!r}."
                )
        self._preprocessors = tuple(preprocessors)
        self._batch_size = batch_size
        self._num_gpus_per_worker = num_gpus_per_worker
        self._concurrency = concurrency

    @property
    def preprocessors(self) -> Tuple[GPUPreprocessor, ...]:
        """Return the preprocessors in execution order."""
        return self._preprocessors

    @classmethod
    def from_cpu_preprocessors(
        cls,
        preprocessors: Sequence[SerializablePreprocessorBase],
        *,
        batch_size: Optional[int],
        num_gpus_per_worker: float,
        concurrency: Optional[int],
        copy_fitted_state: bool,
    ) -> "GPUChain":
        """Build a fused GPU chain from equivalent CPU preprocessors.

        Args:
            preprocessors: CPU preprocessors to convert, in execution order.
            batch_size: Rows per cuDF batch, or ``None`` for the GPU default.
            num_gpus_per_worker: GPUs reserved for each worker.
            concurrency: Maximum number of concurrent GPU workers.
            copy_fitted_state: Whether to copy fitted statistics into the GPU
                preprocessors.

        Returns:
            A fused GPU chain with equivalent supported preprocessors.

        Raises:
            TypeError: If a preprocessor has no GPU equivalent.
        """
        from ray.data.preprocessors.encoder import OrdinalEncoder
        from ray.data.preprocessors.imputer import SimpleImputer
        from ray.data.preprocessors.scaler import StandardScaler
        from ray.data.preprocessors.transformer import PowerTransformer

        gpu_batch_size = batch_size or _DEFAULT_GPU_BATCH_SIZE
        gpu_preprocessors: List[GPUPreprocessor] = []
        for preprocessor in preprocessors:
            if isinstance(preprocessor, PowerTransformer):
                gpu_preprocessor = GPUPowerTransformer(
                    columns=preprocessor.columns,
                    power=preprocessor.power,
                    method=preprocessor.method,
                    output_columns=preprocessor.output_columns,
                    batch_size=gpu_batch_size,
                    num_gpus_per_worker=num_gpus_per_worker,
                    concurrency=concurrency,
                )
            elif isinstance(preprocessor, StandardScaler):
                gpu_preprocessor = GPUStandardScaler(
                    columns=preprocessor.columns,
                    output_columns=preprocessor.output_columns,
                    batch_size=gpu_batch_size,
                    num_gpus_per_worker=num_gpus_per_worker,
                    concurrency=concurrency,
                )
            elif isinstance(preprocessor, SimpleImputer):
                gpu_preprocessor = GPUSimpleImputer(
                    columns=preprocessor.columns,
                    strategy=preprocessor.strategy,
                    fill_value=preprocessor.fill_value,
                    output_columns=preprocessor.output_columns,
                    batch_size=gpu_batch_size,
                    num_gpus_per_worker=num_gpus_per_worker,
                    concurrency=concurrency,
                )
            elif isinstance(preprocessor, OrdinalEncoder):
                gpu_preprocessor = GPUOrdinalEncoder(
                    columns=preprocessor.columns,
                    encode_lists=preprocessor.encode_lists,
                    output_columns=preprocessor.output_columns,
                    min_evidence=getattr(preprocessor, "min_evidence", 1),
                    batch_size=gpu_batch_size,
                    num_gpus_per_worker=num_gpus_per_worker,
                    concurrency=concurrency,
                )
            else:
                raise TypeError(
                    "Chain accelerator='gpu' supports PowerTransformer, "
                    "StandardScaler, SimpleImputer, and OrdinalEncoder. "
                    f"Got {type(preprocessor).__name__}."
                )

            if (
                copy_fitted_state
                and preprocessor.fit_status() == Preprocessor.FitStatus.FITTED
                and gpu_preprocessor.fit_status() != Preprocessor.FitStatus.NOT_FITTABLE
            ):
                gpu_preprocessor.stats_ = dict(preprocessor.stats_)
                gpu_preprocessor._fitted = True
            gpu_preprocessors.append(gpu_preprocessor)

        return cls(
            *gpu_preprocessors,
            batch_size=gpu_batch_size,
            num_gpus_per_worker=num_gpus_per_worker,
            concurrency=concurrency,
        )

    def copy_fitted_state_to(
        self, cpu_preprocessors: Sequence[SerializablePreprocessorBase]
    ) -> None:
        """Copy fitted statistics from this GPU chain to CPU preprocessors."""
        for preprocessor, gpu_preprocessor in zip(
            cpu_preprocessors, self._preprocessors
        ):
            if (
                gpu_preprocessor.fit_status() == Preprocessor.FitStatus.FITTED
                and preprocessor.fit_status() != Preprocessor.FitStatus.NOT_FITTABLE
            ):
                preprocessor.stats_ = dict(gpu_preprocessor.stats_)
                preprocessor._fitted = True

    def fit_status(self) -> Preprocessor.FitStatus:
        """Return the aggregate fit status of the GPU preprocessors."""
        fittable_count = 0
        fitted_count = 0

        for preprocessor in self._preprocessors:
            status = preprocessor.fit_status()
            if status == Preprocessor.FitStatus.FITTED:
                fittable_count += 1
                fitted_count += 1
            elif status in (
                Preprocessor.FitStatus.NOT_FITTED,
                Preprocessor.FitStatus.PARTIALLY_FITTED,
            ):
                fittable_count += 1
            else:
                assert status == Preprocessor.FitStatus.NOT_FITTABLE

        if fittable_count == 0:
            return Preprocessor.FitStatus.NOT_FITTABLE
        if fitted_count == fittable_count:
            return Preprocessor.FitStatus.FITTED
        if fitted_count > 0:
            return Preprocessor.FitStatus.PARTIALLY_FITTED
        return Preprocessor.FitStatus.NOT_FITTED

    @staticmethod
    def _required_prefix(
        preprocessor: "GPUPreprocessor", prefix: Sequence["GPUPreprocessor"]
    ) -> List["GPUPreprocessor"]:
        """Select earlier transforms that produce inputs needed for fitting."""
        required = set(preprocessor.get_input_columns())
        selected: List[GPUPreprocessor] = []
        for candidate in reversed(prefix):
            if set(candidate.get_output_columns()).intersection(required):
                selected.append(candidate)
                required.update(candidate.get_input_columns())
        selected.reverse()
        return selected

    def _fit_combined(self, ds: "Dataset") -> bool:
        """Fit multiple compatible preprocessors in one dataset pass."""
        prefix: List[GPUPreprocessor] = []
        fit_entries: List[Tuple[int, GPUPreprocessor, Tuple[GPUPreprocessor, ...]]] = []

        for index, preprocessor in enumerate(self._preprocessors):
            if preprocessor.fit_status() != Preprocessor.FitStatus.NOT_FITTABLE:
                if not preprocessor._supports_gpu_combined_fit():
                    return False
                required_prefix = tuple(self._required_prefix(preprocessor, prefix))
                fit_entries.append((index, preprocessor, required_prefix))
            prefix.append(preprocessor)

        if len(fit_entries) < 2:
            return False

        fittable_preprocessors = {preprocessor for _, preprocessor, _ in fit_entries}
        if any(
            preprocessor in fittable_preprocessors
            for _, _, required_prefix in fit_entries
            for preprocessor in required_prefix
        ):
            return False

        kwargs = _gpu_actor_compute_strategy(self._concurrency)

        partials = ds.map_batches(
            _GPUFitStatsUDF,
            fn_constructor_args=(tuple(fit_entries),),
            batch_format="cudf",
            batch_size=self._batch_size,
            num_gpus=self._num_gpus_per_worker,
            zero_copy_batch=True,
            udf_modifying_row_count=True,
            **kwargs,
        )

        partial_batches: Dict[int, List[Any]] = {
            index: [] for index, _, _ in fit_entries
        }
        preprocessors_by_index = {
            index: preprocessor for index, preprocessor, _ in fit_entries
        }

        for batch in partials.iter_batches(batch_size=None, batch_format="pandas"):
            if batch.empty or _COMBINED_FIT_STATS_COLUMN not in batch:
                continue
            for index, payload in batch[
                [_COMBINED_FIT_INDEX_COLUMN, _COMBINED_FIT_STATS_COLUMN]
            ].itertuples(index=False, name=None):
                index = int(index)
                if index not in partial_batches:
                    continue
                partial_batches[index].append(
                    preprocessors_by_index[index]._deserialize_gpu_fit_stats(payload)
                )

        finalize_start = time.perf_counter()
        num_partial_rows = 0
        for index, preprocessor, _ in fit_entries:
            num_partial_rows += preprocessor._finalize_gpu_fit_stat_batches(
                partial_batches[index]
            )
            preprocessor._fitted = True
        logger.info(
            "GPU combined-fit driver finalization finished in %.2f seconds "
            "for %d partial rows across %d preprocessors.",
            time.perf_counter() - finalize_start,
            num_partial_rows,
            len(fit_entries),
        )
        return True

    def _fit_distributed_combined(self, ds: "Dataset") -> bool:
        """Fit scalar moments and ordinal counts in one distributed GPU pass."""
        from ray.data.context import ShuffleStrategy
        from ray.data.grouped_data import GroupedData

        prefix: List[GPUPreprocessor] = []
        ordinal_entries = []
        moment_entries = []
        fittable: List[Tuple[int, GPUPreprocessor]] = []

        for index, preprocessor in enumerate(self._preprocessors):
            if preprocessor.fit_status() != Preprocessor.FitStatus.NOT_FITTABLE:
                required_prefix = tuple(self._required_prefix(preprocessor, prefix))
                if any(
                    item.fit_status() != Preprocessor.FitStatus.NOT_FITTABLE
                    for item in required_prefix
                ):
                    return False
                if isinstance(preprocessor, GPUOrdinalEncoder):
                    ordinal_entries.append(
                        (
                            index,
                            tuple(preprocessor.columns),
                            required_prefix,
                            preprocessor.min_evidence,
                        )
                    )
                elif isinstance(preprocessor, GPUStandardScaler):
                    moment_entries.append(
                        (index, tuple(preprocessor.columns), required_prefix)
                    )
                else:
                    return False
                fittable.append((index, preprocessor))
            prefix.append(preprocessor)

        # The serialized combined-fit path is cheaper when there is no
        # distributed cardinality aggregation to share this scan with.
        if not ordinal_entries or not moment_entries:
            return False

        if ds.context.shuffle_strategy != ShuffleStrategy.GPU_SHUFFLE:
            raise ValueError(
                "GPUChain combined distributed fitting requires "
                "DataContext.shuffle_strategy=ShuffleStrategy.GPU_SHUFFLE so "
                "global category counts remain distributed on GPUs."
            )

        aggregate = GPUPreprocessorFitAggregate(
            ordinal_entries=ordinal_entries,
            moment_entries=moment_entries,
            input_batch_rows=self._batch_size,
        )
        retained = GroupedData(
            ds,
            list(aggregate.generated_key_columns()),
            num_partitions=_GPU_ORDINAL_FIT_NUM_PARTITIONS,
        ).aggregate(aggregate)

        moment_values: Dict[int, Dict[str, Dict[int, float]]] = {
            index: {} for index, _, _ in moment_entries
        }
        retained_values: Dict[int, Dict[str, List[Any]]] = {
            index: {column: [] for column in columns}
            for index, columns, _, _ in ordinal_entries
        }
        output_column = aggregate.name
        for batch in retained.iter_batches(batch_size=None, batch_format="pandas"):
            if batch.empty:
                continue
            columns = list(aggregate.generated_key_columns()) + [output_column]
            for kind, index, column, value, metric, fit_value in batch[
                columns
            ].itertuples(index=False, name=None):
                index = int(index)
                kind = int(kind)
                if kind == aggregate.MOMENT_KIND and index in moment_values:
                    moment_values[index].setdefault(column, {})[int(metric)] = float(
                        fit_value
                    )
                elif (
                    kind == aggregate.CATEGORY_KIND
                    and index in retained_values
                    and column in retained_values[index]
                    and not _is_missing_value(value)
                ):
                    retained_values[index][column].append(value)

        preprocessors_by_index = dict(fittable)
        for index, columns, _ in moment_entries:
            rows = []
            for column in columns:
                metrics = moment_values[index].get(column, {})
                rows.append(
                    {
                        "column": column,
                        "count": int(metrics.get(aggregate.COUNT_METRIC, 0)),
                        "sum": metrics.get(aggregate.SUM_METRIC, 0.0),
                        "sum_sq": metrics.get(aggregate.SUM_SQ_METRIC, 0.0),
                    }
                )
            scaler = preprocessors_by_index[index]
            scaler._finalize_gpu_fit_stats(pd.DataFrame(rows))

        for index, columns, _, _ in ordinal_entries:
            encoder = preprocessors_by_index[index]
            encoder.stats_ = {}
            for column in columns:
                encoder.stats_[f"unique_values({column})"] = {
                    value: position + encoder.encoded_value_offset
                    for position, value in enumerate(
                        sorted(retained_values[index][column])
                    )
                }
            encoder._gpu_maps = {}

        for _, preprocessor in fittable:
            preprocessor._fitted = True
        logger.info(
            "GPU distributed combined fit finalized %d moment preprocessors and "
            "%d ordinal preprocessors from one dataset pass.",
            len(moment_entries),
            len(ordinal_entries),
        )
        return True

    def _fit(self, ds: "Dataset") -> "GPUChain":
        """Fit each fittable preprocessor, combining statistics when possible."""
        if self._fit_distributed_combined(ds):
            return self
        if self._fit_combined(ds):
            return self

        prefix: List[GPUPreprocessor] = []
        for preprocessor in self._preprocessors:
            if preprocessor.fit_status() != Preprocessor.FitStatus.NOT_FITTABLE:
                original_concurrency = preprocessor._concurrency
                if original_concurrency is None and self._concurrency is not None:
                    preprocessor._concurrency = self._concurrency
                try:
                    preprocessor._fit_gpu(
                        ds, self._required_prefix(preprocessor, prefix)
                    )
                finally:
                    preprocessor._concurrency = original_concurrency
                preprocessor._fitted = True
            prefix.append(preprocessor)
        return self

    def _transform(
        self,
        ds: "Dataset",
        batch_size: Optional[int],
        num_cpus: Optional[float] = None,
        memory: Optional[float] = None,
        concurrency: Optional[int] = None,
    ) -> "Dataset":
        """Transform a dataset in one fused GPU actor stage."""
        if num_cpus is not None:
            raise ValueError("GPUChain does not support transform num_cpus.")
        if memory is not None:
            raise ValueError("GPUChain does not support transform memory.")

        kwargs: Dict[str, Any] = {
            "batch_format": "cudf",
            "batch_size": batch_size or self._batch_size,
            "num_gpus": self._num_gpus_per_worker,
            "zero_copy_batch": True,
            "udf_modifying_row_count": False,
        }
        effective_concurrency = concurrency or self._concurrency
        kwargs.update(_gpu_actor_compute_strategy(effective_concurrency))

        return ds.map_batches(
            _GPUChainTransformUDF,
            fn_constructor_args=(self._preprocessors,),
            **kwargs,
        )

    def transform(
        self,
        ds: "Dataset",
        *,
        batch_size: Optional[int] = None,
        num_cpus: Optional[float] = None,
        memory: Optional[float] = None,
        concurrency: Optional[int] = None,
    ) -> "Dataset":
        """Transform a dataset using one fused GPU actor stage."""
        fit_status = self.fit_status()
        if fit_status in (
            Preprocessor.FitStatus.PARTIALLY_FITTED,
            Preprocessor.FitStatus.NOT_FITTED,
        ):
            from ray.data.preprocessor import PreprocessorNotFittedException

            raise PreprocessorNotFittedException(
                "`fit` must be called before `transform`, "
                "or simply use fit_transform() to run both steps"
            )
        return self._transform(
            ds,
            batch_size=batch_size,
            num_cpus=num_cpus,
            memory=memory,
            concurrency=concurrency,
        )

    def transform_cudf(self, df: cudf.DataFrame) -> cudf.DataFrame:
        """Transform one cuDF DataFrame eagerly."""
        return _apply_gpu_physical_ops(df, _plan_gpu_transform_ops(self._preprocessors))

    def _transform_batch(self, df: cudf.DataFrame) -> cudf.DataFrame:
        return self.transform_cudf(df)

    def __repr__(self) -> str:
        arguments = ", ".join(repr(p) for p in self._preprocessors)
        return f"{self.__class__.__name__}({arguments})"

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            "preprocessors": self._preprocessors,
            "batch_size": self._batch_size,
            "num_gpus_per_worker": self._num_gpus_per_worker,
            "concurrency": self._concurrency,
            "_fitted": getattr(self, "_fitted", None),
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        self._preprocessors = tuple(fields["preprocessors"])
        self._batch_size = fields.get("batch_size", _DEFAULT_GPU_BATCH_SIZE)
        self._num_gpus_per_worker = fields.get("num_gpus_per_worker", 1)
        self._concurrency = fields.get("concurrency")
        self._fitted = fields.get("_fitted")
