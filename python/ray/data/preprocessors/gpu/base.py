from __future__ import annotations

import pickle
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from ray.data._internal.compute import ActorPoolStrategy
from ray.data.preprocessor import SerializablePreprocessorBase
from ray.data.preprocessors.gpu._runtime import (
    _apply_gpu_preprocessors,
    _GPUTransformContext,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import cudf

    from ray.data.dataset import Dataset

_COMBINED_FIT_INDEX_COLUMN = "__preprocessor_index"
_COMBINED_FIT_STATS_COLUMN = "__fit_stats"
_DEFAULT_GPU_BATCH_SIZE = 4096


def _gpu_actor_compute_strategy(
    concurrency: Optional[int],
) -> Dict[str, ActorPoolStrategy]:
    if concurrency is None:
        return {}
    return {
        "compute": ActorPoolStrategy(
            size=concurrency,
            max_tasks_in_flight_per_actor=2,
        )
    }


class _GPUTransformUDF:
    """Actor class for applying a single GPU preprocessor transform to a cuDF batch."""

    def __init__(self, preprocessor: "GPUPreprocessor"):
        self._preprocessor = preprocessor
        self._preprocessor._prepare_gpu_state()

    def __call__(self, batch: cudf.DataFrame) -> cudf.DataFrame:
        return _apply_gpu_preprocessors(batch, (self._preprocessor,), prepare=False)


class _GPUFitStatsUDF:
    """Actor class for computing fit statistics for a GPU preprocessor."""

    def __init__(
        self,
        fit_entries: Sequence[
            Tuple[int, "GPUPreprocessor", Sequence["GPUPreprocessor"]]
        ],
    ) -> None:
        self._fit_entries = tuple(
            (index, preprocessor, tuple(prefix))
            for index, preprocessor, prefix in fit_entries
        )
        for _, _, prefix in self._fit_entries:
            for prefix_preprocessor in prefix:
                prefix_preprocessor._prepare_gpu_state()

    def __call__(self, batch: cudf.DataFrame) -> pd.DataFrame:
        """Compute serialized partial fit statistics for one cuDF batch."""
        prefix_cache: Dict[Tuple["GPUPreprocessor", ...], cudf.DataFrame] = {}
        rows: List[Dict[str, Any]] = []
        for index, preprocessor, prefix in self._fit_entries:
            if prefix not in prefix_cache:
                prefix_cache[prefix] = _apply_gpu_preprocessors(batch, prefix)
            stats = preprocessor._gpu_fit_stats_cudf(prefix_cache[prefix])
            if stats.empty:
                continue
            rows.append(
                {
                    _COMBINED_FIT_INDEX_COLUMN: index,
                    _COMBINED_FIT_STATS_COLUMN: preprocessor._serialize_gpu_fit_stats(
                        stats
                    ),
                }
            )

        return pd.DataFrame(
            rows, columns=[_COMBINED_FIT_INDEX_COLUMN, _COMBINED_FIT_STATS_COLUMN]
        )


@DeveloperAPI
class GPUPreprocessor(SerializablePreprocessorBase):
    """Base class for preprocessors that transform cuDF batches on GPU.

    Args:
        batch_size: Number of rows per cuDF batch during fit and transform.
        num_gpus_per_worker: GPUs allocated to each ``map_batches`` worker.
        concurrency: Maximum number of concurrent GPU workers. If ``None``,
            Ray Data chooses concurrency automatically.
    """

    _is_fittable = False

    def __init__(
        self,
        *,
        batch_size: int = _DEFAULT_GPU_BATCH_SIZE,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ):
        super().__init__()
        self._batch_size = batch_size
        self._num_gpus_per_worker = num_gpus_per_worker
        self._concurrency = concurrency

    def _fit(self, dataset: "Dataset") -> "GPUPreprocessor":
        """Fit this preprocessor on a dataset.

        Subclasses should override :meth:`_fit_gpu` instead of this method.
        """
        return self._fit_gpu(dataset, ())

    def _fit_gpu(
        self, dataset: "Dataset", prefix: Sequence["GPUPreprocessor"]
    ) -> "GPUPreprocessor":
        """Fit this preprocessor on GPU, optionally after a prefix chain.

        When fitting inside a :class:`GPUChain`, ``prefix`` contains the
        preprocessors that must run on each batch before this one computes fit
        statistics.

        Args:
            dataset: The dataset to fit on.
            prefix: Preprocessors that transform each batch before fitting.

        Returns:
            This fitted preprocessor.
        """
        return self

    def _fit_gpu_with_stats(
        self, dataset: "Dataset", prefix: Sequence["GPUPreprocessor"]
    ) -> "GPUPreprocessor":
        """Fit from per-batch statistics computed by a GPU actor UDF."""
        partials = dataset.map_batches(
            _GPUFitStatsUDF,
            fn_constructor_args=(((0, self, tuple(prefix)),),),
            batch_format="cudf",
            batch_size=self._batch_size,
            num_gpus=self._num_gpus_per_worker,
            zero_copy_batch=True,
            udf_modifying_row_count=True,
            **_gpu_actor_compute_strategy(self._concurrency),
        )

        partial_batches: List[Any] = []
        for batch in partials.iter_batches(batch_size=None, batch_format="pandas"):
            if batch.empty or _COMBINED_FIT_STATS_COLUMN not in batch:
                continue
            for index, payload in batch[
                [_COMBINED_FIT_INDEX_COLUMN, _COMBINED_FIT_STATS_COLUMN]
            ].itertuples(index=False, name=None):
                if int(index) == 0:
                    partial_batches.append(self._deserialize_gpu_fit_stats(payload))

        self._finalize_gpu_fit_stat_batches(partial_batches)
        return self

    def _serialize_gpu_fit_stats(self, stats: pd.DataFrame) -> bytes:
        return pickle.dumps(stats, protocol=pickle.HIGHEST_PROTOCOL)

    def _deserialize_gpu_fit_stats(self, payload: bytes) -> pd.DataFrame:
        return pickle.loads(payload)

    def _finalize_gpu_fit_stat_batches(
        self, partial_batches: Sequence[pd.DataFrame]
    ) -> int:
        num_rows = sum(len(partial) for partial in partial_batches)
        stats = (
            pd.concat(partial_batches, ignore_index=True, sort=False)
            if partial_batches
            else pd.DataFrame()
        )
        self._finalize_gpu_fit_stats(stats)
        return num_rows

    def _prepare_gpu_state(self) -> None:
        """Prepare per-worker GPU state before fit or transform.

        Called once when a ``map_batches`` worker is constructed. Subclasses
        can override this to load fit statistics or other state onto the GPU.
        """
        pass

    def _transform_cudf(
        self, df: cudf.DataFrame, context: Optional[_GPUTransformContext] = None
    ) -> cudf.DataFrame:
        """Transform a single cuDF batch.

        Args:
            df: Input cuDF DataFrame batch.
            context: Shared cache for the current fused transform batch. When
                this preprocessor runs inside a :class:`GPUChain`, the chain
                passes a shared context so intermediate representations (such
                as tokenized text) can be reused across preprocessors. It is
                ``None`` when the preprocessor is applied standalone.

        Returns:
            Transformed cuDF DataFrame batch.
        """
        raise NotImplementedError

    def _gpu_modified_columns(self) -> List[str]:
        """Return columns modified by :meth:`_transform_cudf`.

        Used by fused GPU chains to invalidate cached intermediates when a
        preprocessor changes its source columns.

        Returns:
            Column names written or updated by this preprocessor.
        """
        return self.get_output_columns()

    def _supports_gpu_combined_fit(self) -> bool:
        """Return whether this preprocessor supports combined GPU fitting.

        When ``True``, a :class:`GPUChain` may fuse fit-statistics collection
        for this preprocessor with other fittable preprocessors in a single
        GPU ``map_batches`` pass.

        Returns:
            ``True`` if combined GPU fitting is supported, else ``False``.
        """
        return False

    def _gpu_fit_stats_cudf(self, df: cudf.DataFrame) -> pd.DataFrame:
        """Compute partial fit statistics for one cuDF batch.

        Args:
            df: Input cuDF DataFrame batch, after any required prefix
                preprocessors have been applied.

        Returns:
            Pandas DataFrame of partial statistics for this batch.
        """
        raise NotImplementedError

    def _finalize_gpu_fit_stats(self, partials: pd.DataFrame) -> None:
        """Aggregate partial fit statistics into ``stats_``.

        Args:
            partials: Concatenated partial statistics from all batches.
        """
        raise NotImplementedError

    def _transform(
        self,
        ds: "Dataset",
        batch_size: Optional[int],
        num_cpus: Optional[float] = None,
        memory: Optional[float] = None,
        concurrency: Optional[int] = None,
    ) -> "Dataset":
        """Transform a dataset by applying this preprocessor on GPU.

        Args:
            ds: The dataset to transform.
            batch_size: Rows per cuDF batch. Defaults to this preprocessor's
                configured batch size.
            num_cpus: Not supported for GPU preprocessors.
            memory: Not supported for GPU preprocessors.
            concurrency: Maximum number of concurrent GPU workers. Defaults to
                this preprocessor's configured concurrency.

        Returns:
            The transformed dataset.

        Raises:
            ValueError: If ``num_cpus`` or ``memory`` is provided.
        """
        if num_cpus is not None:
            raise ValueError("GPUPreprocessor does not support transform num_cpus.")
        if memory is not None:
            raise ValueError("GPUPreprocessor does not support transform memory.")

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
            _GPUTransformUDF,
            fn_constructor_args=(self,),
            **kwargs,
        )

    def _get_serializable_fields(self) -> Dict[str, Any]:
        """Return GPU execution settings shared by all GPU preprocessors."""
        return {
            "batch_size": self._batch_size,
            "num_gpus_per_worker": self._num_gpus_per_worker,
            "concurrency": self._concurrency,
            "_fitted": getattr(self, "_fitted", None),
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        """Restore GPU execution settings from deserialized data."""
        self._batch_size = fields.get("batch_size", _DEFAULT_GPU_BATCH_SIZE)
        self._num_gpus_per_worker = fields.get("num_gpus_per_worker", 1)
        self._concurrency = fields.get("concurrency")
        self._fitted = fields.get("_fitted")
