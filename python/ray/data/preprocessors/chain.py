from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

from ray.data.preprocessor import (
    Preprocessor,
    SerializablePreprocessorBase,
)
from ray.data.preprocessors.utils import (
    _PublicField,
    migrate_private_fields,
)
from ray.data.preprocessors.version_support import SerializablePreprocessor
from ray.data.util.data_batch_conversion import BatchFormat

if TYPE_CHECKING:
    from ray.air.data_batch_type import DataBatchType
    from ray.data.dataset import Dataset


@SerializablePreprocessor(version=1, identifier="io.ray.preprocessors.chain")
class Chain(SerializablePreprocessorBase):
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
        ...     Concatenator(columns=["X0", "X1"], output_column_name="X"),
        ...     LabelEncoder(label_column="Y")
        ... )
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           Y                                         X
        0  1  [-1.224744871391589, -1.224744871391589]
        1  0                                [0.0, 0.0]
        2  1    [1.224744871391589, 1.224744871391589]

    Args:
        *preprocessors: The preprocessors to sequentially compose.
    """

    def __init__(self, *preprocessors: SerializablePreprocessorBase):
        super().__init__()
        self._preprocessors = preprocessors
        self._gpu_chain = None

    @property
    def preprocessors(self) -> Tuple[SerializablePreprocessorBase, ...]:
        """Return the preprocessors in execution order."""
        return self._preprocessors

    def fit_status(self) -> Preprocessor.FitStatus:
        """Return the aggregate fit status of the contained preprocessors."""
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

    def _fit(self, ds: "Dataset") -> SerializablePreprocessorBase:
        for preprocessor in self._preprocessors[:-1]:
            ds = preprocessor.fit_transform(ds)
        self._preprocessors[-1].fit(ds)
        return self

    def fit(
        self,
        ds: "Dataset",
        *,
        accelerator: Optional[str] = None,
        batch_size: Optional[int] = None,
        num_gpus: Optional[int] = None,
        num_gpus_per_worker: float = 1,
        concurrency: Optional[int] = None,
    ) -> "Chain":
        """Fit the chain using either its standard or GPU implementation.

        Args:
            ds: Dataset used to fit each fittable preprocessor.
            accelerator: Set to ``"gpu"`` to use equivalent GPU preprocessors.
            batch_size: Rows per cuDF batch when using the GPU implementation.
            num_gpus: Maximum number of concurrent GPU workers.
            num_gpus_per_worker: GPUs reserved for each worker.
            concurrency: Maximum number of concurrent workers.

        Returns:
            This fitted chain.
        """
        from ray.data.preprocessors.gpu.chain import gpu_fit, use_gpu_accelerator

        if use_gpu_accelerator(accelerator):
            return gpu_fit(
                self,
                ds,
                batch_size=batch_size,
                num_gpus=num_gpus,
                num_gpus_per_worker=num_gpus_per_worker,
                concurrency=concurrency,
            )

        self._gpu_chain = None
        return super().fit(ds)

    def fit_transform(
        self,
        ds: "Dataset",
        *,
        transform_num_cpus: Optional[float] = None,
        transform_memory: Optional[float] = None,
        transform_batch_size: Optional[int] = None,
        transform_concurrency: Optional[int] = None,
        accelerator: Optional[str] = None,
        num_gpus: Optional[int] = None,
        num_gpus_per_worker: float = 1,
    ) -> "Dataset":
        """Fit the chain and transform ``ds`` in one pass.

        GPU execution converts supported CPU preprocessors to a fused
        :class:`GPUChain`; the default path retains the standard sequential
        behavior.
        """
        from ray.data.preprocessors.gpu.chain import (
            gpu_fit_transform,
            use_gpu_accelerator,
        )

        if use_gpu_accelerator(accelerator):
            return gpu_fit_transform(
                self,
                ds,
                transform_num_cpus=transform_num_cpus,
                transform_memory=transform_memory,
                transform_batch_size=transform_batch_size,
                transform_concurrency=transform_concurrency,
                num_gpus=num_gpus,
                num_gpus_per_worker=num_gpus_per_worker,
            )

        for preprocessor in self._preprocessors:
            ds = preprocessor.fit_transform(
                ds,
                transform_num_cpus=transform_num_cpus,
                transform_memory=transform_memory,
                transform_batch_size=transform_batch_size,
                transform_concurrency=transform_concurrency,
            )
        return ds

    def _transform(
        self,
        ds: "Dataset",
        batch_size: Optional[int],
        num_cpus: Optional[float] = None,
        memory: Optional[float] = None,
        concurrency: Optional[int] = None,
    ) -> "Dataset":
        for preprocessor in self._preprocessors:
            ds = preprocessor.transform(
                ds,
                batch_size=batch_size,
                num_cpus=num_cpus,
                memory=memory,
                concurrency=concurrency,
            )
        return ds

    def transform(
        self,
        ds: "Dataset",
        *,
        batch_size: Optional[int] = None,
        num_cpus: Optional[float] = None,
        memory: Optional[float] = None,
        concurrency: Optional[int] = None,
        accelerator: Optional[str] = None,
        num_gpus: Optional[int] = None,
        num_gpus_per_worker: float = 1,
    ) -> "Dataset":
        """Transform a dataset using the standard or fused GPU chain.

        Args:
            ds: Dataset to transform.
            batch_size: Rows per transform batch.
            num_cpus: CPUs reserved per standard transform worker.
            memory: Heap memory reserved per standard transform worker.
            concurrency: Maximum number of concurrent workers.
            accelerator: Set to ``"gpu"`` to use the fused GPU implementation.
            num_gpus: Maximum number of concurrent GPU workers.
            num_gpus_per_worker: GPUs reserved for each GPU worker.

        Returns:
            The lazily transformed dataset.
        """
        from ray.data.preprocessors.gpu.chain import gpu_transform, use_gpu_accelerator

        if use_gpu_accelerator(accelerator):
            return gpu_transform(
                self,
                ds,
                batch_size=batch_size,
                num_cpus=num_cpus,
                memory=memory,
                concurrency=concurrency,
                num_gpus=num_gpus,
                num_gpus_per_worker=num_gpus_per_worker,
            )

        return super().transform(
            ds,
            batch_size=batch_size,
            num_cpus=num_cpus,
            memory=memory,
            concurrency=concurrency,
        )

    def _transform_batch(self, df: "DataBatchType") -> "DataBatchType":
        for preprocessor in self._preprocessors:
            df = preprocessor.transform_batch(df)
        return df

    def __repr__(self):
        arguments = ", ".join(
            repr(preprocessor) for preprocessor in self._preprocessors
        )
        return f"{self.__class__.__name__}({arguments})"

    def _determine_transform_to_use(self) -> BatchFormat:
        # This is relevant for BatchPrediction.
        # For Chain preprocessor, we picked the first one as entry point.
        # TODO (jiaodong): We should revisit if our Chain preprocessor is
        # still optimal with context of lazy execution.
        return self._preprocessors[0]._determine_transform_to_use()

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            "preprocessors": self._preprocessors,
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        # required fields
        self._preprocessors = fields["preprocessors"]
        self._gpu_chain = None

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Handle backwards compatibility for old pickled objects."""
        super().__setstate__(state)
        migrate_private_fields(
            self,
            fields={
                "_preprocessors": _PublicField(public_field="preprocessors"),
            },
        )
