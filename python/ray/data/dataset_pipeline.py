from typing import Callable, Iterable

from ray.data.dataset import Dataset, _raise_dataset_pipeline_deprecation_warning
from ray.util.annotations import Deprecated


@Deprecated
class DatasetPipeline:
    """Implements a pipeline of Datasets.

    DatasetPipelines implement pipelined execution. This allows for the
    overlapped execution of data input (e.g., reading files), computation
    (e.g. feature preprocessing), and output (e.g., distributed ML training).

    A DatasetPipeline can be created by either repeating a Dataset
    (``ds.repeat(times=None)``), by turning a single Dataset into a pipeline
    (``ds.window(blocks_per_window=10)``), or defined explicitly using
    ``DatasetPipeline.from_iterable()``.

    DatasetPipeline supports the all the per-record transforms of Datasets
    (e.g., map, flat_map, filter), holistic transforms (e.g., repartition),
    and output methods (e.g., iter_rows, to_tf, to_torch, write_datasource).
    """

    @staticmethod
    def from_iterable(
        iterable: Iterable[Callable[[], Dataset]],
    ) -> "DatasetPipeline":
        """Create a pipeline from an sequence of Dataset producing functions.

        Args:
            iterable: A finite or infinite-length sequence of functions that
                each produce a Dataset when called.
        """
        _raise_dataset_pipeline_deprecation_warning()
