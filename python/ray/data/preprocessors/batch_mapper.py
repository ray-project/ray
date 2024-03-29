from typing import TYPE_CHECKING, Callable, Dict, Literal, Optional, Union

import numpy as np

from ray.air.util.data_batch_conversion import BatchFormat
from ray.data.preprocessor import Preprocessor
from ray.util.annotations import Deprecated

if TYPE_CHECKING:
    import pandas

BATCH_MAPPER_DEPRECATION_MESSAGE = (
    "The BatchMapper preprocessor is deprecated as of Ray 2.7. "
    "Instead, transform your Ray Dataset directly using `map_batches`."
)


@Deprecated(message=BATCH_MAPPER_DEPRECATION_MESSAGE)
class BatchMapper(Preprocessor):
    """Apply an arbitrary operation to a dataset.

    :class:`BatchMapper` applies a user-defined function to batches of a dataset. A
    batch is a Pandas ``DataFrame`` that represents a small amount of data. By modifying
    batches instead of individual records, this class can efficiently transform a
    dataset with vectorized operations.

    Use this preprocessor to apply stateless operations that aren't already built-in.

    .. tip::
        :class:`BatchMapper` doesn't need to be fit. You can call
        ``transform`` without calling ``fit``.

    Args:
        fn: The function to apply to data batches.
        batch_size: The desired number of rows in each data batch provided to ``fn``.
            Semantics are the same as in ```dataset.map_batches()``: specifying
            ``None`` wil use the entire underlying blocks as batches (blocks may
            contain different number of rows) and the actual size of the batch provided
            to ``fn`` may be smaller than ``batch_size`` if ``batch_size`` doesn't
            evenly divide the block(s) sent to a given map task. Defaults to 4096,
            which is the same default value as ``dataset.map_batches()``.
        batch_format: The preferred batch format to use in UDF. If not given,
            we will infer based on the input dataset data format.
    """

    _is_fittable = False

    def __init__(
        self,
        fn: Union[
            Callable[["pandas.DataFrame"], "pandas.DataFrame"],
            Callable[
                [Union[np.ndarray, Dict[str, np.ndarray]]],
                Union[np.ndarray, Dict[str, np.ndarray]],
            ],
        ],
        batch_format: Optional[BatchFormat],
        batch_size: Optional[Union[int, Literal["default"]]] = "default",
        # TODO: Introduce a "zero_copy" format
        # TODO: We should reach consistency of args between BatchMapper and map_batches.
    ):
        raise DeprecationWarning(BATCH_MAPPER_DEPRECATION_MESSAGE)
