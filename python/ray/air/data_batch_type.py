from typing import TYPE_CHECKING, Dict, Union

if TYPE_CHECKING:
    import numpy
    import pandas

DataBatchType = Union["numpy.ndarray", "pandas.DataFrame", Dict[str, "numpy.ndarray"]]
