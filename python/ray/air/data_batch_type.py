from typing import Dict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import numpy
    import pandas

DataBatchType = Union["numpy.ndarray", "pandas.DataFrame", Dict[str, "numpy.ndarray"]]
