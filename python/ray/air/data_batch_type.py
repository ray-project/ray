from typing import Dict, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import numpy
    import pandas
    import pyarrow

DataBatchType = Union[
    "numpy.ndarray", "pandas.DataFrame", "pyarrow.Table", Dict[str, "numpy.ndarray"]
]
