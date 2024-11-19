from typing import TYPE_CHECKING, Dict, Union

if TYPE_CHECKING:
    import numpy
    import pandas
    import pyarrow

# TODO de-dup with ray.data.block.DataBatch
DataBatchType = Union["numpy.ndarray", "pyarrow.Table" "pandas.DataFrame", Dict[str, "numpy.ndarray"]]
