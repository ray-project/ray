import numpy as np
import pandas as pd


def column_subject_to_casting(col: "pd.core.series.Series"):
    """Return if a column in DataFrame could be subject to TensorArray casting."""
    return (
        # Fail early
        col.dtype.type is np.object_
        and not col.empty
        and isinstance(col.iloc[0], np.ndarray)
        and isinstance(col.iloc[-1], np.ndarray)
        # Only attempt cast if all values are ndarrays in case batch or block
        # concat has fallback python objects
        # TODO: Is this the most efficient implementation ?
        # TODO: Revisit this when we fully support Ragged Tensor
        and col.map(lambda x: isinstance(x, np.ndarray)).all()
    )
