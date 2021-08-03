from io import BytesIO
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pyarrow

from ray.data.datasource.file_based_datasource import (FileBasedDatasource)


class NumpyDatasource(FileBasedDatasource):
    """Numpy datasource, for reading and writing Numpy files.

    Examples:
        >>> source = NumpyDatasource()
        >>> ray.data.read_datasource(source, paths="/path/to/dir").take()
        ... [array([0., 1., 2.]), ...]

    """

    def _read_file(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        # TODO(ekl) Ideally numpy can read directly from the file, but it
        # seems like it requires the file to be seekable.
        buf = BytesIO()
        data = f.readall()
        buf.write(data)
        buf.seek(0)
        return np.load(buf)
