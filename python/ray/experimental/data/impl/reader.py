from typing import Any, Union, Optional, Tuple, TYPE_CHECKING

from ray.experimental.data.datasource import _S3FileSystemWrapper

if TYPE_CHECKING:
    import pyarrow


def read_file(path: str,
              include_paths: bool = False,
              filesystem: Optional["pyarrow.fs.FileSystem"] = None
              ) -> Union[Any, Tuple[str, Any]]:
    """A helper function which takes in the path to a single file, and the params
    from `dataset.from_binary_files`.

    Returns The contents of the file. If `include_paths` is True, a tuple of
      the path and the contents of the file.
    """
    if filesystem:
        if isinstance(filesystem, _S3FileSystemWrapper):
            filesystem = filesystem.unwrap()
        contents = filesystem.open_input_stream(path).readall()
    else:
        contents = open(path, "rb").read()

    if include_paths:
        return path, contents
    else:
        return contents
