# flake8: noqa
# fmt: off

from typing import Iterator, Union, List

import pyarrow

from ray.data.block import Block

# __datasource_constructor_start__
from ray.data.datasource import FileBasedDatasource

class ImageDatasource(FileBasedDatasource):
    def __init__(self, paths: Union[str, List[str]], *, mode: str):
        super().__init__(
            paths,
            file_extensions=["png", "jpg", "jpeg", "bmp", "gif", "tiff"],
        )

        self.mode = mode  # Specify read options in the constructor
# __datasource_constructor_end__

# __read_stream_start__
    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import io
        import numpy as np
        from PIL import Image
        from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder

        data = f.readall()
        image = Image.open(io.BytesIO(data))
        image = image.convert(self.mode)

        # Each block contains one row
        builder = DelegatingBlockBuilder()
        array = np.array(image)
        item = {"image": array}
        builder.add(item)
        yield builder.build()
# __read_stream_end__

# __read_datasource_start__
import ray

ds = ray.data.read_datasource(
    ImageDatasource("s3://anonymous@ray-example-data/batoidea", mode="RGB")
)
# __read_datasource_end__


from typing import Any, Dict
import pyarrow

# __datasink_constructor_start__
from ray.data.datasource import RowBasedFileDatasink

class ImageDatasink(RowBasedFileDatasink):
    def __init__(self, path: str, column: str, file_format: str):
        super().__init__(path, file_format=file_format)

        self.column = column
        self.file_format = file_format  # Specify write options in the constructor
# __datasink_constructor_end__

# __write_row_to_file_start__
    def write_row_to_file(self, row: Dict[str, Any], file: pyarrow.NativeFile):
        import io
        from PIL import Image

        # PIL can't write to a NativeFile, so we have to write to a buffer first.
        image = Image.fromarray(row[self.column])
        buffer = io.BytesIO()
        image.save(buffer, format=self.file_format)
        file.write(buffer.getvalue())
# __write_row_to_file_end__

# __write_datasink_start__
ds.write_datasink(ImageDatasink("/tmp/results", column="image", file_format="png"))
# __write_datasink_end__
