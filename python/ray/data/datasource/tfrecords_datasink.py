from typing import TYPE_CHECKING, Any, Dict, Optional

import pyarrow

from ray.data._internal.util import _check_import
from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider
from ray.data.datasource.file_datasink import BlockBasedFileDatasink
from ray.data.datasource.filename_provider import FilenameProvider
from ray.data.datasource.tfrecords_datasource import (
    _convert_arrow_table_to_examples,
    _write_record,
)

if TYPE_CHECKING:
    import pyarrow
    import tensorflow as tf
    from tensorflow_metadata.proto.v0 import schema_pb2


class _TFRecordDatasink(BlockBasedFileDatasink):
    def __init__(
        self,
        path: str,
        *,
        tf_schema: Optional["schema_pb2.Schema"] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
    ):
        _check_import(self, module="crc32c", package="crc32c")

        self.tf_schema = tf_schema

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            file_format="tar",
        )

    def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
        arrow_table = block.to_arrow()

        # It seems like TFRecords are typically row-based,
        # https://www.tensorflow.org/tutorials/load_data/tfrecord#writing_a_tfrecord_file_2
        # so we must iterate through the rows of the block,
        # serialize to tf.train.Example proto, and write to file.

        examples = _convert_arrow_table_to_examples(arrow_table, self.tf_schema)

        # Write each example to the arrow file in the TFRecord format.
        for example in examples:
            _write_record(file, example)
