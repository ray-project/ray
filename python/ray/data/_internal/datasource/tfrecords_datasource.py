import logging
from typing import TYPE_CHECKING, Iterator, List, Optional, Union

import pyarrow

from ray.air.util.tensor_extensions.arrow import pyarrow_table_from_pydict
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.data.datasource.tfrecords_datasource import (
    TFXReadOptions,
    _cast_large_list_to_list,
    _convert_example_to_dict,
    _read_records,
)

if TYPE_CHECKING:
    from tensorflow_metadata.proto.v0 import schema_pb2

logger = logging.getLogger(__name__)


class TFRecordDatasource(FileBasedDatasource):
    """TFRecord datasource, for reading and writing TFRecord files."""

    _FILE_EXTENSIONS = ["tfrecords"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        tf_schema: Optional["schema_pb2.Schema"] = None,
        tfx_read_options: Optional[TFXReadOptions] = None,
        **file_based_datasource_kwargs,
    ):
        """
        Args:
            tf_schema: Optional TensorFlow Schema which is used to explicitly set
                the schema of the underlying Dataset.
            tfx_read_options: Optional options for enabling reading tfrecords
                using tfx-bsl.

        """
        super().__init__(paths, **file_based_datasource_kwargs)

        self._tf_schema = tf_schema
        self._tfx_read_options = tfx_read_options

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        if self._tfx_read_options:
            yield from self._tfx_read_stream(f, path)
        else:
            yield from self._default_read_stream(f, path)

    def _default_read_stream(
        self, f: "pyarrow.NativeFile", path: str
    ) -> Iterator[Block]:
        import tensorflow as tf
        from google.protobuf.message import DecodeError

        for record in _read_records(f, path):
            example = tf.train.Example()
            try:
                example.ParseFromString(record)
            except DecodeError as e:
                raise ValueError(
                    "`TFRecordDatasource` failed to parse `tf.train.Example` "
                    f"record in '{path}'. This error can occur if your TFRecord "
                    f"file contains a message type other than `tf.train.Example`: {e}"
                )

            yield pyarrow_table_from_pydict(
                _convert_example_to_dict(example, self._tf_schema)
            )

    def _tfx_read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import tensorflow as tf
        from tfx_bsl.cc.tfx_bsl_extension.coders import ExamplesToRecordBatchDecoder

        full_path = self._resolve_full_path(path)

        compression = (self._open_stream_args or {}).get("compression", None)

        if compression:
            compression = compression.upper()

        tf_schema_string = (
            self._tf_schema.SerializeToString() if self._tf_schema else None
        )

        decoder = ExamplesToRecordBatchDecoder(tf_schema_string)
        exception_thrown = None
        try:
            for record in tf.data.TFRecordDataset(
                full_path, compression_type=compression
            ).batch(self._tfx_read_options.batch_size):
                yield _cast_large_list_to_list(
                    pyarrow.Table.from_batches([decoder.DecodeBatch(record.numpy())])
                )
        except Exception as error:
            logger.exception(f"Failed to read TFRecord file {full_path}")
            exception_thrown = error

        # we need to do this hack were we raise an exception outside of the
        # except block because tensorflow DataLossError is unpickable, and
        # even if we raise a runtime error, ray keeps information about the
        # original error, which makes it unpickable still.
        if exception_thrown:
            raise RuntimeError(f"Failed to read TFRecord file {full_path}.")

    def _resolve_full_path(self, relative_path):
        if isinstance(self._filesystem, pyarrow.fs.S3FileSystem):
            return f"s3://{relative_path}"
        if isinstance(self._filesystem, pyarrow.fs.GcsFileSystem):
            return f"gs://{relative_path}"
        if isinstance(self._filesystem, pyarrow.fs.HadoopFileSystem):
            return f"hdfs:///{relative_path}"
        if isinstance(self._filesystem, pyarrow.fs.PyFileSystem):
            protocol = self._filesystem.handler.fs.protocol
            if isinstance(protocol, list) or isinstance(protocol, tuple):
                protocol = protocol[0]
            if protocol == "gcs":
                protocol = "gs"
            return f"{protocol}://{relative_path}"

        return relative_path
