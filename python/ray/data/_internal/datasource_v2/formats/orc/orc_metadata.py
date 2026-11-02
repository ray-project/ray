"""ORC layouts from the footer already parsed by an Arrow file reader."""

import logging
from dataclasses import dataclass
from typing import Optional, Tuple, cast

import pyarrow as pa
from google.protobuf.message import DecodeError, Message
from pyarrow import _orc, orc
from pyarrow.fs import FileSystem, LocalFileSystem

from ray.data._internal.datasource_v2.formats.orc.orc_batch_size_estimator import (
    _file_tail_message_type,
    _FileTailFields,
    _FooterFields,
    _optional_int,
)
from ray.data._internal.util import call_with_retry
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrcStripeMetadata:
    index: int
    num_rows: int
    row_offset: int
    size_bytes: int
    bytes_offset: int


@dataclass(frozen=True)
class OrcFileMetadata:
    path: str
    size: int
    num_rows: int
    # None means the file has no supported layout interface, not an empty file.
    stripes: Optional[Tuple[OrcStripeMetadata, ...]]


def supports_stripe_reads() -> bool:
    """Require native batch iteration and the existing serialized-footer API."""
    return callable(getattr(orc.ORCFile, "iter_batches", None)) and callable(
        getattr(getattr(_orc, "ORCReader", None), "serialized_file_tail", None)
    )


def require_stripe_reads(path: str) -> None:
    """An existing assignment must not fall back to a whole-file read."""
    if not supports_stripe_reads():
        raise RuntimeError(
            f"ORC stripe assignment for {path} requires Arrow native batch/footer APIs"
        )


def orc_read_unit_id(path: str, stripe_index: int) -> str:
    return f"{path}#stripe{stripe_index}"


def _required_int(message: Message, name: str) -> int:
    value = _optional_int(message, name)
    if value is None:
        raise ValueError(f"Missing ORC footer field {name}")
    return value


def read_orc_layout(
    reader: orc.ORCFile,
    path: str,
    size: int,
    *,
    allow_unsupported: bool = False,
) -> OrcFileMetadata:
    """Inspect the open decoder's footer without opening or reading its input."""
    get_tail = getattr(getattr(reader, "reader", None), "serialized_file_tail", None)
    if not callable(get_tail):
        raise RuntimeError(f"ORC serialized footer is unavailable on worker for {path}")
    try:
        try:
            data = get_tail()
        except NotImplementedError as error:
            if not allow_unsupported:
                raise RuntimeError(
                    f"ORC stripe layout is unavailable for {path}"
                ) from error
            logger.debug("ORC layout is unsupported for %s; using a whole file", path)
            return OrcFileMetadata(path, size, reader.nrows, None)
        if not isinstance(data, bytes):
            raise ValueError("Serialized ORC footer must be bytes")
        tail = _file_tail_message_type().FromString(data)
        if not tail.HasField("footer"):
            raise ValueError("Missing ORC footer")
        footer = cast(_FileTailFields, tail).footer
        stripes = []
        row = 0
        byte_end = 3  # ORC magic
        for index, stripe in enumerate(cast(_FooterFields, footer).stripes):
            offset = _required_int(stripe, "offset")
            length = sum(
                _required_int(stripe, name)
                for name in ("index_length", "data_length", "footer_length")
            )
            rows = _required_int(stripe, "number_of_rows")
            if offset < byte_end or length < 0 or offset + length > size:
                raise ValueError(f"Invalid stripe {index} byte range")
            stripes.append(OrcStripeMetadata(index, rows, row, length, offset))
            row += rows
            byte_end = offset + length
        if (
            row != _required_int(footer, "number_of_rows")
            or row != reader.nrows
            or len(stripes) != reader.nstripes
        ):
            raise ValueError("Stripe rows/count disagree with the ORC file")
        return OrcFileMetadata(path, size, row, tuple(stripes))
    except (DecodeError, ValueError, pa.ArrowInvalid) as error:
        raise pa.ArrowInvalid(f"Invalid ORC layout for {path}: {error}") from error


def read_orc_file(path, filesystem, inspect):
    """Open one owned Arrow reader, with the usual path context and IO retry."""

    def read():
        try:
            with (filesystem or LocalFileSystem()).open_input_file(path) as source:
                return inspect(orc.ORCFile(source), source.size())
        except (pa.ArrowInvalid, OSError) as error:
            raise type(error)(f"Failed to read ORC file {path}: {error}") from error

    return call_with_retry(
        read,
        description=f"read ORC file {path}",
        match=DataContext.get_current().retried_io_errors,
    )


def read_orc_metadata(path: str, filesystem: Optional[FileSystem]) -> OrcFileMetadata:
    """Read layout only; decoding and schema inference remain separate."""
    return read_orc_file(
        path,
        filesystem,
        lambda reader, size: read_orc_layout(
            reader, path, size, allow_unsupported=True
        ),
    )


def read_orc_schema_and_layout(path: str, filesystem: Optional[FileSystem]):
    """Reuse the schema reader's footer for a bounded bootstrap seed."""
    return read_orc_file(
        path,
        filesystem,
        lambda reader, size: (
            reader.schema,
            read_orc_layout(reader, path, size, allow_unsupported=True),
        ),
    )
