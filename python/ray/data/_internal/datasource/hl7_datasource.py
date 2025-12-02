from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


class HL7Datasource(FileBasedDatasource):
    """HL7 datasource, for reading HL7 (Health Level Seven) message files.

    HL7 is a standard for exchanging health information electronically.
    This datasource reads HL7 v2.x message files, which are text-based
    files containing healthcare data in a structured format.

    HL7 messages consist of segments separated by carriage returns (\\r),
    with fields separated by pipes (|). Each segment starts with a 3-character
    segment type identifier (e.g., MSH, PID, OBR).

    For more information about HL7 format, see:
    - HL7 v2.x Standard: https://www.hl7.org/fhir/v2/
    - HL7 Documentation: https://www.hl7.org/documentcenter/

    Examples:
        Read HL7 files from local directory:

        >>> import ray
        >>> ds = ray.data.read_hl7("path/to/hl7/files/")

        Read HL7 files from S3:

        >>> ds = ray.data.read_hl7("s3://bucket/hl7/messages/")

    Note:
        This implementation uses the `hl7` Python library for parsing HL7 messages.
        Install it with: ``pip install hl7``

        Library documentation: https://python-hl7.readthedocs.io/
    """

    _FILE_EXTENSIONS = ["hl7", "hl7v2", "msg"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        *,
        encoding: str = "utf-8",
        parse_messages: bool = True,
        segment_separator: str = "\r",
        field_separator: str = "|",
        **file_based_datasource_kwargs,
    ):
        """Initialize HL7 datasource.

        Args:
            paths: File path(s) to read. Can be a single path string or list of paths.
            encoding: Text encoding for HL7 files. Defaults to "utf-8".
            parse_messages: If True, parse HL7 messages into structured format.
                If False, return raw message text. Defaults to True.
            segment_separator: Character(s) used to separate HL7 segments.
                Defaults to carriage return ("\\r").
            field_separator: Character used to separate fields within segments.
                Defaults to pipe ("|").
            **file_based_datasource_kwargs: Additional arguments passed to
                FileBasedDatasource.
        """
        super().__init__(paths, **file_based_datasource_kwargs)

        self.encoding = encoding
        self.parse_messages = parse_messages
        self.segment_separator = segment_separator
        self.field_separator = field_separator

        if parse_messages:
            _check_import(self, module="hl7", package="hl7")

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Read HL7 messages from file stream.

        Args:
            f: PyArrow file handle.
            path: File path (for logging/debugging).

        Yields:
            Blocks containing parsed HL7 messages or raw text.
        """
        data = f.readall()
        if not data:
            return

        text = data.decode(self.encoding)

        builder = DelegatingBlockBuilder()

        messages = self._split_messages(text)
        for msg_text in messages:
            if not msg_text.strip():
                continue

            if self.parse_messages:
                parsed = self._parse_hl7_message(msg_text)
                builder.add(parsed)
            else:
                builder.add({"message": msg_text.strip()})

        block = builder.build()
        yield block

    def _split_messages(self, text: str) -> List[str]:
        """Split text into individual HL7 messages.

        HL7 messages are typically separated by double segment separators or MSH segments.
        """
        if not text.strip():
            return []

        # Split on double segment separators (common message delimiter)
        double_sep = self.segment_separator * 2
        if double_sep in text:
            return [msg.strip() for msg in text.split(double_sep) if msg.strip()]

        # Otherwise split on MSH segments (message header)
        msh_pattern = "MSH" + self.field_separator
        if msh_pattern not in text:
            return [text.strip()] if text.strip() else []

        parts = text.split(msh_pattern)
        messages = []
        for i, part in enumerate(parts):
            if i == 0 and part.strip():
                messages.append(part.strip())
            elif part.strip():
                messages.append((msh_pattern + part).strip())

        return [msg for msg in messages if msg]

    def _parse_hl7_message(self, message_text: str) -> Dict[str, Any]:
        """Parse a single HL7 message into structured format."""
        import hl7

        message = hl7.parse(message_text)

        result: Dict[str, Any] = {
            "message_id": None,
            "message_type": None,
            "segments": [],
        }

        # Extract MSH segment (message header) - first segment
        msh = message[0]
        if str(msh[0][0]) != "MSH":
            raise ValueError(f"First segment must be MSH, got {msh[0][0]}")

        # MSH-9: message type
        if len(msh) > 9 and msh[9] and msh[9][0] is not None:
            result["message_type"] = str(msh[9][0])

        # MSH-10: message control ID
        if len(msh) > 10 and msh[10] and msh[10][0] is not None:
            result["message_id"] = str(msh[10][0])

        # Extract all segments
        for segment in message:
            segment_type = str(segment[0][0]) if segment[0] and segment[0][0] else None
            segment_data: Dict[str, Optional[str]] = {}

            for i, field in enumerate(segment[1:], start=1):
                if field and isinstance(field, list) and field[0] is not None:
                    segment_data[f"field_{i}"] = str(field[0])

            result["segments"].append({"type": segment_type, "data": segment_data})

        return result
