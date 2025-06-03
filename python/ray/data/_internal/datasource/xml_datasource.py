import logging
from io import BytesIO
from typing import TYPE_CHECKING, List, Optional, Union, Iterable

from ray.data.block import DataBatch
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)

_XML_ROWS_PER_CHUNK = 10000


def _element_to_dict(element, parent_key="", sep="."):
    """Recursively flattens XML element into a dict."""
    d = {}

    # Include attributes if present
    for k, v in element.attrib.items():
        d[f"{parent_key}@{k}" if parent_key else f"@{k}"] = v

    children = list(element)
    if children:
        # For each child, recurse
        for child in children:
            child_key = f"{parent_key}{sep}{child.tag}" if parent_key else child.tag
            child_dict = _element_to_dict(child, child_key, sep=sep)
            d.update(child_dict)
    else:
        # Leaf node: keep the element text
        if element.text and element.text.strip():
            d[parent_key] = element.text.strip()
        elif element.attrib:
            pass  # Already added above
        else:
            d[parent_key] = None  # No info

    return d


class XMLDatasource(FileBasedDatasource):
    """XML datasource, handles nested XML elements."""

    _FILE_EXTENSIONS = [
        "xml",
        "xml.gz",
        "xml.br",
        "xml.zst",
        "xml.lz4",
    ]

    def __init__(
        self,
        paths: Union[str, List[str]],
        record_tag: Optional[str] = None,
        sep: str = ".",
        **file_based_datasource_kwargs,
    ):
        """
        Args:
            paths: The file or directory paths.
            record_tag: Tag corresponding to repeated record, e.g. 'record' or 'row'.
            sep: Separator for flattened nested keys; default is '.' (for 'user.name').
        """
        super().__init__(paths, **file_based_datasource_kwargs)
        self.record_tag = record_tag
        self.sep = sep

    def _parse_xml_buffer(self, buffer: "pyarrow.lib.Buffer") -> Iterable[DataBatch]:
        import xml.etree.ElementTree as ET
        import pyarrow as pa

        if buffer.size == 0:
            return

        tree = ET.parse(BytesIO(buffer))
        root = tree.getroot()

        # Determine the tag used for each record
        tag = self.record_tag or (root[0].tag if len(root) > 0 else None)
        if tag is None:
            raise ValueError("Cannot determine XML record tag.")

        batch = []
        for elem in root.findall(tag):
            row = _element_to_dict(elem, sep=self.sep)
            batch.append(row)
            if len(batch) >= _XML_ROWS_PER_CHUNK:
                yield pa.Table.from_pylist(batch)
                batch = []

        if batch:
            yield pa.Table.from_pylist(batch)

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        import pyarrow as pa

        buffer: pa.lib.Buffer = f.read_buffer()
        yield from self._parse_xml_buffer(buffer)
