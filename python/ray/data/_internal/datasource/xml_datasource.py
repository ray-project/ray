"""XML datasource for Ray Data.

This module provides XMLDatasource for reading XML files in Ray Data.
It supports both simple flat XML structures and nested XML with attributes.

References:
- lxml: https://lxml.de/
- xml.etree.ElementTree: https://docs.python.org/3/library/xml.etree.elementtree.html
"""

import logging
import re
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)

# Maximum nesting depth to prevent stack overflow
_MAX_NESTING_DEPTH = 100

# Characters that need sanitization in column names
_INVALID_COLUMN_CHARS = re.compile(r"[^\w@\-_.]")

# Supported XML file extensions
XML_FILE_EXTENSIONS = [
    "xml",
    "xml.gz",  # gzip-compressed files
    "xml.br",  # Brotli-compressed files
    "xml.zst",  # Zstandard-compressed files
    "xml.lz4",  # lz4-compressed files
]


class XMLDatasource(FileBasedDatasource):
    """XML datasource for reading XML files.

    This datasource reads XML files and converts them to PyArrow tables.
    It supports various XML structures including:
    - Simple flat XML with repeating elements
    - XML with attributes
    - Nested XML structures (flattened to columns)
    - Compressed XML files (.gz, .br, .zst, .lz4)

    Dependencies:
        The datasource uses lxml (https://lxml.de/) for parsing when available,
        providing better performance and full XPath support. If lxml is not installed,
        it automatically falls back to xml.etree.ElementTree from Python's standard
        library, which is always available but has limited XPath capabilities.

        To install lxml: ``pip install lxml``
    """

    _FILE_EXTENSIONS = XML_FILE_EXTENSIONS

    def __init__(
        self,
        paths: Union[str, List[str]],
        *,
        xpath: Optional[str] = None,
        **file_based_datasource_kwargs,
    ):
        """Initialize XMLDatasource.

        Args:
            paths: A single file path or a list of file paths to read.
            xpath: Optional XPath expression to select specific elements.
                   If None, reads all direct children of the root element.
                   Example: ".//record" to select all <record> elements.
            **file_based_datasource_kwargs: Additional keyword arguments passed
                                           to FileBasedDatasource.
        """
        super().__init__(paths, **file_based_datasource_kwargs)
        self._xpath = xpath

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        """Read XML file and yield PyArrow tables.

        This method reads an XML file, parses it, and converts the elements
        to a tabular format suitable for Ray Data processing.

        Args:
            f: The PyArrow file object to read from.
            path: The path to the file being read (used for error messages).

        Yields:
            PyArrow Table containing the parsed XML data.

        Raises:
            ValueError: If the XML file cannot be parsed or has an invalid format.
        """
        try:
            # Read the entire file content
            content = f.read()

            # Parse XML and convert to table
            table = self._parse_xml_to_table(content, path)

            if table is not None and len(table) > 0:
                yield table

        except Exception as e:
            raise ValueError(
                f"Failed to read XML file: {path}. "
                "Please check that the XML file has correct format, or filter out "
                "non-XML file with 'partition_filter' field. See read_xml() "
                "documentation for more details."
            ) from e

    def _parse_xml_to_table(
        self, content: bytes, path: str
    ) -> Optional["pyarrow.Table"]:
        """Parse XML content and convert to PyArrow table.

        Args:
            content: The XML content as bytes.
            path: The file path (used for error messages).

        Returns:
            PyArrow Table containing the parsed XML data, or None if no data.
        """
        import pyarrow as pa

        # Handle empty or whitespace-only content
        if not content or not content.strip():
            return pa.table({})

        # Prepare content by removing BOM markers
        content = self._remove_bom(content)

        # Parse XML root element
        root = self._parse_xml_root(content, path)
        if root is None:
            return pa.table({})

        # Select elements to process based on XPath
        elements = self._select_elements(root, path)
        if not elements:
            return pa.table({})

        # Convert XML elements to table records
        records = self._convert_elements_to_records(elements)
        if not records:
            return pa.table({})

        # Convert to PyArrow table
        try:
            return pa.Table.from_pylist(records)
        except Exception as e:
            raise ValueError(
                f"Failed to convert XML data to table: {e}. "
                "This may be due to inconsistent data types or schemas "
                "across XML elements."
            ) from e

    def _remove_bom(self, content: bytes) -> bytes:
        """Remove Byte Order Mark (BOM) from content if present.

        Args:
            content: Raw bytes from the XML file.

        Returns:
            Content with BOM removed.
        """
        # UTF-8 BOM
        if content.startswith(b"\xef\xbb\xbf"):
            return content[3:]
        # UTF-16 BOM (both big-endian and little-endian)
        elif content.startswith((b"\xff\xfe", b"\xfe\xff")):
            return content[2:]
        return content

    def _parse_xml_root(self, content: bytes, path: str):
        """Parse XML content and return the root element.

        Uses lxml for better performance and security features when available,
        with automatic fallback to xml.etree.ElementTree from the standard library.

        Args:
            content: XML content as bytes.
            path: File path for error messages.

        Returns:
            The root XML element, or None if parsing fails.

        Raises:
            ValueError: If the XML cannot be parsed.
        """
        try:
            # Try to use lxml for better performance and features
            # Reference: https://lxml.de/
            import lxml.etree as ET
            use_lxml = True
            logger.debug("Using lxml parser for XML processing")
            # Disable external entity resolution to prevent XXE attacks
            # Reference: https://lxml.de/parsing.html#parser-options
            parser = ET.XMLParser(
                resolve_entities=False,
                no_network=True,
                remove_comments=True,
                remove_pis=True,
            )
        except ImportError:
            # Fall back to standard library xml.etree.ElementTree
            # Reference: https://docs.python.org/3/library/xml.etree.elementtree.html
            import xml.etree.ElementTree as ET
            use_lxml = False
            parser = None
            logger.debug(
                "lxml not available, using xml.etree.ElementTree from standard library. "
                "For better performance and full XPath support, install lxml: "
                "pip install lxml"
            )

        try:
            if use_lxml:
                try:
                    return ET.fromstring(content, parser=parser)
                except ET.XMLSyntaxError as e:
                    raise ValueError(f"Invalid XML syntax: {e}") from e
            else:
                # Try multiple encodings for stdlib parser
                for encoding in ["utf-8", "utf-16", "latin-1", "cp1252"]:
                    try:
                        decoded_content = content.decode(encoding)
                        return ET.fromstring(decoded_content)
                    except (UnicodeDecodeError, ET.ParseError):
                        continue
                raise ValueError("Could not decode XML with any common encoding")
        except Exception as e:
            raise ValueError(
                f"Failed to parse XML from {path}. "
                f"Error: {e}. "
                "Please ensure the file is valid XML."
            ) from e

    def _select_elements(self, root, path: str) -> List:
        """Select elements from XML root based on XPath or default selection.

        Args:
            root: The root XML element.
            path: File path for error messages.

        Returns:
            List of selected XML elements.

        Raises:
            ValueError: If XPath expression is invalid.
        """
        if self._xpath:
            # Use XPath to select specific elements
            try:
                # Check if we're using lxml (has xpath method) or ElementTree (doesn't)
                if hasattr(root, "xpath"):
                    # lxml: full XPath support
                    elements = root.xpath(self._xpath)
                    # Filter out non-Element nodes (text, comments, etc.)
                    elements = [e for e in elements if hasattr(e, "tag")]
                else:
                    # ElementTree: limited XPath support
                    # Convert common XPath expressions to findall
                    xpath_query = self._xpath.replace(".//", "")
                    # Remove namespace prefixes for simple compatibility
                    xpath_query = re.sub(r"\w+:", "", xpath_query)
                    elements = root.findall(f".//{xpath_query}")
            except Exception as e:
                raise ValueError(
                    f"Invalid XPath expression '{self._xpath}': {e}"
                ) from e
        else:
            # Process all direct children of root
            elements = list(root)

        return elements

    def _convert_elements_to_records(self, elements: List) -> List[Dict[str, Any]]:
        """Convert XML elements to list of dictionary records.

        Args:
            elements: List of XML elements to convert.

        Returns:
            List of dictionary records suitable for PyArrow table creation.

        Raises:
            ValueError: If nesting is too deep.
        """
        records = []
        for element in elements:
            try:
                record = self._element_to_dict(element, depth=0)
                if record:  # Only add non-empty records
                    records.append(record)
            except RecursionError:
                raise ValueError(
                    f"XML nesting too deep (max {_MAX_NESTING_DEPTH}). "
                    "Please simplify your XML structure or use XPath to select "
                    "specific elements."
                )
        return records

    def _element_to_dict(self, element, depth: int = 0) -> Dict[str, Any]:
        """Convert XML element to dictionary.

        This method converts an XML element to a dictionary suitable for
        creating a PyArrow table. It handles:
        - Element text content
        - Element attributes
        - Nested elements (flattened to columns)
        - Namespaces (stripped from tag names)
        - Deep nesting protection

        Args:
            element: The XML element to convert.
            depth: Current recursion depth (for protection against deep nesting).

        Returns:
            Dictionary representation of the element.

        Raises:
            RecursionError: If nesting depth exceeds maximum.
        """
        # Protect against excessive nesting
        if depth > _MAX_NESTING_DEPTH:
            raise RecursionError(
                f"Maximum nesting depth ({_MAX_NESTING_DEPTH}) exceeded"
            )

        result = {}

        # Clean up tag name by removing namespace and sanitizing
        tag = self._sanitize_column_name(self._strip_namespace(element.tag))

        # Add attributes with '@' prefix to distinguish from elements
        for key, value in element.attrib.items():
            attr_key = self._sanitize_column_name(f"@{self._strip_namespace(key)}")
            result[attr_key] = value if value is not None else ""

        # Process child elements
        children = list(element)

        if not children:
            # Leaf element with text content
            text = element.text
            if text is not None:
                text = text.strip()
                if text:
                    result[tag] = text
                elif not result:
                    result[tag] = None
            elif not result:
                result[tag] = None
        else:
            # Element has children - process them recursively
            for child in children:
                child_data = self._element_to_dict(child, depth=depth + 1)
                child_tag = self._sanitize_column_name(self._strip_namespace(child.tag))

                if child_tag in child_data:
                    # Leaf node: extract the value
                    value = child_data[child_tag]
                else:
                    # Nested structure: flatten by merging all keys
                    for key, val in child_data.items():
                        if key in result:
                            # Handle multiple values for same key
                            if not isinstance(result[key], list):
                                result[key] = [result[key]]
                            result[key].append(val)
                        else:
                            result[key] = val
                    continue

                # Handle multiple children with same tag
                if child_tag in result:
                    if not isinstance(result[child_tag], list):
                        result[child_tag] = [result[child_tag]]
                    result[child_tag].append(value)
                else:
                    result[child_tag] = value

                # Handle tail text (text after child element)
                if child.tail and child.tail.strip():
                    tail_key = f"{child_tag}_tail"
                    result[tail_key] = child.tail.strip()

            # Add text content if present (for mixed content elements)
            if element.text and element.text.strip():
                result["_text"] = element.text.strip()

        return result

    def _strip_namespace(self, tag: str) -> str:
        """Strip namespace from XML tag.

        Handles both {namespace}tag and prefix:tag formats.

        Args:
            tag: XML tag possibly with namespace.

        Returns:
            Tag without namespace.
        """
        # Remove namespace in {namespace}tag format
        if tag.startswith("{"):
            return tag.split("}", 1)[1] if "}" in tag else tag
        # Remove namespace prefix in prefix:tag format
        if ":" in tag:
            return tag.split(":", 1)[1]
        return tag

    def _sanitize_column_name(self, name: str) -> str:
        """Sanitize column name for PyArrow compatibility.

        Ensures column names are valid for PyArrow tables by:
        - Replacing invalid characters with underscores
        - Ensuring names don't start with numbers
        - Handling empty names
        - Truncating very long names

        Args:
            name: Original column name from XML.

        Returns:
            Sanitized column name safe for PyArrow.
        """
        # Replace invalid characters with underscores
        sanitized = _INVALID_COLUMN_CHARS.sub("_", name)

        # Ensure name doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = "_" + sanitized

        # Handle empty names
        if not sanitized:
            sanitized = "_unnamed"

        # Truncate very long names (PyArrow has practical limits)
        max_length = 256
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length]

        return sanitized
