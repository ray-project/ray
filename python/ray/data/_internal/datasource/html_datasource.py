"""HTML datasource for Ray Data.

This module provides HTMLDatasource for reading HTML files into Ray Datasets.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.data.datasource.file_meta_provider import DefaultFileMetadataProvider
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow
    from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# The default size multiplier for reading HTML data source.
# HTML files typically expand in memory due to parsing overhead.
HTML_ENCODING_RATIO_ESTIMATE_DEFAULT = 3.0

# The lower bound value to estimate HTML encoding ratio.
HTML_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 1.5


@DeveloperAPI
class HTMLDatasource(FileBasedDatasource):
    """A datasource for reading HTML files.

    This datasource reads HTML files and extracts:
    - Text content (with various cleaning options)
    - Tables as structured data
    - Links and metadata
    - Custom element selection via CSS selectors

    Features:
    - Multiple text extraction modes (clean, raw, markdown)
    - Table extraction with pandas DataFrame conversion
    - Link and image URL extraction
    - Metadata extraction (title, meta tags, headers)
    - Encoding detection and handling
    - CSS selector support for targeted extraction

    Examples:
        Read HTML files and extract clean text:

        >>> import ray
        >>> ds = ray.data.read_html("s3://bucket/pages/")  # doctest: +SKIP
        >>> ds.take(1)  # doctest: +SKIP
        [{'text': 'Clean page content...', 'title': 'Page Title', 'url': '...'}]

        Extract tables from HTML:

        >>> ds = ray.data.read_html(  # doctest: +SKIP
        ...     "s3://bucket/tables.html",
        ...     extract_tables=True
        ... )

        Extract with CSS selector:

        >>> ds = ray.data.read_html(  # doctest: +SKIP
        ...     "s3://bucket/docs/",
        ...     selector="article.main-content"
        ... )
    """

    _FILE_EXTENSIONS = ["html", "htm"]
    _COLUMN_NAME = "text"
    # Use 4 threads per task for HTML parsing (CPU-intensive)
    _NUM_THREADS_PER_TASK = 4

    def __init__(
        self,
        paths: Union[str, List[str]],
        *,
        text_mode: str = "clean",
        extract_tables: bool = False,
        extract_links: bool = False,
        extract_metadata: bool = True,
        selector: Optional[str] = None,
        encoding: Optional[str] = None,
        **file_based_datasource_kwargs,
    ):
        """Initialize HTML datasource.

        Args:
            paths: Path(s) to HTML file(s).
            text_mode: Text extraction mode. Options:
                - "clean": Remove extra whitespace, scripts, styles (default)
                - "raw": Preserve all text including whitespace
                - "markdown": Convert to markdown format
            extract_tables: Whether to extract HTML tables as structured data.
            extract_links: Whether to extract all links from the HTML.
            extract_metadata: Whether to extract metadata (title, meta tags).
            selector: CSS selector to extract specific elements only.
            encoding: Character encoding. If None, auto-detect.
            **file_based_datasource_kwargs: Additional arguments for FileBasedDatasource.

        Raises:
            ValueError: If text_mode is not valid.
        """
        super().__init__(paths, **file_based_datasource_kwargs)

        # Check dependencies
        _check_import(self, module="bs4", package="beautifulsoup4")
        if extract_tables:
            _check_import(self, module="pandas", package="pandas")
        if text_mode == "markdown":
            _check_import(self, module="markdownify", package="markdownify")

        # Validate text_mode
        valid_modes = ["clean", "raw", "markdown"]
        if text_mode not in valid_modes:
            raise ValueError(
                f"text_mode must be one of {valid_modes}, got '{text_mode}'"
            )

        self.text_mode = text_mode
        self.extract_tables = extract_tables
        self.extract_links = extract_links
        self.extract_metadata = extract_metadata
        self.selector = selector
        self.encoding = encoding

        # Setup encoding ratio for metadata provider
        meta_provider = file_based_datasource_kwargs.get("meta_provider", None)
        if isinstance(meta_provider, HTMLFileMetadataProvider):
            self._encoding_ratio = HTML_ENCODING_RATIO_ESTIMATE_DEFAULT
            meta_provider._set_encoding_ratio(self._encoding_ratio)
        else:
            self._encoding_ratio = HTML_ENCODING_RATIO_ESTIMATE_DEFAULT

    def _read_stream(
        self,
        f: "pyarrow.NativeFile",
        path: str,
    ) -> Iterator[Block]:
        """Read and parse HTML file.

        Args:
            f: File handle.
            path: File path.

        Yields:
            Blocks containing parsed HTML data.
        """
        from bs4 import BeautifulSoup

        # Read file content
        data = f.readall()

        # Handle encoding
        if self.encoding:
            try:
                html_content = data.decode(self.encoding)
            except UnicodeDecodeError as e:
                raise ValueError(
                    f"Failed to decode HTML file '{path}' with encoding '{self.encoding}': {e}"
                ) from e
        else:
            # Auto-detect encoding
            html_content = self._decode_html(data, path)

        # Parse HTML with appropriate parser
        # Try lxml first (faster) if available, otherwise use html.parser
        parser = "html.parser"  # Default fallback
        try:
            import lxml  # noqa: F401

            parser = "lxml"
        except ImportError:
            pass  # lxml not available, use html.parser

        try:
            soup = BeautifulSoup(html_content, parser)
        except Exception as parse_error:
            raise ValueError(
                f"Failed to parse HTML file '{path}' with {parser} parser: {parse_error}"
            ) from parse_error

        # Extract document-level metadata from full document (before selector)
        # This includes title, description, keywords from <head> section
        doc_metadata = None
        if self.extract_metadata:
            doc_metadata = self._extract_document_metadata(soup)

        # Apply selector if specified (for content extraction)
        content_soup = soup
        if self.selector:
            elements = soup.select(self.selector)
            if not elements:
                logger.warning(
                    f"CSS selector '{self.selector}' matched no elements in '{path}'"
                )
                # Create empty soup for consistent behavior
                content_soup = BeautifulSoup("", parser)
            else:
                # Create new soup with selected elements while preserving structure
                import copy

                from bs4 import BeautifulSoup as BS

                # Create minimal HTML document structure
                new_soup = BS("<html><body></body></html>", parser)
                body = new_soup.body

                # Clone and append selected elements directly to preserve context
                for elem in elements:
                    # Use deepcopy to preserve nested structure and attributes
                    elem_copy = copy.deepcopy(elem)
                    body.append(elem_copy)

                content_soup = new_soup

        # Extract content-specific metadata (headers) from selected content
        content_metadata = None
        if self.extract_metadata:
            content_metadata = self._extract_content_metadata(content_soup)
            # Combine document and content metadata
            metadata = {**doc_metadata, **content_metadata}
        else:
            metadata = None

        # Build row data
        row_data = self._extract_content(content_soup, path, metadata)

        # Create block
        builder = DelegatingBlockBuilder()
        builder.add(row_data)
        yield builder.build()

    def _decode_html(self, data: bytes, path: str) -> str:
        """Decode HTML with encoding detection.

        Args:
            data: Raw bytes.
            path: File path for error reporting.

        Returns:
            Decoded HTML string.
        """
        # Try common encodings in order
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings:
            try:
                return data.decode(encoding)
            except UnicodeDecodeError:
                continue

        # If all fail, use utf-8 with error replacement
        logger.warning(
            f"Could not detect encoding for '{path}', using utf-8 with error replacement"
        )
        return data.decode("utf-8", errors="replace")

    def _extract_content(
        self,
        soup: "BeautifulSoup",
        path: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Extract content from parsed HTML.

        Args:
            soup: BeautifulSoup object for content extraction.
            path: File path (for logging/debugging, not added to output).
            metadata: Pre-extracted metadata from full document (optional).

        Returns:
            Dictionary with extracted data.

        Note:
            The 'path' column is automatically added by FileBasedDatasource
            if include_paths=True. Do not manually add it here.
        """
        row_data = {}

        # Extract text
        if self.text_mode == "clean":
            row_data[self._COLUMN_NAME] = self._extract_clean_text(soup)
        elif self.text_mode == "raw":
            row_data[self._COLUMN_NAME] = soup.get_text()
        elif self.text_mode == "markdown":
            row_data[self._COLUMN_NAME] = self._extract_markdown(soup)

        # Add pre-extracted metadata (if provided)
        if metadata is not None:
            row_data.update(metadata)

        # Extract tables
        if self.extract_tables:
            tables = self._extract_tables(soup)
            row_data["tables"] = tables

        # Extract links
        if self.extract_links:
            links = self._extract_links(soup)
            row_data["links"] = links

        return row_data

    def _extract_clean_text(self, soup: "BeautifulSoup") -> str:
        """Extract clean text from HTML.

        Removes scripts, styles, and extra whitespace.

        Args:
            soup: BeautifulSoup object.

        Returns:
            Clean text string.
        """
        # Remove script and style elements
        for element in soup(["script", "style", "noscript", "iframe"]):
            element.decompose()

        # Get text and clean whitespace
        text = soup.get_text(separator=" ")

        # Clean up whitespace (collapse all whitespace to single spaces)
        text = " ".join(text.split())

        return text

    def _extract_markdown(self, soup: "BeautifulSoup") -> str:
        """Convert HTML to markdown.

        Args:
            soup: BeautifulSoup object.

        Returns:
            Markdown string.
        """
        from markdownify import markdownify as md

        # Remove script and style elements
        for element in soup(["script", "style", "noscript", "iframe"]):
            element.decompose()

        html_str = str(soup)
        markdown_text = md(html_str, heading_style="ATX", bullets="-")

        return markdown_text.strip()

    def _extract_document_metadata(self, soup: "BeautifulSoup") -> Dict[str, Any]:
        """Extract document-level metadata from HTML.

        This extracts metadata from the document's <head> section, including
        title, meta description, and meta keywords. This should be called on
        the full document before applying any CSS selectors.

        Args:
            soup: BeautifulSoup object for the full document.

        Returns:
            Dictionary with document-level metadata.
        """
        metadata = {}

        # Extract title
        title_tag = soup.find("title")
        metadata["title"] = title_tag.get_text().strip() if title_tag else None

        # Extract meta description
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            metadata["description"] = meta_desc["content"]
        else:
            metadata["description"] = None

        # Extract meta keywords
        meta_keywords = soup.find("meta", attrs={"name": "keywords"})
        if meta_keywords and meta_keywords.get("content"):
            metadata["keywords"] = meta_keywords["content"]
        else:
            metadata["keywords"] = None

        return metadata

    def _extract_content_metadata(self, soup: "BeautifulSoup") -> Dict[str, Any]:
        """Extract content-specific metadata from HTML.

        This extracts metadata from the content, such as headers (h1-h6).
        This should be called on the selected content after applying CSS selectors.

        Args:
            soup: BeautifulSoup object for the content (possibly filtered by selector).

        Returns:
            Dictionary with content-specific metadata.
        """
        metadata = {}

        # Extract headers
        headers = {}
        for i in range(1, 7):  # h1 to h6
            h_tags = soup.find_all(f"h{i}")
            if h_tags:
                headers[f"h{i}"] = [h.get_text().strip() for h in h_tags]

        if headers:
            metadata["headers"] = headers

        return metadata

    def _extract_tables(self, soup: "BeautifulSoup") -> List[List[List[str]]]:
        """Extract tables from HTML.

        Args:
            soup: BeautifulSoup object.

        Returns:
            List of tables, where each table is a list of rows,
            and each row is a list of cell values.
        """
        tables = []

        for table in soup.find_all("table"):
            table_data = []

            # Extract table rows
            for row in table.find_all("tr"):
                row_data = []
                # Get both td and th cells
                for cell in row.find_all(["td", "th"]):
                    cell_text = cell.get_text().strip()
                    row_data.append(cell_text)
                if row_data:  # Only add non-empty rows
                    table_data.append(row_data)

            if table_data:
                tables.append(table_data)

        return tables

    def _extract_links(self, soup: "BeautifulSoup") -> List[Dict[str, str]]:
        """Extract links from HTML.

        Args:
            soup: BeautifulSoup object.

        Returns:
            List of dictionaries with link data (href, text).
        """
        links = []

        for a_tag in soup.find_all("a", href=True):
            link_data = {
                "href": a_tag["href"],
                "text": a_tag.get_text().strip(),
            }
            links.append(link_data)

        return links

    def _rows_per_file(self):
        """Return number of rows per file."""
        return 1

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory size of parsed HTML data.

        Returns:
            Estimated size in bytes, or None if size cannot be estimated.
        """
        total_size = 0
        has_any_size = False

        for file_size in self._file_sizes():
            if file_size is not None:
                total_size += file_size
                has_any_size = True

        # Return None if no file sizes available (Ray Data will handle accordingly)
        if not has_any_size:
            return None

        # Apply encoding ratio (HTML expands in memory after parsing)
        return int(total_size * self._encoding_ratio)


@DeveloperAPI
class HTMLFileMetadataProvider(DefaultFileMetadataProvider):
    """Metadata provider for HTML files with custom encoding ratio."""

    def __init__(self):
        """Initialize with default encoding ratio."""
        super().__init__()
        self._encoding_ratio = HTML_ENCODING_RATIO_ESTIMATE_DEFAULT

    def _set_encoding_ratio(self, encoding_ratio: float):
        """Set HTML file encoding ratio.

        Args:
            encoding_ratio: Ratio for estimating in-memory size.
        """
        self._encoding_ratio = encoding_ratio

    def _get_block_metadata(
        self,
        paths: List[str],
        *,
        rows_per_file: Optional[int],
        file_sizes: List[Optional[int]],
    ) -> BlockMetadata:
        """Get block metadata with adjusted size estimate.

        Args:
            paths: File paths.
            rows_per_file: Rows per file.
            file_sizes: File sizes in bytes.

        Returns:
            BlockMetadata with adjusted size estimate.
        """
        metadata = super()._get_block_metadata(
            paths, rows_per_file=rows_per_file, file_sizes=file_sizes
        )
        if metadata.size_bytes is not None:
            metadata.size_bytes = int(metadata.size_bytes * self._encoding_ratio)
        return metadata
