"""HTML datasource for Ray Data.

This module provides HTMLDatasource for reading HTML files into Ray Datasets.

The implementation uses BeautifulSoup4 for HTML parsing:
https://www.crummy.com/software/BeautifulSoup/bs4/doc/

Optional dependencies:
- lxml: Faster HTML parser (https://lxml.de/)
- markdownify: HTML to markdown conversion (https://github.com/matthewwithanm/markdownify)
"""

import copy
import logging
import re
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


@DeveloperAPI
class HTMLDatasource(FileBasedDatasource):
    """Datasource for reading HTML files and extracting text, tables, links, and metadata."""

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
            text_mode: Text extraction mode ("clean", "raw", or "markdown").
            extract_tables: Whether to extract HTML tables as structured data.
            extract_links: Whether to extract all links from the HTML.
            extract_metadata: Whether to extract metadata (title, meta tags).
            selector: CSS selector to extract specific elements only.
            encoding: Character encoding. If None, auto-detect.
            **file_based_datasource_kwargs: Additional arguments for FileBasedDatasource.
        """
        super().__init__(paths, **file_based_datasource_kwargs)

        # Check dependencies
        # BeautifulSoup4 is always required for HTML parsing
        # See: https://www.crummy.com/software/BeautifulSoup/bs4/doc/
        _check_import(self, module="bs4", package="beautifulsoup4")
        # markdownify is only required for markdown text mode
        # See: https://github.com/matthewwithanm/markdownify
        if text_mode == "markdown":
            _check_import(self, module="markdownify", package="markdownify")

        # Validate text_mode
        valid_modes = ["clean", "raw", "markdown"]
        if text_mode not in valid_modes:
            raise ValueError(
                f"text_mode must be one of {valid_modes}, got '{text_mode}'"
            )

        # Validate selector
        if selector is not None:
            if not isinstance(selector, str):
                raise TypeError(
                    f"selector must be a string, got {type(selector).__name__}"
                )
            selector = selector.strip()
            if not selector:
                raise ValueError("selector cannot be empty or whitespace-only")

        # Validate encoding
        if encoding is not None:
            if not isinstance(encoding, str):
                raise TypeError(
                    f"encoding must be a string, got {type(encoding).__name__}"
                )
            encoding = encoding.strip()
            if not encoding:
                raise ValueError("encoding cannot be empty or whitespace-only")

        self.text_mode = text_mode
        self.extract_tables = extract_tables
        self.extract_links = extract_links
        self.extract_metadata = extract_metadata
        self.selector = selector
        self.encoding = encoding

        # Setup encoding ratio for metadata provider
        self._encoding_ratio = HTML_ENCODING_RATIO_ESTIMATE_DEFAULT
        meta_provider = file_based_datasource_kwargs.get("meta_provider", None)
        if isinstance(meta_provider, HTMLFileMetadataProvider):
            meta_provider._set_encoding_ratio(self._encoding_ratio)

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
            Block: Blocks containing parsed HTML data.

        Raises:
            ValueError: If HTML file cannot be decoded or parsed.
            OSError: If file cannot be read.
        """

        data = f.readall()

        # Handle empty files
        if not data:
            logger.warning(f"HTML file '{path}' is empty")
            builder = DelegatingBlockBuilder()
            row_data = {self._COLUMN_NAME: ""}
            if self.extract_metadata:
                row_data.update(
                    {
                        "title": None,
                        "description": None,
                        "keywords": None,
                        "headers": {},
                    }
                )
            if self.extract_tables:
                row_data["tables"] = []
            if self.extract_links:
                row_data["links"] = []
            builder.add(row_data)
            yield builder.build()
            return

        # Handle encoding
        if self.encoding:
            try:
                html_content = data.decode(self.encoding)
            except UnicodeDecodeError as e:
                raise ValueError(
                    f"Failed to decode HTML file '{path}' with encoding '{self.encoding}': {e}"
                ) from e
        else:
            html_content = self._decode_html(data, path)

        # Parse HTML with parser fallback
        soup, parser = self._parse_html(html_content, path)

        if self.extract_metadata:
            doc_metadata = self._extract_document_metadata(soup)
        else:
            doc_metadata = None

        # Apply CSS selector if provided
        content_soup = self._apply_selector(soup, parser, path)

        if self.extract_metadata:
            content_metadata = self._extract_content_metadata(content_soup)
            # Merge metadata, with content metadata taking precedence for overlapping keys
            metadata = {**(doc_metadata or {}), **(content_metadata or {})}
        else:
            metadata = None

        builder = DelegatingBlockBuilder()
        builder.add(self._extract_content(content_soup, path, metadata))
        yield builder.build()

    def _parse_html(self, html_content: str, path: str) -> tuple["BeautifulSoup", str]:
        """Parse HTML content with parser fallback.

        Tries lxml parser first (faster), falls back to html.parser if lxml is not
        available or fails. See https://lxml.de/ for lxml documentation.

        Args:
            html_content: HTML content as string.
            path: File path for error messages.

        Returns:
            Tuple of (BeautifulSoup object, parser name used).

        Raises:
            ValueError: If HTML cannot be parsed with any parser.
        """
        from bs4 import BeautifulSoup

        # Try lxml parser first (faster, but requires lxml package)
        # lxml is optional - if not available, fall back to html.parser
        try:
            import lxml  # noqa: F401

            soup = BeautifulSoup(html_content, "lxml")
            return soup, "lxml"
        except ImportError:
            # lxml not installed, fall back to html.parser
            pass
        except Exception as e:
            # lxml failed to parse, try html.parser
            logger.debug(f"lxml parser failed for '{path}', trying html.parser: {e}")

        # Fall back to html.parser (always available)
        soup = BeautifulSoup(html_content, "html.parser")
        return soup, "html.parser"

    def _apply_selector(
        self, soup: "BeautifulSoup", parser: str, path: str
    ) -> "BeautifulSoup":
        """Apply CSS selector to extract specific elements.

        Uses BeautifulSoup's select() method for CSS selector matching.
        See: https://www.crummy.com/software/BeautifulSoup/bs4/doc/#css-selectors

        Args:
            soup: BeautifulSoup object.
            parser: Parser name used.
            path: File path for logging.

        Returns:
            BeautifulSoup object with selected content.

        Raises:
            ValueError: If CSS selector is invalid or fails to parse.
        """
        if not self.selector:
            return soup

        from bs4 import BeautifulSoup

        try:
            elements = soup.select(self.selector)
        except Exception as e:
            raise ValueError(
                f"Invalid CSS selector '{self.selector}' for '{path}': {e}"
            ) from e

        if not elements:
            logger.warning(
                f"CSS selector '{self.selector}' matched no elements in '{path}'"
            )
            return BeautifulSoup("", parser)

        # Create new soup with selected elements
        # Use shallow copy for memory efficiency with large documents
        new_soup = BeautifulSoup("<html><body></body></html>", parser)
        for elem in elements:
            new_soup.body.append(copy.copy(elem))

        return new_soup

    def _decode_html(self, data: bytes, path: str) -> str:
        """Decode HTML with encoding detection.

        First tries to detect encoding from HTML meta charset tag,
        then falls back to common encodings (utf-8, latin-1, cp1252, iso-8859-1).
        If all attempts fail, uses utf-8 with error replacement.

        Args:
            data: Raw HTML bytes.
            path: File path for logging.

        Returns:
            Decoded HTML string.
        """
        # Try to detect encoding from HTML meta charset tag
        # Check first 8KB for performance (charset is typically in first few KB)
        sample_size = min(len(data), 8192)
        charset_match = re.search(
            rb'<meta\s+[^>]*charset\s*=\s*["\']?([^"\'>\s]+)',
            data[:sample_size],
            re.IGNORECASE,
        )
        if charset_match:
            encoding = charset_match.group(1).decode("ascii", errors="ignore").lower()
            # Normalize common encoding names
            encoding_map = {
                "utf8": "utf-8",
                "utf-16le": "utf-16-le",
                "utf-16be": "utf-16-be",
                "utf16": "utf-16",
                "utf16le": "utf-16-le",
                "utf16be": "utf-16-be",
                "windows-1252": "cp1252",
                "iso8859-1": "iso-8859-1",
                "iso88591": "iso-8859-1",
            }
            encoding = encoding_map.get(encoding, encoding)
            try:
                return data.decode(encoding)
            except (UnicodeDecodeError, LookupError) as e:
                logger.debug(
                    f"Detected charset '{encoding}' in HTML meta tag for '{path}' "
                    f"but failed to decode: {e}. Trying fallback encodings"
                )

        # Fall back to common encodings
        for encoding in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
            try:
                return data.decode(encoding)
            except UnicodeDecodeError:
                continue

        # Last resort: use utf-8 with error replacement
        logger.warning(
            f"Could not detect encoding for '{path}', using utf-8 with error replacement"
        )
        return data.decode("utf-8", errors="replace")

    def _extract_content(
        self,
        soup: "BeautifulSoup",
        path: str,  # noqa: ARG002
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Extract content from parsed HTML.

        Args:
            soup: BeautifulSoup object.
            path: File path (unused, kept for API consistency).
            metadata: Optional metadata dict to include.

        Returns:
            Dictionary with extracted content.
        """
        row_data = {self._COLUMN_NAME: self._extract_text(soup)}
        if metadata:
            row_data.update(metadata)
        if self.extract_tables:
            row_data["tables"] = self._extract_tables(soup)
        if self.extract_links:
            row_data["links"] = self._extract_links(soup)
        return row_data

    def _extract_text(self, soup: "BeautifulSoup") -> str:
        """Extract text based on text_mode.

        Args:
            soup: BeautifulSoup object.

        Returns:
            Extracted text string.
        """
        if self.text_mode == "clean":
            return self._extract_clean_text(soup)
        elif self.text_mode == "raw":
            # Preserve whitespace structure in raw mode
            return soup.get_text(separator="\n")
        elif self.text_mode == "markdown":
            return self._extract_markdown(soup)
        # Should never reach here due to validation in __init__
        raise ValueError(f"Invalid text_mode: {self.text_mode}")

    def _extract_clean_text(self, soup: "BeautifulSoup") -> str:
        """Extract clean text from HTML, removing scripts, styles, and extra whitespace.

        Args:
            soup: BeautifulSoup object.

        Returns:
            Cleaned text string.
        """
        # Remove script, style, and other non-content elements
        for element in soup(
            ["script", "style", "noscript", "iframe", "svg", "canvas", "meta", "link"]
        ):
            element.decompose()

        # Extract text and normalize whitespace
        text = soup.get_text(separator=" ")
        # Replace multiple whitespace (including newlines, tabs) with single space
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _extract_markdown(self, soup: "BeautifulSoup") -> str:
        """Convert HTML to markdown.

        Uses markdownify library for conversion. See:
        https://github.com/matthewwithanm/markdownify

        Args:
            soup: BeautifulSoup object.

        Returns:
            Markdown string.

        Raises:
            ImportError: If markdownify is not installed.
            ValueError: If markdown conversion fails.
        """
        from markdownify import markdownify as md

        # Remove script, style, and other non-content elements before conversion
        for element in soup(["script", "style", "noscript", "iframe"]):
            element.decompose()

        return md(str(soup), heading_style="ATX", bullets="-").strip()

    def _extract_document_metadata(self, soup: "BeautifulSoup") -> Dict[str, Any]:
        """Extract document-level metadata from HTML <head> section.

        Args:
            soup: BeautifulSoup object.

        Returns:
            Dictionary with metadata fields.
        """
        metadata = {}

        # Extract title
        title_tag = soup.find("title")
        metadata["title"] = title_tag.get_text(strip=True) if title_tag else None

        # Extract meta description (use first if multiple exist)
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            desc_content = meta_desc.get("content")
            metadata["description"] = desc_content.strip() if desc_content else None
        else:
            # Check Open Graph property
            og_desc = soup.find("meta", attrs={"property": "og:description"})
            og_content = og_desc.get("content") if og_desc else None
            metadata["description"] = og_content.strip() if og_content else None

        # Extract meta keywords (use first if multiple exist)
        meta_keywords = soup.find("meta", attrs={"name": "keywords"})
        keywords_content = meta_keywords.get("content") if meta_keywords else None
        metadata["keywords"] = keywords_content.strip() if keywords_content else None

        return metadata

    def _extract_content_metadata(self, soup: "BeautifulSoup") -> Dict[str, Any]:
        """Extract content-specific metadata from HTML (headers h1-h6).

        Args:
            soup: BeautifulSoup object.

        Returns:
            Dictionary with headers metadata.
        """
        headers = {}
        for i in range(1, 7):
            h_tags = soup.find_all(f"h{i}")
            if h_tags:
                header_texts = []
                for h in h_tags:
                    text = h.get_text(strip=True)
                    if text:  # Only include non-empty headers
                        header_texts.append(text)
                if header_texts:
                    headers[f"h{i}"] = header_texts
        return {"headers": headers} if headers else {}

    def _extract_tables(self, soup: "BeautifulSoup") -> List[List[List[str]]]:
        """Extract tables from HTML.

        Handles nested tables by extracting them separately.
        Each table is represented as a list of rows, where each row is a list of cells.

        Args:
            soup: BeautifulSoup object.

        Returns:
            List of tables, where each table is a list of rows (list of cell strings).
        """
        tables = []
        # Find all top-level tables (not nested inside other tables)
        for table in soup.find_all("table", recursive=True):
            # Skip if this table is nested inside another table
            parent_table = table.find_parent("table")
            if parent_table is not None:
                continue

            table_data = []
            # Find all rows in table (including header rows)
            # Use recursive=False to only get direct children rows
            for row in table.find_all("tr", recursive=False):
                # Only get direct children cells (td/th) to avoid nested table cells
                cells = row.find_all(["td", "th"], recursive=False)
                row_data = [cell.get_text(separator=" ", strip=True) for cell in cells]
                if row_data:
                    table_data.append(row_data)
            if table_data:
                tables.append(table_data)
        return tables

    def _extract_links(self, soup: "BeautifulSoup") -> List[Dict[str, str]]:
        """Extract links from HTML.

        Handles links with missing href attributes or text gracefully.

        Args:
            soup: BeautifulSoup object.

        Returns:
            List of dictionaries with 'href' and 'text' keys.
        """
        links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if not href or not isinstance(href, str) or not href.strip():
                continue
            href = href.strip()
            text = a_tag.get_text(separator=" ", strip=True) or ""
            links.append({"href": href, "text": text})
        return links

    def _rows_per_file(self):
        """Return number of rows per file."""
        return 1

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory size of parsed HTML data.

        Returns:
            Estimated size in bytes, or None if file sizes are unknown.
        """
        sizes = [s for s in self._file_sizes() if s is not None]
        if not sizes:
            return None
        return int(sum(sizes) * self._encoding_ratio)


@DeveloperAPI
class HTMLFileMetadataProvider(DefaultFileMetadataProvider):
    """Metadata provider for HTML files with custom encoding ratio."""

    def __init__(self):
        super().__init__()
        self._encoding_ratio = HTML_ENCODING_RATIO_ESTIMATE_DEFAULT

    def _set_encoding_ratio(self, encoding_ratio: float):
        """Set HTML file encoding ratio.

        Args:
            encoding_ratio: Multiplier for file size estimation.
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
            paths: List of file paths.
            rows_per_file: Number of rows per file.
            file_sizes: List of file sizes in bytes.

        Returns:
            BlockMetadata with adjusted size estimate.
        """
        metadata = super()._get_block_metadata(
            paths, rows_per_file=rows_per_file, file_sizes=file_sizes
        )
        if metadata.size_bytes is not None:
            metadata.size_bytes = int(metadata.size_bytes * self._encoding_ratio)
        return metadata
