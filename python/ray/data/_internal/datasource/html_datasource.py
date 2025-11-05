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

        try:
            soup = BeautifulSoup(html_content, "lxml")
            parser = "lxml"
        except Exception:
            try:
                soup = BeautifulSoup(html_content, "html.parser")
                parser = "html.parser"
            except Exception as e:
                raise ValueError(f"Failed to parse HTML file '{path}': {e}") from e

        if self.extract_metadata:
            doc_metadata = self._extract_document_metadata(soup)
        else:
            doc_metadata = None

        content_soup = soup
        if self.selector:
            elements = soup.select(self.selector)
            if not elements:
                logger.warning(f"CSS selector '{self.selector}' matched no elements in '{path}'")
                content_soup = BeautifulSoup("", parser)
            else:
                import copy

                from bs4 import BeautifulSoup as BS

                new_soup = BS("<html><body></body></html>", parser)
                for elem in elements:
                    new_soup.body.append(copy.deepcopy(elem))
                content_soup = new_soup

        if self.extract_metadata:
            content_metadata = self._extract_content_metadata(content_soup)
            metadata = {**(doc_metadata or {}), **(content_metadata or {})}
        else:
            metadata = None

        builder = DelegatingBlockBuilder()
        builder.add(self._extract_content(content_soup, path, metadata))
        yield builder.build()

    def _decode_html(self, data: bytes, path: str) -> str:
        """Decode HTML with encoding detection."""
        for encoding in ["utf-8", "latin-1", "cp1252", "iso-8859-1"]:
            try:
                return data.decode(encoding)
            except UnicodeDecodeError:
                continue
        logger.warning(f"Could not detect encoding for '{path}', using utf-8 with error replacement")
        return data.decode("utf-8", errors="replace")

    def _extract_content(
        self,
        soup: "BeautifulSoup",
        path: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Extract content from parsed HTML."""
        row_data = {self._COLUMN_NAME: self._extract_text(soup)}
        if metadata:
            row_data.update(metadata)
        if self.extract_tables:
            row_data["tables"] = self._extract_tables(soup)
        if self.extract_links:
            row_data["links"] = self._extract_links(soup)
        return row_data

    def _extract_text(self, soup: "BeautifulSoup") -> str:
        """Extract text based on text_mode."""
        if self.text_mode == "clean":
            return self._extract_clean_text(soup)
        elif self.text_mode == "raw":
            return soup.get_text()
        elif self.text_mode == "markdown":
            return self._extract_markdown(soup)
        return ""

    def _extract_clean_text(self, soup: "BeautifulSoup") -> str:
        """Extract clean text from HTML, removing scripts, styles, and extra whitespace."""
        for element in soup(["script", "style", "noscript", "iframe"]):
            element.decompose()
        return " ".join(soup.get_text(separator=" ").split())

    def _extract_markdown(self, soup: "BeautifulSoup") -> str:
        """Convert HTML to markdown."""
        from markdownify import markdownify as md

        for element in soup(["script", "style", "noscript", "iframe"]):
            element.decompose()
        return md(str(soup), heading_style="ATX", bullets="-").strip()

    def _extract_document_metadata(self, soup: "BeautifulSoup") -> Dict[str, Any]:
        """Extract document-level metadata from HTML <head> section."""
        metadata = {}
        title_tag = soup.find("title")
        metadata["title"] = title_tag.get_text().strip() if title_tag else None
        meta_desc = soup.find("meta", attrs={"name": "description"})
        metadata["description"] = meta_desc.get("content") if meta_desc else None
        meta_keywords = soup.find("meta", attrs={"name": "keywords"})
        metadata["keywords"] = meta_keywords.get("content") if meta_keywords else None
        return metadata

    def _extract_content_metadata(self, soup: "BeautifulSoup") -> Dict[str, Any]:
        """Extract content-specific metadata from HTML (headers h1-h6)."""
        headers = {}
        for i in range(1, 7):
            h_tags = soup.find_all(f"h{i}")
            if h_tags:
                headers[f"h{i}"] = [h.get_text().strip() for h in h_tags]
        return {"headers": headers} if headers else {}

    def _extract_tables(self, soup: "BeautifulSoup") -> List[List[List[str]]]:
        """Extract tables from HTML."""
        tables = []
        for table in soup.find_all("table"):
            table_data = []
            for row in table.find_all("tr"):
                row_data = [cell.get_text().strip() for cell in row.find_all(["td", "th"])]
                if row_data:
                    table_data.append(row_data)
            if table_data:
                tables.append(table_data)
        return tables

    def _extract_links(self, soup: "BeautifulSoup") -> List[Dict[str, str]]:
        """Extract links from HTML."""
        return [
            {"href": a_tag["href"], "text": a_tag.get_text().strip()}
            for a_tag in soup.find_all("a", href=True)
        ]

    def _rows_per_file(self):
        """Return number of rows per file."""
        return 1

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory size of parsed HTML data."""
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
        """Set HTML file encoding ratio."""
        self._encoding_ratio = encoding_ratio

    def _get_block_metadata(
        self,
        paths: List[str],
        *,
        rows_per_file: Optional[int],
        file_sizes: List[Optional[int]],
    ) -> BlockMetadata:
        """Get block metadata with adjusted size estimate."""
        metadata = super()._get_block_metadata(
            paths, rows_per_file=rows_per_file, file_sizes=file_sizes
        )
        if metadata.size_bytes is not None:
            metadata.size_bytes = int(metadata.size_bytes * self._encoding_ratio)
        return metadata
