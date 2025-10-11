import io
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.data.datasource.file_meta_provider import DefaultFileMetadataProvider

if TYPE_CHECKING:
    import pyarrow
    import PyPDF2


logger = logging.getLogger(__name__)

# Default encoding ratio estimate for PDF files.
# PDFs are typically compressed, so the in-memory size is larger than file size.
# This ratio accounts for decompression and text extraction overhead.
PDF_ENCODING_RATIO_ESTIMATE_DEFAULT = 3.0

# Lower bound for PDF encoding ratio estimation.
PDF_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 1.5


class PDFFileMetadataProvider(DefaultFileMetadataProvider):
    """Metadata provider for PDF files with custom encoding ratio.

    This provider estimates the in-memory size of PDF files by considering
    the decompression and text extraction overhead. PDFs are typically compressed,
    so the in-memory representation is larger than the file size on disk.
    """

    def __init__(self):
        super().__init__()
        self._encoding_ratio = PDF_ENCODING_RATIO_ESTIMATE_DEFAULT

    def _set_encoding_ratio(self, encoding_ratio: float):
        """Set custom encoding ratio for more accurate memory estimation."""
        self._encoding_ratio = max(
            encoding_ratio, PDF_ENCODING_RATIO_ESTIMATE_LOWER_BOUND
        )


class PDFDatasource(FileBasedDatasource):
    """Datasource for reading PDF files.

    This datasource reads PDF files and extracts text content page by page.
    Each row in the resulting dataset represents either a single page (if
    `pages=True`) or an entire document (if `pages=False`).

    The datasource supports:
    - Text extraction from both text-based and image-based PDFs
    - Page-level metadata (page numbers, dimensions)
    - Document-level metadata (title, author, creation date)
    - OCR for image-based PDFs (requires additional dependencies)
    """

    _FILE_EXTENSIONS = ["pdf"]
    # Use multi-threading for PDF parsing since it can be CPU-intensive
    _NUM_THREADS_PER_TASK = 4

    def __init__(
        self,
        paths: Union[str, List[str]],
        *,
        pages: bool = True,
        include_images: bool = False,
        ocr: bool = False,
        ocr_config: Optional[Dict[str, Any]] = None,
        max_pages_per_block: Optional[int] = None,
        **file_based_datasource_kwargs,
    ):
        """Initialize PDF datasource.

        Args:
            paths: A single file or directory path, or a list of file or directory paths.
                A list of paths can contain both files and directories.
            pages: If True, each row represents a page. If False, each row represents
                a complete document with all pages combined. Defaults to True.
            include_images: If True, extract images from PDFs and include them in the
                output. This increases memory usage significantly. Defaults to False.
            ocr: If True, use OCR (Optical Character Recognition) to extract text from
                image-based PDFs. Requires pytesseract, pdf2image, and Pillow to be installed.
                Defaults to False.
            ocr_config: Configuration options for OCR processing. Only used if ocr=True.
                See pytesseract documentation for available options. Defaults to None.
            max_pages_per_block: Maximum number of pages to include in a single block.
                This helps manage memory usage for very large PDF files. If None, all
                pages are included in their respective blocks based on the pages parameter.
                For example, if pages=True and max_pages_per_block=10, a 100-page PDF
                will yield 10 blocks of 10 pages each. Only applicable when pages=True.
                Defaults to None (no limit).
            **file_based_datasource_kwargs: Additional arguments passed to
                FileBasedDatasource constructor (filesystem, partition_filter, etc.).

        Raises:
            ValueError: If invalid configuration options are provided.
            ImportError: If required dependencies (PyPDF2, pytesseract, pdf2image) are not installed.
        """
        super().__init__(paths, **file_based_datasource_kwargs)

        # Check for required dependency
        _check_import(self, module="PyPDF2", package="PyPDF2")

        if ocr:
            # Check for OCR dependencies if OCR is enabled
            _check_import(self, module="pytesseract", package="pytesseract")
            _check_import(self, module="PIL", package="Pillow")
            _check_import(self, module="pdf2image", package="pdf2image")

        if include_images and not pages:
            raise ValueError(
                "include_images=True requires pages=True. Cannot include images "
                "when pages=False (document-level reading)."
            )

        if max_pages_per_block is not None:
            if max_pages_per_block <= 0:
                raise ValueError(
                    f"max_pages_per_block must be positive, got {max_pages_per_block}"
                )
            if not pages:
                raise ValueError(
                    "max_pages_per_block is only applicable when pages=True. "
                    "When pages=False, the entire document is returned as one row."
                )

        self.pages = pages
        self.include_images = include_images
        self.ocr = ocr
        self.ocr_config = ocr_config or {}
        self.max_pages_per_block = max_pages_per_block

        # Set encoding ratio for memory estimation
        meta_provider = file_based_datasource_kwargs.get("meta_provider", None)
        if isinstance(meta_provider, PDFFileMetadataProvider):
            self._encoding_ratio = self._estimate_files_encoding_ratio()
            meta_provider._set_encoding_ratio(self._encoding_ratio)
        else:
            self._encoding_ratio = PDF_ENCODING_RATIO_ESTIMATE_DEFAULT

    def _read_stream(
        self,
        f: "pyarrow.NativeFile",
        path: str,
    ) -> Iterator[Block]:
        """Read and parse a single PDF file.

        This method extracts text and metadata from each page in the PDF file.
        It yields one or more blocks containing the parsed data.

        Args:
            f: The file handle from PyArrow filesystem.
            path: The path to the PDF file being read.

        Yields:
            Blocks containing parsed PDF data. Each block contains one or more rows
            depending on the configuration (page-level or document-level).

        Raises:
            ValueError: If the PDF file is corrupted or cannot be parsed.
        """
        from PyPDF2 import PdfReader
        from PyPDF2.errors import PdfReadError

        try:
            # Read file content into memory
            data = f.readall()
            pdf_stream = io.BytesIO(data)

            # Create PDF reader
            reader = PdfReader(pdf_stream)

            if self.pages:
                # Page-level reading: yield one block per page
                yield from self._read_pages(reader, path, data)
            else:
                # Document-level reading: yield one block for entire document
                yield from self._read_document(reader, path, data)

        except PdfReadError as e:
            raise ValueError(
                f"Failed to read PDF file at path '{path}'. "
                f"The file may be corrupted or password-protected. "
                f"Original error: {e}"
            ) from e
        except Exception as e:
            raise ValueError(
                f"Unexpected error while reading PDF file at path '{path}': {e}"
            ) from e

    def _process_page_data(
        self,
        page: "PyPDF2.PageObject",
        page_num: int,
        num_pages: int,
        reader: "PyPDF2.PdfReader",
        pdf_bytes: bytes,
    ) -> Dict[str, Any]:
        """Process a single PDF page and extract its data.

        This helper method contains the common logic for extracting text,
        dimensions, images, and metadata from a single page.

        Args:
            page: PyPDF2 PageObject instance.
            page_num: Zero-based page number.
            num_pages: Total number of pages in the PDF.
            reader: PyPDF2 PdfReader instance.
            pdf_bytes: Raw PDF file bytes.

        Returns:
            Dictionary containing extracted page data.
        """
        # Extract text from page
        text = self._extract_text_from_page(page, page_num, pdf_bytes)

        # Build row data
        row_data = {
            "text": text,
            "page_number": page_num + 1,
            "num_pages": num_pages,
        }

        # Add page dimensions if available
        if hasattr(page, "mediabox"):
            mediabox = page.mediabox
            row_data["page_width"] = float(mediabox.width)
            row_data["page_height"] = float(mediabox.height)

        # Extract and include images if requested
        if self.include_images:
            images = self._extract_images_from_page(page)
            row_data["images"] = images
            row_data["num_images"] = len(images)

        # Add document metadata on first page
        if page_num == 0:
            row_data.update(self._extract_document_metadata(reader))

        return row_data

    def _read_pages(
        self,
        reader: "PyPDF2.PdfReader",
        path: str,
        pdf_bytes: bytes,
    ) -> Iterator[Block]:
        """Read PDF pages individually or in batches.

        If max_pages_per_block is set, pages are batched together into blocks
        to control memory usage for very large PDFs.

        Args:
            reader: PyPDF2 PdfReader instance.
            path: Path to the PDF file.
            pdf_bytes: Raw PDF file bytes.

        Yields:
            Blocks containing individual page data (or batched pages if
            max_pages_per_block is set).
        """
        num_pages = len(reader.pages)

        if self.max_pages_per_block is None:
            # Original behavior: one block per page
            for page_num in range(num_pages):
                try:
                    page = reader.pages[page_num]

                    # Process page data using helper method
                    row_data = self._process_page_data(
                        page, page_num, num_pages, reader, pdf_bytes
                    )

                    # Create block with single row
                    builder = DelegatingBlockBuilder()
                    builder.add(row_data)
                    yield builder.build()

                except Exception as e:
                    logger.warning(
                        f"Failed to extract content from page {page_num + 1} of "
                        f"'{path}': {e}. Skipping page."
                    )
                    continue
        else:
            # Batched behavior: multiple pages per block
            for block_start in range(0, num_pages, self.max_pages_per_block):
                block_end = min(block_start + self.max_pages_per_block, num_pages)
                builder = DelegatingBlockBuilder()

                for page_num in range(block_start, block_end):
                    try:
                        page = reader.pages[page_num]

                        # Process page data using helper method
                        row_data = self._process_page_data(
                            page, page_num, num_pages, reader, pdf_bytes
                        )

                        builder.add(row_data)

                    except Exception as e:
                        logger.warning(
                            f"Failed to extract content from page {page_num + 1} of "
                            f"'{path}': {e}. Skipping page."
                        )
                        continue

                # Yield block with multiple pages
                if builder.num_rows() > 0:
                    yield builder.build()

    def _read_document(
        self,
        reader: "PyPDF2.PdfReader",
        path: str,
        pdf_bytes: bytes,
    ) -> Iterator[Block]:
        """Read entire PDF document as single row.

        Args:
            reader: PyPDF2 PdfReader instance.
            path: Path to the PDF file.
            pdf_bytes: Raw PDF file bytes.

        Yields:
            Block containing document-level data.
        """
        num_pages = len(reader.pages)
        all_text = []

        # Extract text from all pages
        for page_num in range(num_pages):
            try:
                page = reader.pages[page_num]
                text = self._extract_text_from_page(page, page_num, pdf_bytes)
                if text.strip():
                    all_text.append(text)
            except Exception as e:
                logger.warning(
                    f"Failed to extract text from page {page_num + 1} of "
                    f"'{path}': {e}. Skipping page."
                )
                continue

        # Combine all pages
        combined_text = "\n\n".join(all_text)

        # Build row data
        row_data = {
            "text": combined_text,
            "num_pages": num_pages,
        }

        # Add document metadata
        row_data.update(self._extract_document_metadata(reader))

        # Create block
        builder = DelegatingBlockBuilder()
        builder.add(row_data)
        yield builder.build()

    def _extract_text_from_page(
        self, page: "PyPDF2.PageObject", page_num: int, pdf_bytes: bytes
    ) -> str:
        """Extract text from a PDF page.

        Args:
            page: PyPDF2 PageObject instance.
            page_num: Zero-based page number.
            pdf_bytes: Raw PDF file bytes.

        Returns:
            Extracted text content.
        """
        try:
            text = page.extract_text()

            # If text extraction failed or returned empty text, try OCR
            if self.ocr and (not text or not text.strip()):
                text = self._ocr_page(page_num, pdf_bytes)

            return text or ""

        except Exception as e:
            logger.warning(f"Failed to extract text from page: {e}")
            return ""

    def _ocr_page(self, page_num: int, pdf_bytes: bytes) -> str:
        """Extract text from page using OCR.

        This method converts the PDF page to an image and uses Tesseract OCR
        to extract text. This is useful for image-based PDFs or scanned documents.

        Args:
            page_num: Zero-based page number.
            pdf_bytes: Raw PDF file bytes.

        Returns:
            OCR-extracted text content.
        """
        try:
            import pytesseract
            from pdf2image import convert_from_bytes

            # Convert page to image
            # Note: This requires poppler to be installed
            # Use 1-based page numbering for pdf2image
            images = convert_from_bytes(
                pdf_bytes,
                first_page=page_num + 1,
                last_page=page_num + 1,
            )

            if not images:
                return ""

            # Apply OCR to the page image
            text = pytesseract.image_to_string(images[0], **self.ocr_config)
            return text or ""

        except ImportError as e:
            logger.warning(
                f"OCR requested but dependencies not available: {e}. "
                "Install pdf2image and pytesseract for OCR support."
            )
            return ""
        except Exception as e:
            logger.warning(f"OCR failed for page: {e}")
            return ""

    def _extract_images_from_page(
        self,
        page: "PyPDF2.PageObject",
    ) -> List[Dict[str, Any]]:
        """Extract images from a PDF page.

        Args:
            page: PyPDF2 PageObject instance.

        Returns:
            List of dictionaries containing image data and metadata.
        """
        images = []

        try:
            # Check if the page has resources
            if "/Resources" not in page:
                return images

            # Check if the resources contain XObjects (which may include images)
            if "/XObject" not in page["/Resources"]:
                return images

            x_objects = page["/Resources"]["/XObject"].get_object()

            for obj_name in x_objects:
                obj = x_objects[obj_name]

                if obj["/Subtype"] == "/Image":
                    try:
                        # Extract image data
                        size = (obj["/Width"], obj["/Height"])
                        data = obj.get_data()
                        color_space = obj.get("/ColorSpace", "Unknown")

                        images.append(
                            {
                                "name": obj_name,
                                "width": size[0],
                                "height": size[1],
                                "color_space": str(color_space),
                                "data": data,
                            }
                        )
                    except Exception as e:
                        logger.warning(f"Failed to extract image '{obj_name}': {e}")
                        continue

        except Exception as e:
            logger.warning(f"Failed to extract images from page: {e}")

        return images

    def _extract_document_metadata(
        self,
        reader: "PyPDF2.PdfReader",
    ) -> Dict[str, Any]:
        """Extract document-level metadata from PDF.

        Args:
            reader: PyPDF2 PdfReader instance.

        Returns:
            Dictionary containing document metadata.
        """
        metadata = {}

        try:
            if reader.metadata:
                info = reader.metadata

                # Extract common metadata fields
                if "/Title" in info:
                    metadata["title"] = str(info["/Title"])
                if "/Author" in info:
                    metadata["author"] = str(info["/Author"])
                if "/Subject" in info:
                    metadata["subject"] = str(info["/Subject"])
                if "/Creator" in info:
                    metadata["creator"] = str(info["/Creator"])
                if "/Producer" in info:
                    metadata["producer"] = str(info["/Producer"])
                if "/CreationDate" in info:
                    metadata["creation_date"] = str(info["/CreationDate"])
                if "/ModDate" in info:
                    metadata["modification_date"] = str(info["/ModDate"])

        except Exception as e:
            logger.warning(f"Failed to extract document metadata: {e}")

        return metadata

    def _rows_per_file(self) -> Optional[int]:
        """Return number of rows per file.

        For page-level reading, this is None (unknown until parsed).
        For document-level reading, this is always 1.

        Returns:
            1 if pages=False (document-level), None if pages=True (page-level).
        """
        return None if self.pages else 1

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory size of parsed PDF data.

        This estimates the total memory required to hold all PDF content
        after text extraction and parsing. The estimate accounts for:
        - File decompression
        - Text extraction overhead
        - Metadata storage
        - Optional image extraction

        Returns:
            Estimated size in bytes, or None if file sizes are unavailable.
        """
        total_size = 0

        for file_size in self._file_sizes():
            if file_size is not None:
                # Base encoding ratio for text extraction
                estimated_size = file_size * self._encoding_ratio

                # Additional overhead for image extraction
                if self.include_images:
                    # Images can significantly increase memory usage
                    # Assume 2x additional overhead for images
                    estimated_size *= 2.0

                total_size += estimated_size

        return int(total_size) if total_size > 0 else None

    def _estimate_files_encoding_ratio(self) -> float:
        """Estimate encoding ratio by sampling PDF files.

        This method samples a few PDF files to estimate the ratio between
        file size on disk and in-memory size after parsing.

        Returns:
            Estimated encoding ratio (in-memory size / file size).
        """
        import time

        # Sample up to 5 files for estimation
        paths = self._paths()
        sample_size = min(5, len(paths))
        if sample_size == 0:
            return PDF_ENCODING_RATIO_ESTIMATE_DEFAULT

        ratios = []
        timeout_seconds = 10
        start_time = time.time()

        for path in paths[:sample_size]:
            try:
                # Check global timeout
                elapsed_time = time.time() - start_time
                if elapsed_time > timeout_seconds:
                    logger.warning(
                        f"PDF encoding ratio estimation timed out after sampling "
                        f"{len(ratios)} files. Using partial estimate."
                    )
                    break

                # Get file size
                file_info = self._filesystem.get_file_info(path)
                file_size = file_info.size

                if file_size == 0:
                    continue

                # Read and parse a sample
                with self._filesystem.open_input_file(path) as f:
                    data = f.readall()
                    pdf_stream = io.BytesIO(data)

                    from PyPDF2 import PdfReader

                    reader = PdfReader(pdf_stream)

                    # Estimate memory size based on extracted text
                    text_size = 0
                    for page_num in range(min(5, len(reader.pages))):
                        page = reader.pages[page_num]
                        text = page.extract_text()
                        text_size += len(text.encode("utf-8"))

                    # Extrapolate to full document
                    if len(reader.pages) > 5:
                        text_size = text_size * (len(reader.pages) / 5)

                    # Calculate ratio
                    ratio = (
                        text_size / file_size
                        if file_size > 0
                        else PDF_ENCODING_RATIO_ESTIMATE_DEFAULT
                    )
                    ratios.append(ratio)

            except Exception as e:
                logger.warning(
                    f"Failed to estimate encoding ratio for '{path}': {e}. "
                    "Continuing with remaining samples."
                )
                continue

        if ratios:
            # Use median to avoid outliers
            ratios.sort()
            median_ratio = ratios[len(ratios) // 2]
            # Ensure ratio is within reasonable bounds
            return max(median_ratio, PDF_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)

        return PDF_ENCODING_RATIO_ESTIMATE_DEFAULT
