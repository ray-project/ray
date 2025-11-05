import io
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockMetadata
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
    """Metadata provider for PDF files with custom encoding ratio."""

    def __init__(self):
        super().__init__()
        self._encoding_ratio = PDF_ENCODING_RATIO_ESTIMATE_DEFAULT

    def _set_encoding_ratio(self, encoding_ratio: float):
        """Set custom encoding ratio for memory estimation."""
        self._encoding_ratio = max(encoding_ratio, PDF_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)

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


class PDFDatasource(FileBasedDatasource):
    """Datasource for reading PDF files and extracting text content."""

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
            paths: Path(s) to PDF file(s).
            pages: If True, each row represents a page. If False, each row represents a document.
            include_images: If True, extract images from PDFs. Requires pages=True.
            ocr: If True, use OCR for image-based PDFs. Requires pytesseract and pdf2image.
            ocr_config: Configuration options for OCR processing.
            max_pages_per_block: Maximum pages per block for large PDFs. Only when pages=True.
            **file_based_datasource_kwargs: Additional arguments for FileBasedDatasource.
        """
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="PyPDF2", package="PyPDF2")
        if ocr:
            _check_import(self, module="pytesseract", package="pytesseract")
            _check_import(self, module="PIL", package="Pillow")
            _check_import(self, module="pdf2image", package="pdf2image")

        if include_images and not pages:
            raise ValueError("include_images=True requires pages=True")
        if max_pages_per_block is not None:
            if max_pages_per_block <= 0:
                raise ValueError(f"max_pages_per_block must be positive, got {max_pages_per_block}")
            if not pages:
                raise ValueError("max_pages_per_block is only applicable when pages=True")

        self.pages = pages
        self.include_images = include_images
        self.ocr = ocr
        self.ocr_config = ocr_config or {}
        self.max_pages_per_block = max_pages_per_block

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
        """Read and parse a single PDF file."""
        from PyPDF2 import PdfReader
        from PyPDF2.errors import PdfReadError

        try:
            data = f.readall()
            reader = PdfReader(io.BytesIO(data))
            if self.pages:
                yield from self._read_pages(reader, path, data)
            else:
                yield from self._read_document(reader, path, data)
        except PdfReadError as e:
            logger.warning(f"Failed to read PDF file '{path}': {e}. Skipping file.")
        except Exception as e:
            logger.warning(f"Unexpected error reading PDF file '{path}': {e}. Skipping file.")

    def _process_page_data(
        self,
        page: "PyPDF2.PageObject",
        page_num: int,
        num_pages: int,
        reader: "PyPDF2.PdfReader",
        pdf_bytes: bytes,
    ) -> Dict[str, Any]:
        """Process a single PDF page and extract its data."""
        text = self._extract_text_from_page(page, page_num, pdf_bytes)
        row_data = {
            "text": text,
            "page_number": page_num + 1,
            "num_pages": num_pages,
        }

        if hasattr(page, "mediabox"):
            mediabox = page.mediabox
            row_data["page_width"] = float(mediabox.width)
            row_data["page_height"] = float(mediabox.height)

        if self.include_images:
            images = self._extract_images_from_page(page)
            row_data["images"] = images
            row_data["num_images"] = len(images)

        if page_num == 0:
            row_data.update(self._extract_document_metadata(reader))

        return row_data

    def _read_pages(
        self,
        reader: "PyPDF2.PdfReader",
        path: str,
        pdf_bytes: bytes,
    ) -> Iterator[Block]:
        """Read PDF pages individually or in batches."""
        num_pages = len(reader.pages)

        if self.max_pages_per_block is None:
            for page_num in range(num_pages):
                try:
                    row_data = self._process_page_data(
                        reader.pages[page_num], page_num, num_pages, reader, pdf_bytes
                    )
                    builder = DelegatingBlockBuilder()
                    builder.add(row_data)
                    yield builder.build()
                except Exception as e:
                    logger.warning(f"Failed to extract content from page {page_num + 1} of '{path}': {e}. Skipping page.")
                    continue
        else:
            for block_start in range(0, num_pages, self.max_pages_per_block):
                block_end = min(block_start + self.max_pages_per_block, num_pages)
                builder = DelegatingBlockBuilder()
                for page_num in range(block_start, block_end):
                    try:
                        row_data = self._process_page_data(
                            reader.pages[page_num], page_num, num_pages, reader, pdf_bytes
                        )
                        builder.add(row_data)
                    except Exception as e:
                        logger.warning(f"Failed to extract content from page {page_num + 1} of '{path}': {e}. Skipping page.")
                        continue
                if builder.num_rows() > 0:
                    yield builder.build()

    def _read_document(
        self,
        reader: "PyPDF2.PdfReader",
        path: str,
        pdf_bytes: bytes,
    ) -> Iterator[Block]:
        """Read entire PDF document as single row."""
        num_pages = len(reader.pages)
        all_text = []
        for page_num in range(num_pages):
            try:
                text = self._extract_text_from_page(reader.pages[page_num], page_num, pdf_bytes)
                if text.strip():
                    all_text.append(text)
            except Exception as e:
                logger.warning(f"Failed to extract text from page {page_num + 1} of '{path}': {e}. Skipping page.")
                continue

        row_data = {
            "text": "\n\n".join(all_text),
            "num_pages": num_pages,
        }
        row_data.update(self._extract_document_metadata(reader))

        builder = DelegatingBlockBuilder()
        builder.add(row_data)
        yield builder.build()

    def _extract_text_from_page(
        self, page: "PyPDF2.PageObject", page_num: int, pdf_bytes: bytes
    ) -> str:
        """Extract text from a PDF page."""
        try:
            text = page.extract_text()
            if self.ocr and (not text or not text.strip()):
                text = self._ocr_page(page_num, pdf_bytes)
            return text or ""
        except Exception as e:
            logger.warning(f"Failed to extract text from page: {e}")
            return ""

    def _ocr_page(self, page_num: int, pdf_bytes: bytes) -> str:
        """Extract text from page using OCR."""
        try:
            import pytesseract
            from pdf2image import convert_from_bytes

            images = convert_from_bytes(
                pdf_bytes, first_page=page_num + 1, last_page=page_num + 1
            )
            if not images:
                return ""
            return pytesseract.image_to_string(images[0], **self.ocr_config) or ""
        except ImportError as e:
            logger.warning(f"OCR requested but dependencies not available: {e}")
            return ""
        except Exception as e:
            logger.warning(f"OCR failed for page: {e}")
            return ""

    def _extract_images_from_page(
        self,
        page: "PyPDF2.PageObject",
    ) -> List[Dict[str, Any]]:
        """Extract images from a PDF page."""
        images = []
        try:
            if "/Resources" not in page or "/XObject" not in page["/Resources"]:
                return images

            x_objects = page["/Resources"]["/XObject"].get_object()
            for obj_name in x_objects:
                obj = x_objects[obj_name]
                if obj["/Subtype"] == "/Image":
                    try:
                        images.append(
                            {
                                "name": obj_name,
                                "width": obj["/Width"],
                                "height": obj["/Height"],
                                "color_space": str(obj.get("/ColorSpace", "Unknown")),
                                "data": obj.get_data(),
                            }
                        )
                    except Exception as e:
                        logger.warning(f"Failed to extract image '{obj_name}': {e}")
        except Exception as e:
            logger.warning(f"Failed to extract images from page: {e}")
        return images

    def _extract_document_metadata(
        self,
        reader: "PyPDF2.PdfReader",
    ) -> Dict[str, Any]:
        """Extract document-level metadata from PDF."""
        metadata = {}
        try:
            if reader.metadata:
                info = reader.metadata
                for key, field in [
                    ("/Title", "title"),
                    ("/Author", "author"),
                    ("/Subject", "subject"),
                    ("/Creator", "creator"),
                    ("/Producer", "producer"),
                    ("/CreationDate", "creation_date"),
                    ("/ModDate", "modification_date"),
                ]:
                    if key in info:
                        metadata[field] = str(info[key])
        except Exception as e:
            logger.warning(f"Failed to extract document metadata: {e}")
        return metadata

    def _rows_per_file(self) -> Optional[int]:
        """Return number of rows per file."""
        return None if self.pages else 1

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory size of parsed PDF data."""
        sizes = [s for s in self._file_sizes() if s is not None]
        if not sizes:
            return None
        ratio = self._encoding_ratio * (2.0 if self.include_images else 1.0)
        return int(sum(sizes) * ratio)

    def _estimate_single_file_ratio(self, path: str, file_size: int) -> Optional[float]:
        """Estimate encoding ratio for a single PDF file."""
        try:
            from PyPDF2 import PdfReader

            with self._filesystem.open_input_file(path) as f:
                data = f.readall()
                reader = PdfReader(io.BytesIO(data))

                pages_to_sample = min(5, len(reader.pages))
                text_size = sum(
                    len(reader.pages[i].extract_text().encode("utf-8"))
                    for i in range(pages_to_sample)
                )

                if len(reader.pages) > pages_to_sample:
                    text_size = int(text_size * (len(reader.pages) / pages_to_sample))

                return text_size / file_size if file_size > 0 else None
        except Exception as e:
            logger.warning(f"Failed to estimate encoding ratio for '{path}': {e}")
            return None

    def _calculate_median_ratio(self, ratios: List[float]) -> float:
        """Calculate median ratio from a list of ratios."""
        if not ratios:
            return PDF_ENCODING_RATIO_ESTIMATE_DEFAULT
        ratios.sort()
        return max(ratios[len(ratios) // 2], PDF_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)

    def _estimate_files_encoding_ratio(self) -> float:
        """Estimate encoding ratio by sampling PDF files."""
        import time

        paths = self._paths()
        sample_size = min(5, len(paths))
        if sample_size == 0:
            return PDF_ENCODING_RATIO_ESTIMATE_DEFAULT

        ratios = []
        timeout_seconds = 10
        start_time = time.time()

        for path in paths[:sample_size]:
            if time.time() - start_time > timeout_seconds:
                logger.warning(f"PDF encoding ratio estimation timed out after sampling {len(ratios)} files.")
                break

            file_info = self._filesystem.get_file_info(path)
            if file_info.size == 0:
                continue

            ratio = self._estimate_single_file_ratio(path, file_info.size)
            if ratio is not None:
                ratios.append(ratio)

        return self._calculate_median_ratio(ratios)
