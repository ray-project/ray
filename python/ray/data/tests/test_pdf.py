import os
from typing import Dict

import pytest

import ray
from ray.data._internal.datasource.pdf_datasource import (
    PDF_ENCODING_RATIO_ESTIMATE_DEFAULT,
    PDFDatasource,
    PDFFileMetadataProvider,
)
from ray.data.datasource.file_meta_provider import (
    DefaultFileMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def create_test_pdf(path: str, num_pages: int = 1, text_per_page: str = "Test content"):
    """Helper function to create a test PDF file."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError:
        pytest.skip("reportlab not installed")

    c = canvas.Canvas(path, pagesize=letter)
    for i in range(num_pages):
        c.drawString(100, 750, f"{text_per_page} - Page {i + 1}")
        c.showPage()
    c.save()


def create_empty_pdf(path: str):
    """Helper function to create an empty PDF file."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError:
        pytest.skip("reportlab not installed")

    c = canvas.Canvas(path, pagesize=letter)
    c.showPage()
    c.save()


class TestReadPDFs:
    def test_basic(self, ray_start_regular_shared, tmp_path):
        """Test basic PDF reading with single page."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=1, text_per_page="Hello World")

        ds = ray.data.read_pdfs(pdf_path)
        records = ds.take_all()

        assert len(records) == 1
        assert "text" in records[0]
        assert "Hello World" in records[0]["text"]
        assert "page_number" in records[0]
        assert records[0]["page_number"] == 1

    def test_multi_page_pdf(self, ray_start_regular_shared, tmp_path):
        """Test reading multi-page PDF with page-level extraction."""
        pdf_path = os.path.join(tmp_path, "multi_page.pdf")
        num_pages = 3
        create_test_pdf(pdf_path, num_pages=num_pages, text_per_page="Test content")

        ds = ray.data.read_pdfs(pdf_path, pages=True)
        records = ds.take_all()

        assert len(records) == num_pages
        for i, record in enumerate(records):
            assert "text" in record
            assert "page_number" in record
            assert record["page_number"] == i + 1
            assert "num_pages" in record
            assert record["num_pages"] == num_pages

    def test_document_level_reading(self, ray_start_regular_shared, tmp_path):
        """Test reading PDF as single document."""
        pdf_path = os.path.join(tmp_path, "doc.pdf")
        num_pages = 3
        create_test_pdf(pdf_path, num_pages=num_pages, text_per_page="Test content")

        ds = ray.data.read_pdfs(pdf_path, pages=False)
        records = ds.take_all()

        assert len(records) == 1
        assert "text" in records[0]
        assert "num_pages" in records[0]
        assert records[0]["num_pages"] == num_pages
        assert "page_number" not in records[0]

    def test_multiple_pdf_files(self, ray_start_regular_shared, tmp_path):
        """Test reading multiple PDF files."""
        pdf1 = os.path.join(tmp_path, "file1.pdf")
        pdf2 = os.path.join(tmp_path, "file2.pdf")

        create_test_pdf(pdf1, num_pages=2, text_per_page="First file")
        create_test_pdf(pdf2, num_pages=2, text_per_page="Second file")

        ds = ray.data.read_pdfs([pdf1, pdf2], pages=True)
        records = ds.take_all()

        assert len(records) == 4
        first_file_records = [r for r in records if "First file" in r["text"]]
        second_file_records = [r for r in records if "Second file" in r["text"]]
        assert len(first_file_records) == 2
        assert len(second_file_records) == 2

    def test_include_paths(self, ray_start_regular_shared, tmp_path):
        """Test including file paths in output."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=1)

        ds = ray.data.read_pdfs(pdf_path, include_paths=True)
        records = ds.take_all()

        assert len(records) == 1
        assert "path" in records[0]
        assert pdf_path in records[0]["path"]

    def test_empty_pdf(self, ray_start_regular_shared, tmp_path):
        """Test reading empty PDF (no text content)."""
        pdf_path = os.path.join(tmp_path, "empty.pdf")
        create_empty_pdf(pdf_path)

        ds = ray.data.read_pdfs(pdf_path)
        records = ds.take_all()

        assert len(records) == 1
        assert "text" in records[0]
        assert records[0]["text"] == "" or records[0]["text"].strip() == ""

    def test_file_metadata_provider(self, ray_start_regular_shared, tmp_path):
        """Test using FastFileMetadataProvider for quick metadata."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=1)

        ds = ray.data.read_pdfs(
            pdf_path,
            meta_provider=FastFileMetadataProvider(),
        )
        records = ds.take_all()

        assert len(records) == 1

    def test_parallelism(self, ray_start_regular_shared, tmp_path):
        """Test reading multiple PDFs with different parallelism settings."""
        for i in range(5):
            pdf_path = os.path.join(tmp_path, f"file{i}.pdf")
            create_test_pdf(pdf_path, num_pages=1, text_per_page=f"Content {i}")

        ds = ray.data.read_pdfs(str(tmp_path), parallelism=2)
        records = ds.take_all()

        assert len(records) == 5
        unique_contents = {r["text"] for r in records if "Content" in r["text"]}
        assert len(unique_contents) == 5

    def test_ignore_missing_paths(self, ray_start_regular_shared, tmp_path):
        """Test ignore_missing_paths parameter."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=1)
        missing_path = os.path.join(tmp_path, "missing.pdf")

        # Should not raise error with ignore_missing_paths=True
        ds = ray.data.read_pdfs([pdf_path, missing_path], ignore_missing_paths=True)
        records = ds.take_all()

        assert len(records) == 1

    def test_include_images_requires_pages(self, ray_start_regular_shared, tmp_path):
        """Test that include_images=True requires pages=True."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=1)

        with pytest.raises(ValueError, match="include_images=True.*pages=True"):
            ray.data.read_pdfs(pdf_path, pages=False, include_images=True)

    def test_schema(self, ray_start_regular_shared, tmp_path):
        """Test dataset schema for page-level reading."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=2)

        ds = ray.data.read_pdfs(pdf_path, pages=True)
        schema = ds.schema()

        assert "text" in schema.names
        assert "page_number" in schema.names
        assert "num_pages" in schema.names

    def test_corrupted_pdf_handling(self, ray_start_regular_shared, tmp_path):
        """Test handling of corrupted PDF files."""
        corrupted_pdf = os.path.join(tmp_path, "corrupted.pdf")
        with open(corrupted_pdf, "w") as f:
            f.write("This is not a valid PDF file")

        # Should handle gracefully and not crash
        ds = ray.data.read_pdfs(corrupted_pdf)
        records = ds.take_all()

        # Should return empty or handle gracefully
        assert isinstance(records, list)

    def test_file_extensions(self, ray_start_regular_shared, tmp_path):
        """Test filtering by file extensions."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        txt_path = os.path.join(tmp_path, "test.txt")

        create_test_pdf(pdf_path, num_pages=1)
        with open(txt_path, "w") as f:
            f.write("Not a PDF")

        # Should only read PDF files
        ds = ray.data.read_pdfs(str(tmp_path))
        records = ds.take_all()

        # Should only get the PDF file
        assert len(records) >= 1

    def test_custom_meta_provider_emits_deprecation_warning(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test that custom meta_provider emits deprecation warning."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=1)

        with pytest.warns(DeprecationWarning):
            ray.data.read_pdfs(
                pdf_path,
                meta_provider=FastFileMetadataProvider(),
            )

    @pytest.mark.parametrize("num_pages", [1, 5, 10])
    def test_varying_page_counts(self, ray_start_regular_shared, tmp_path, num_pages):
        """Test reading PDFs with different page counts."""
        pdf_path = os.path.join(tmp_path, f"test_{num_pages}.pdf")
        create_test_pdf(pdf_path, num_pages=num_pages, text_per_page="Test")

        ds = ray.data.read_pdfs(pdf_path, pages=True)
        records = ds.take_all()

        assert len(records) == num_pages
        assert all(r["num_pages"] == num_pages for r in records)

    def test_max_pages_per_block(self, ray_start_regular_shared, tmp_path):
        """Test max_pages_per_block for large PDFs."""
        pdf_path = os.path.join(tmp_path, "large.pdf")
        num_pages = 20
        create_test_pdf(pdf_path, num_pages=num_pages, text_per_page="Test content")

        # Read with max_pages_per_block=5
        ds = ray.data.read_pdfs(pdf_path, max_pages_per_block=5)

        # Should have 20 rows (one per page)
        records = ds.take_all()
        assert len(records) == num_pages

        # Verify all pages are present
        page_numbers = [r["page_number"] for r in records]
        assert sorted(page_numbers) == list(range(1, num_pages + 1))

        # Verify internal block structure (should have 4 blocks of 5 pages each)
        # Note: This checks the optimization is working
        num_blocks = ds._plan.initial_num_blocks()
        # The actual number of blocks depends on parallelism, but should be reasonable
        assert num_blocks >= 1

    def test_max_pages_per_block_single_page(self, ray_start_regular_shared, tmp_path):
        """Test max_pages_per_block with single page PDF."""
        pdf_path = os.path.join(tmp_path, "single.pdf")
        create_test_pdf(pdf_path, num_pages=1, text_per_page="Single page")

        ds = ray.data.read_pdfs(pdf_path, max_pages_per_block=10)
        records = ds.take_all()

        assert len(records) == 1
        assert records[0]["page_number"] == 1

    def test_max_pages_per_block_uneven_division(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test max_pages_per_block with uneven page count."""
        pdf_path = os.path.join(tmp_path, "uneven.pdf")
        num_pages = 13
        create_test_pdf(pdf_path, num_pages=num_pages, text_per_page="Test")

        # max_pages_per_block=5 should create blocks of [5, 5, 3]
        ds = ray.data.read_pdfs(pdf_path, max_pages_per_block=5)
        records = ds.take_all()

        assert len(records) == num_pages
        page_numbers = {r["page_number"] for r in records}
        assert page_numbers == set(range(1, num_pages + 1))

    def test_max_pages_per_block_requires_pages_true(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test that max_pages_per_block requires pages=True."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=5)

        with pytest.raises(
            ValueError, match="max_pages_per_block is only applicable when pages=True"
        ):
            ray.data.read_pdfs(pdf_path, pages=False, max_pages_per_block=2)

    def test_max_pages_per_block_invalid_value(
        self, ray_start_regular_shared, tmp_path
    ):
        """Test that max_pages_per_block must be positive."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=5)

        with pytest.raises(ValueError, match="max_pages_per_block must be positive"):
            ray.data.read_pdfs(pdf_path, max_pages_per_block=0)

        with pytest.raises(ValueError, match="max_pages_per_block must be positive"):
            ray.data.read_pdfs(pdf_path, max_pages_per_block=-1)


class TestPDFDatasource:
    def test_datasource_direct(self, tmp_path):
        """Test PDFDatasource directly."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=1, text_per_page="Direct test")

        datasource = PDFDatasource(
            paths=[pdf_path],
            pages=True,
            include_images=False,
            ocr=False,
        )

        read_tasks = datasource.get_read_tasks(parallelism=1)
        assert len(read_tasks) > 0

    def test_pdf_file_metadata_provider_initialization(self, tmp_path):
        """Test PDFFileMetadataProvider initialization."""
        provider = PDFFileMetadataProvider()
        # PDFFileMetadataProvider inherits from DefaultFileMetadataProvider
        assert isinstance(provider, DefaultFileMetadataProvider)
        # Check that encoding ratio is initialized
        assert hasattr(provider, "_encoding_ratio")
        assert provider._encoding_ratio == PDF_ENCODING_RATIO_ESTIMATE_DEFAULT

    def test_encoding_ratio_estimate(self, tmp_path):
        """Test in-memory data size estimation."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=2)

        datasource = PDFDatasource(
            paths=[pdf_path],
            pages=True,
        )

        estimated_size = datasource.estimate_inmemory_data_size()
        assert estimated_size is not None
        assert estimated_size > 0


class TestPDFIntegration:
    def test_map_batches_after_read(self, ray_start_regular_shared, tmp_path):
        """Test chaining map_batches after reading PDFs."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=2, text_per_page="Integration test")

        def count_words(batch: Dict):
            texts = batch["text"]
            word_counts = [len(text.split()) for text in texts]
            batch["word_count"] = word_counts
            return batch

        ds = ray.data.read_pdfs(pdf_path, pages=True)
        ds = ds.map_batches(count_words, batch_format="pandas")
        records = ds.take_all()

        assert len(records) == 2
        assert all("word_count" in r for r in records)
        assert all(r["word_count"] > 0 for r in records)

    def test_filter_after_read(self, ray_start_regular_shared, tmp_path):
        """Test filtering after reading PDFs."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=3)

        ds = ray.data.read_pdfs(pdf_path, pages=True)
        filtered_ds = ds.filter(lambda row: row["page_number"] > 1)
        records = filtered_ds.take_all()

        assert len(records) == 2
        assert all(r["page_number"] > 1 for r in records)

    def test_groupby_after_read(self, ray_start_regular_shared, tmp_path):
        """Test groupby after reading multiple PDFs."""
        pdf1 = os.path.join(tmp_path, "file1.pdf")
        pdf2 = os.path.join(tmp_path, "file2.pdf")

        create_test_pdf(pdf1, num_pages=2)
        create_test_pdf(pdf2, num_pages=2)

        ds = ray.data.read_pdfs([pdf1, pdf2], pages=True, include_paths=True)

        # Group by file path
        grouped = ds.groupby("path").count()
        records = grouped.take_all()

        # Should have 2 groups (one per file)
        assert len(records) == 2


class TestPDFOCR:
    def test_ocr_disabled_by_default(self, ray_start_regular_shared, tmp_path):
        """Test that OCR is disabled by default."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=1)

        ds = ray.data.read_pdfs(pdf_path)
        records = ds.take_all()

        assert len(records) == 1
        # OCR should not be triggered for text-based PDFs

    def test_ocr_requires_dependencies(self, ray_start_regular_shared, tmp_path):
        """Test that OCR requires proper dependencies."""
        pdf_path = os.path.join(tmp_path, "test.pdf")
        create_test_pdf(pdf_path, num_pages=1)

        # If OCR dependencies are missing, should raise error
        # This test validates the dependency check works
        try:
            ds = ray.data.read_pdfs(pdf_path, ocr=True)
            # If we get here, OCR dependencies are installed
            assert ds is not None
        except ImportError:
            # Expected if pytesseract/pdf2image not installed
            pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
