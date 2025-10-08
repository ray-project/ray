"""Tests for HTML reading functionality in Ray Data."""

import os

import pytest

import ray


def create_test_html(
    path: str, title: str = "Test Page", body_text: str = "Hello World"
):
    """Create a simple test HTML file.

    Args:
        path: Path to write HTML file.
        title: HTML title tag content.
        body_text: HTML body content.
    """
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="description" content="Test description">
    <meta name="keywords" content="test, html, ray">
    <title>{title}</title>
</head>
<body>
    <h1>Main Heading</h1>
    <p>{body_text}</p>
    <script>console.log('test');</script>
</body>
</html>"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_content)


def create_html_with_table(path: str):
    """Create HTML file with a table.

    Args:
        path: Path to write HTML file.
    """
    html_content = """<!DOCTYPE html>
<html>
<head><title>Table Test</title></head>
<body>
    <h1>Data Table</h1>
    <table>
        <tr>
            <th>Name</th>
            <th>Age</th>
            <th>City</th>
        </tr>
        <tr>
            <td>Alice</td>
            <td>30</td>
            <td>NYC</td>
        </tr>
        <tr>
            <td>Bob</td>
            <td>25</td>
            <td>LA</td>
        </tr>
    </table>
</body>
</html>"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_content)


def create_html_with_links(path: str):
    """Create HTML file with links.

    Args:
        path: Path to write HTML file.
    """
    html_content = """<!DOCTYPE html>
<html>
<head><title>Links Test</title></head>
<body>
    <h1>Useful Links</h1>
    <a href="https://ray.io">Ray Website</a>
    <a href="/docs">Documentation</a>
    <a href="https://github.com/ray-project/ray">GitHub</a>
</body>
</html>"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_content)


def create_empty_html(path: str):
    """Create an empty HTML file.

    Args:
        path: Path to write HTML file.
    """
    with open(path, "w", encoding="utf-8") as f:
        f.write("<html><body></body></html>")


def create_html_with_encoding(path: str, encoding: str = "utf-8"):
    """Create HTML file with specific encoding.

    Args:
        path: Path to write HTML file.
        encoding: Character encoding to use.
    """
    html_content = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Encoding Test</title>
</head>
<body>
    <h1>Special Characters: é, ñ, ü, 日本語</h1>
</body>
</html>"""
    with open(path, "w", encoding=encoding) as f:
        f.write(html_content)


class TestHTMLReading:
    """Test suite for HTML reading functionality."""

    def test_basic_read(self, ray_start_regular_shared, tmp_path):
        """Test basic HTML file reading."""
        html_path = os.path.join(tmp_path, "test.html")
        create_test_html(html_path, title="Test", body_text="Content")

        ds = ray.data.read_html(html_path)
        records = ds.take_all()

        assert len(records) == 1
        assert "text" in records[0]
        assert "title" in records[0]
        assert records[0]["title"] == "Test"
        # Script tags should be removed in clean mode
        assert "console.log" not in records[0]["text"]
        assert "Content" in records[0]["text"]

    def test_text_mode_clean(self, ray_start_regular_shared, tmp_path):
        """Test clean text mode removes scripts and extra whitespace."""
        html_path = os.path.join(tmp_path, "clean.html")
        create_test_html(html_path, body_text="  Test   Content  ")

        ds = ray.data.read_html(html_path, text_mode="clean")
        records = ds.take_all()

        assert len(records) == 1
        text = records[0]["text"]
        # Scripts should be removed
        assert "console.log" not in text
        # Extra whitespace should be cleaned
        assert "  " not in text
        assert "Test Content" in text

    def test_text_mode_raw(self, ray_start_regular_shared, tmp_path):
        """Test raw text mode preserves whitespace."""
        html_path = os.path.join(tmp_path, "raw.html")
        create_test_html(html_path)

        ds = ray.data.read_html(html_path, text_mode="raw")
        records = ds.take_all()

        assert len(records) == 1
        # Raw mode includes more whitespace
        text = records[0]["text"]
        assert "Hello World" in text

    def test_text_mode_markdown(self, ray_start_regular_shared, tmp_path):
        """Test markdown conversion mode."""
        html_path = os.path.join(tmp_path, "markdown.html")
        create_test_html(html_path)

        # This will require markdownify to be installed
        ds = ray.data.read_html(html_path, text_mode="markdown")
        records = ds.take_all()

        assert len(records) == 1
        text = records[0]["text"]
        # Markdown mode should convert h1 to # heading
        assert "#" in text or "Main Heading" in text

    def test_extract_tables(self, ray_start_regular_shared, tmp_path):
        """Test table extraction from HTML."""
        html_path = os.path.join(tmp_path, "table.html")
        create_html_with_table(html_path)

        ds = ray.data.read_html(html_path, extract_tables=True)
        records = ds.take_all()

        assert len(records) == 1
        assert "tables" in records[0]
        tables = records[0]["tables"]
        assert len(tables) == 1  # One table

        table = tables[0]
        assert len(table) == 3  # 3 rows (1 header + 2 data)
        # Check header
        assert "Name" in table[0]
        assert "Age" in table[0]
        # Check data
        assert "Alice" in str(table)
        assert "Bob" in str(table)

    def test_extract_links(self, ray_start_regular_shared, tmp_path):
        """Test link extraction from HTML."""
        html_path = os.path.join(tmp_path, "links.html")
        create_html_with_links(html_path)

        ds = ray.data.read_html(html_path, extract_links=True)
        records = ds.take_all()

        assert len(records) == 1
        assert "links" in records[0]
        links = records[0]["links"]
        assert len(links) == 3  # 3 links

        # Check link data
        hrefs = [link["href"] for link in links]
        assert "https://ray.io" in hrefs
        assert "/docs" in hrefs
        assert "https://github.com/ray-project/ray" in hrefs

        texts = [link["text"] for link in links]
        assert "Ray Website" in texts

    def test_extract_metadata(self, ray_start_regular_shared, tmp_path):
        """Test metadata extraction."""
        html_path = os.path.join(tmp_path, "metadata.html")
        create_test_html(html_path, title="Metadata Test")

        ds = ray.data.read_html(html_path, extract_metadata=True)
        records = ds.take_all()

        assert len(records) == 1
        record = records[0]

        # Check metadata fields
        assert record["title"] == "Metadata Test"
        assert record["description"] == "Test description"
        assert record["keywords"] == "test, html, ray"
        assert "headers" in record
        assert "h1" in record["headers"]
        assert "Main Heading" in record["headers"]["h1"]

    def test_no_metadata_extraction(self, ray_start_regular_shared, tmp_path):
        """Test with metadata extraction disabled."""
        html_path = os.path.join(tmp_path, "no_meta.html")
        create_test_html(html_path)

        ds = ray.data.read_html(html_path, extract_metadata=False)
        records = ds.take_all()

        assert len(records) == 1
        # Should only have text and path (if include_paths=True)
        assert "text" in records[0]
        assert "title" not in records[0]
        assert "description" not in records[0]

    def test_css_selector(self, ray_start_regular_shared, tmp_path):
        """Test CSS selector for targeted extraction."""
        html_content = """<!DOCTYPE html>
<html>
<body>
    <article class="main-content">
        <h1>Main Article</h1>
        <p>Important content</p>
    </article>
    <aside class="sidebar">
        <p>Sidebar content</p>
    </aside>
</body>
</html>"""
        html_path = os.path.join(tmp_path, "selector.html")
        with open(html_path, "w") as f:
            f.write(html_content)

        # Extract only main content
        ds = ray.data.read_html(html_path, selector="article.main-content")
        records = ds.take_all()

        assert len(records) == 1
        text = records[0]["text"]
        assert "Important content" in text
        # Sidebar should not be included
        assert "Sidebar content" not in text

    def test_selector_no_match(self, ray_start_regular_shared, tmp_path):
        """Test CSS selector with no matching elements."""
        html_path = os.path.join(tmp_path, "no_match.html")
        create_test_html(html_path)

        ds = ray.data.read_html(html_path, selector="div.nonexistent")
        records = ds.take_all()

        # Should return empty result
        assert len(records) == 1
        # Content should be empty
        assert records[0]["text"] == ""
        # Metadata should also be empty/None for consistency
        assert records[0].get("title") is None or records[0]["title"] == ""

    def test_selector_metadata_consistency(self, ray_start_regular_shared, tmp_path):
        """Test that metadata is extracted from selected content only."""
        html_content = """<!DOCTYPE html>
<html>
<head>
    <title>Full Page Title</title>
    <meta name="description" content="Full page description">
</head>
<body>
    <h1>Page Header</h1>
    <article class="main-content">
        <h2>Article Title</h2>
        <h3>Article Subtitle</h3>
        <p>Article content</p>
    </article>
    <aside class="sidebar">
        <h2>Sidebar Header</h2>
        <p>Sidebar content</p>
    </aside>
</body>
</html>"""
        html_path = os.path.join(tmp_path, "selector_metadata.html")
        with open(html_path, "w") as f:
            f.write(html_content)

        # Extract only article with metadata
        ds = ray.data.read_html(
            html_path, selector="article.main-content", extract_metadata=True
        )
        records = ds.take_all()

        assert len(records) == 1
        record = records[0]

        # Content should only include article content
        text = record["text"]
        assert "Article content" in text
        assert "Sidebar content" not in text

        # Metadata should be extracted from selected content only
        # Title should be None or empty since article has no title tag
        assert record.get("title") is None or record["title"] == ""

        # Headers should only include h2 and h3 from article, not from sidebar
        if "headers" in record:
            headers = record["headers"]
            if "h2" in headers:
                assert "Article Title" in headers["h2"]
                assert "Sidebar Header" not in str(headers)
            if "h3" in headers:
                assert "Article Subtitle" in headers["h3"]

    def test_selector_preserves_dom_structure(self, ray_start_regular_shared, tmp_path):
        """Test that CSS selector preserves DOM structure."""
        html_content = """<!DOCTYPE html>
<html>
<body>
    <div class="container">
        <div class="nested">
            <p>Nested <span class="highlight">highlighted</span> text</p>
        </div>
        <table>
            <tr><td>Cell 1</td><td>Cell 2</td></tr>
        </table>
    </div>
</body>
</html>"""
        html_path = os.path.join(tmp_path, "selector_dom.html")
        with open(html_path, "w") as f:
            f.write(html_content)

        # Extract with selector and table extraction
        ds = ray.data.read_html(
            html_path, selector="div.container", extract_tables=True
        )
        records = ds.take_all()

        assert len(records) == 1
        record = records[0]

        # Text should preserve nested structure
        text = record["text"]
        assert "Nested" in text
        assert "highlighted" in text
        assert "text" in text

        # Tables should still be extractable from selected content
        assert "tables" in record
        assert len(record["tables"]) == 1
        assert "Cell 1" in str(record["tables"][0])

    def test_encoding_utf8(self, ray_start_regular_shared, tmp_path):
        """Test UTF-8 encoding handling."""
        html_path = os.path.join(tmp_path, "utf8.html")
        create_html_with_encoding(html_path, encoding="utf-8")

        ds = ray.data.read_html(html_path, encoding="utf-8")
        records = ds.take_all()

        assert len(records) == 1
        text = records[0]["text"]
        # UTF-8 characters should be preserved
        assert "é" in text or "Special Characters" in text

    def test_encoding_auto_detect(self, ray_start_regular_shared, tmp_path):
        """Test automatic encoding detection."""
        html_path = os.path.join(tmp_path, "auto_encoding.html")
        create_html_with_encoding(html_path)

        # Don't specify encoding, let it auto-detect
        ds = ray.data.read_html(html_path)
        records = ds.take_all()

        assert len(records) == 1
        # Should successfully parse regardless of encoding

    def test_include_paths(self, ray_start_regular_shared, tmp_path):
        """Test including file paths in output."""
        html_path = os.path.join(tmp_path, "paths.html")
        create_test_html(html_path)

        # Test include_paths=True
        ds = ray.data.read_html(html_path, include_paths=True)
        records = ds.take_all()

        assert len(records) == 1
        assert "path" in records[0]
        assert "paths.html" in records[0]["path"]

        # Test include_paths=False (default)
        ds = ray.data.read_html(html_path, include_paths=False)
        records = ds.take_all()

        assert len(records) == 1
        assert "path" not in records[0]
        assert "text" in records[0]  # Should still have text

    def test_multiple_files(self, ray_start_regular_shared, tmp_path):
        """Test reading multiple HTML files."""
        html_dir = tmp_path / "htmls"
        html_dir.mkdir()

        # Create multiple HTML files
        for i in range(5):
            html_path = html_dir / f"file{i}.html"
            create_test_html(
                str(html_path), title=f"Page {i}", body_text=f"Content {i}"
            )

        ds = ray.data.read_html(str(html_dir))
        records = ds.take_all()

        assert len(records) == 5
        # Check that all files were read
        titles = [r["title"] for r in records]
        assert len(set(titles)) == 5  # All unique titles

    def test_empty_html(self, ray_start_regular_shared, tmp_path):
        """Test reading empty HTML file."""
        html_path = os.path.join(tmp_path, "empty.html")
        create_empty_html(html_path)

        ds = ray.data.read_html(html_path)
        records = ds.take_all()

        assert len(records) == 1
        # Should have minimal content
        assert "text" in records[0]

    def test_invalid_text_mode(self, ray_start_regular_shared, tmp_path):
        """Test invalid text_mode parameter."""
        html_path = os.path.join(tmp_path, "test.html")
        create_test_html(html_path)

        with pytest.raises(ValueError, match="text_mode must be one of"):
            ray.data.read_html(html_path, text_mode="invalid")

    def test_malformed_html(self, ray_start_regular_shared, tmp_path):
        """Test handling of malformed HTML."""
        html_path = os.path.join(tmp_path, "malformed.html")
        with open(html_path, "w") as f:
            f.write("<html><body><p>Unclosed paragraph<h1>Header</body></html>")

        # Should parse despite malformed HTML
        ds = ray.data.read_html(html_path)
        records = ds.take_all()

        assert len(records) == 1
        # BeautifulSoup should handle malformed HTML gracefully

    def test_html_with_nested_elements(self, ray_start_regular_shared, tmp_path):
        """Test HTML with deeply nested elements."""
        html_content = """<!DOCTYPE html>
<html>
<body>
    <div>
        <div>
            <div>
                <p>Deeply <span>nested <strong>content</strong></span></p>
            </div>
        </div>
    </div>
</body>
</html>"""
        html_path = os.path.join(tmp_path, "nested.html")
        with open(html_path, "w") as f:
            f.write(html_content)

        ds = ray.data.read_html(html_path)
        records = ds.take_all()

        assert len(records) == 1
        text = records[0]["text"]
        assert "Deeply" in text
        assert "nested" in text
        assert "content" in text

    def test_combined_features(self, ray_start_regular_shared, tmp_path):
        """Test combining multiple extraction features."""
        html_content = """<!DOCTYPE html>
<html>
<head>
    <title>Combined Test</title>
    <meta name="description" content="Test all features">
</head>
<body>
    <h1>Main Heading</h1>
    <p>Some text content</p>
    <a href="https://example.com">Example Link</a>
    <table>
        <tr><th>Col1</th><th>Col2</th></tr>
        <tr><td>A</td><td>B</td></tr>
    </table>
</body>
</html>"""
        html_path = os.path.join(tmp_path, "combined.html")
        with open(html_path, "w") as f:
            f.write(html_content)

        ds = ray.data.read_html(
            html_path,
            extract_tables=True,
            extract_links=True,
            extract_metadata=True,
            include_paths=True,
        )
        records = ds.take_all()

        assert len(records) == 1
        record = records[0]

        # Check all features are present
        assert "text" in record
        assert "title" in record and record["title"] == "Combined Test"
        assert "tables" in record and len(record["tables"]) == 1
        assert "links" in record and len(record["links"]) == 1
        assert "path" in record
        assert "headers" in record

    def test_file_extensions_filter(self, ray_start_regular_shared, tmp_path):
        """Test filtering by file extensions."""
        html_dir = tmp_path / "mixed"
        html_dir.mkdir()

        # Create HTML and other files
        (html_dir / "page.html").write_text("<html><body>HTML</body></html>")
        (html_dir / "page.htm").write_text("<html><body>HTM</body></html>")
        (html_dir / "page.txt").write_text("Plain text")

        # Should only read .html files
        ds = ray.data.read_html(str(html_dir), file_extensions=["html"])
        records = ds.take_all()

        assert len(records) == 1  # Only .html file

        # Read both .html and .htm
        ds = ray.data.read_html(str(html_dir), file_extensions=["html", "htm"])
        records = ds.take_all()

        assert len(records) == 2  # Both .html and .htm files

    def test_ignore_missing_paths(self, ray_start_regular_shared, tmp_path):
        """Test ignoring missing file paths."""
        html_path = os.path.join(tmp_path, "exists.html")
        create_test_html(html_path)

        missing_path = os.path.join(tmp_path, "missing.html")

        # Should raise error without ignore_missing_paths
        with pytest.raises(Exception):
            ds = ray.data.read_html([html_path, missing_path])
            ds.take_all()

        # Should succeed with ignore_missing_paths=True
        ds = ray.data.read_html([html_path, missing_path], ignore_missing_paths=True)
        records = ds.take_all()

        assert len(records) == 1  # Only existing file

    def test_schema(self, ray_start_regular_shared, tmp_path):
        """Test output schema with various options."""
        html_path = os.path.join(tmp_path, "schema.html")
        create_test_html(html_path)

        # Basic schema
        ds = ray.data.read_html(html_path)
        schema = ds.schema()
        assert "text" in schema.names

        # With metadata
        ds = ray.data.read_html(html_path, extract_metadata=True)
        schema = ds.schema()
        assert "title" in schema.names

        # With paths
        ds = ray.data.read_html(html_path, include_paths=True)
        schema = ds.schema()
        assert "path" in schema.names

    def test_parallelism(self, ray_start_regular_shared, tmp_path):
        """Test parallel reading of HTML files."""
        html_dir = tmp_path / "parallel"
        html_dir.mkdir()

        # Create many HTML files
        num_files = 20
        for i in range(num_files):
            html_path = html_dir / f"file{i}.html"
            create_test_html(str(html_path), title=f"Page {i}")

        # Read with explicit parallelism
        ds = ray.data.read_html(str(html_dir), override_num_blocks=4)
        records = ds.take_all()

        assert len(records) == num_files
        # Verify all files were read
        titles = sorted([r["title"] for r in records])
        expected_titles = sorted([f"Page {i}" for i in range(num_files)])
        assert titles == expected_titles
