"""Tests for XML datasource functionality."""

import os
import shutil

import pandas as pd
import pytest

import ray
from ray.data.block import BlockAccessor
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def df_to_xml(dataframe, path):
    """Helper function to write a pandas DataFrame to XML file.

    Creates simple XML with repeating <record> elements.
    """
    with open(path, "w") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write("<root>\n")
        for _, row in dataframe.iterrows():
            f.write("    <record>\n")
            for col in dataframe.columns:
                value = row[col]
                f.write(f"        <{col}>{value}</{col}>\n")
            f.write("    </record>\n")
        f.write("</root>\n")


def test_xml_read(ray_start_regular_shared, tmp_path):
    """Test reading XML files in various configurations."""
    # Single file.
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(tmp_path, "test1.xml")
    df_to_xml(df1, path1)
    ds = ray.data.read_xml(path1, partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert len(dsdf) == 3
    # Test metadata ops.
    assert ds.count() == 3
    assert ds.input_files() == [_unwrap_protocol(path1)]

    # Two files, override_num_blocks=2.
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "test2.xml")
    df_to_xml(df2, path2)
    ds = ray.data.read_xml([path1, path2], override_num_blocks=2, partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert len(dsdf) == 6
    # Test metadata ops.
    for block, meta in ds._plan.execute().blocks:
        assert BlockAccessor.for_block(ray.get(block)).size_bytes() == meta.size_bytes

    # Three files, override_num_blocks=2.
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    path3 = os.path.join(tmp_path, "test3.xml")
    df_to_xml(df3, path3)
    ds = ray.data.read_xml(
        [path1, path2, path3],
        override_num_blocks=2,
        partitioning=None,
    )
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert len(dsdf) == 9

    # Directory, two files.
    path = os.path.join(tmp_path, "test_xml_dir")
    os.mkdir(path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(path, "data0.xml")
    df_to_xml(df1, path1)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(path, "data1.xml")
    df_to_xml(df2, path2)
    ds = ray.data.read_xml(path, partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert len(dsdf) == 6
    shutil.rmtree(path)

    # Two directories, three files.
    path1 = os.path.join(tmp_path, "test_xml_dir1")
    path2 = os.path.join(tmp_path, "test_xml_dir2")
    os.mkdir(path1)
    os.mkdir(path2)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    file_path1 = os.path.join(path1, "data0.xml")
    df_to_xml(df1, file_path1)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    file_path2 = os.path.join(path2, "data1.xml")
    df_to_xml(df2, file_path2)
    df3 = pd.DataFrame({"one": [7, 8, 9], "two": ["h", "i", "j"]})
    file_path3 = os.path.join(path2, "data2.xml")
    df_to_xml(df3, file_path3)
    ds = ray.data.read_xml([path1, path2], partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert len(dsdf) == 9
    shutil.rmtree(path1)
    shutil.rmtree(path2)

    # Directory and file, two files.
    dir_path = os.path.join(tmp_path, "test_xml_dir")
    os.mkdir(dir_path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(dir_path, "data0.xml")
    df_to_xml(df1, path1)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(tmp_path, "data1.xml")
    df_to_xml(df2, path2)
    ds = ray.data.read_xml([dir_path, path2], partitioning=None)
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert len(dsdf) == 6
    shutil.rmtree(dir_path)

    # Directory, two files and non-xml file (test extension-based path filtering).
    path = os.path.join(tmp_path, "test_xml_dir")
    os.mkdir(path)
    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    path1 = os.path.join(path, "data0.xml")
    df_to_xml(df1, path1)
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    path2 = os.path.join(path, "data1.xml")
    df_to_xml(df2, path2)

    # Add a file with a non-matching file extension. This file should be ignored.
    df_txt = pd.DataFrame({"foobar": [1, 2, 3]})
    df_txt.to_json(os.path.join(path, "foo.txt"))

    ds = ray.data.read_xml(
        path,
        file_extensions=["xml"],
        partitioning=None,
    )
    dsdf = ds.to_pandas().sort_values(by=["one", "two"]).reset_index(drop=True)
    assert len(dsdf) == 6
    shutil.rmtree(path)


def test_xml_read_with_attributes(ray_start_regular_shared, tmp_path):
    """Test reading XML with attributes."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <person id="1" status="active">
        <name>Alice</name>
    </person>
    <person id="2" status="inactive">
        <name>Bob</name>
    </person>
</root>"""

    xml_path = os.path.join(tmp_path, "attributes.xml")
    with open(xml_path, "w") as f:
        f.write(xml_content)

    ds = ray.data.read_xml(xml_path, partitioning=None)
    result = ds.to_pandas()

    assert len(result) == 2
    # Attributes are prefixed with '@'
    assert "@id" in result.columns
    assert "@status" in result.columns
    assert "name" in result.columns


def test_xml_read_with_xpath(ray_start_regular_shared, tmp_path):
    """Test reading XML with XPath selector."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <section>
        <item><value>1</value></item>
        <item><value>2</value></item>
    </section>
    <other>
        <data><value>3</value></data>
    </other>
</root>"""

    xml_path = os.path.join(tmp_path, "xpath_test.xml")
    with open(xml_path, "w") as f:
        f.write(xml_content)

    # Read only item elements using XPath
    ds = ray.data.read_xml(xml_path, xpath=".//item", partitioning=None)
    result = ds.to_pandas()

    # Should have 2 items
    assert len(result) == 2


def test_xml_read_empty_file(ray_start_regular_shared, tmp_path):
    """Test reading an XML file with no data elements."""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<root>
</root>"""

    xml_path = os.path.join(tmp_path, "empty.xml")
    with open(xml_path, "w") as f:
        f.write(xml_content)

    ds = ray.data.read_xml(xml_path, partitioning=None)
    result = ds.to_pandas()

    # Should return empty dataset
    assert len(result) == 0


def test_xml_read_malformed_raises_error(ray_start_regular_shared, tmp_path):
    """Test that malformed XML raises appropriate error."""
    # Create malformed XML (missing closing tag)
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<root>
    <record>
        <id>1</id>
"""

    xml_path = os.path.join(tmp_path, "malformed.xml")
    with open(xml_path, "w") as f:
        f.write(xml_content)

    # Should raise ValueError for malformed XML
    with pytest.raises(ValueError, match="Failed to read XML file"):
        ds = ray.data.read_xml(xml_path, partitioning=None)
        ds.take_all()


def test_xml_include_paths(ray_start_regular_shared, tmp_path):
    """Test including file paths in the output."""
    df = pd.DataFrame({"one": [1, 2], "two": ["a", "b"]})
    xml_path = os.path.join(tmp_path, "with_path.xml")
    df_to_xml(df, xml_path)

    ds = ray.data.read_xml(xml_path, include_paths=True, partitioning=None)
    result = ds.to_pandas()

    assert "path" in result.columns
    assert len(result) == 2


def test_xml_ignore_missing_paths(ray_start_regular_shared, tmp_path):
    """Test ignoring missing paths."""
    df = pd.DataFrame({"one": [1, 2], "two": ["a", "b"]})
    xml_path = os.path.join(tmp_path, "exists.xml")
    df_to_xml(df, xml_path)

    missing_path = os.path.join(tmp_path, "missing.xml")

    # Should not raise error with ignore_missing_paths=True
    ds = ray.data.read_xml(
        [xml_path, missing_path],
        ignore_missing_paths=True,
        partitioning=None,
    )
    result = ds.to_pandas()

    assert len(result) == 2
