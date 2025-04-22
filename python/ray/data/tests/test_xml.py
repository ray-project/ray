import os
import pytest
import ray

# Example single-record and multi-record XML
SIMPLE_XML = """<root>
  <user id="1" active="true">
    <name>John</name>
    <email>john@example.com</email>
    <info>
      <age>35</age>
      <city>NY</city>
    </info>
  </user>
</root>"""

MULTI_XML = """<root>
  <user id="1" active="true">
    <name>John</name>
    <email>john@example.com</email>
    <info>
      <age>35</age>
      <city>NY</city>
    </info>
  </user>
  <user id="2">
    <name>Jane</name>
    <info>
      <age>25</age>
      <city>LA</city>
    </info>
  </user>
</root>"""

EMPTY_XML = "<root></root>"


def write_xml(tmp_path, fname, content):
    path = os.path.join(tmp_path, fname)
    with open(path, "w") as f:
        f.write(content)
    return path


def test_read_xml_simple(tmp_path):
    path = write_xml(tmp_path, "simple.xml", SIMPLE_XML)
    ds = ray.data.read_xml(paths=path, record_tag="user")
    rows = ds.take_all()
    assert len(rows) == 1
    row = rows[0]
    assert row["@id"] == "1"
    assert row["name"] == "John"
    assert row["info.age"] == "35"
    assert row["info.city"] == "NY"
    assert row["@active"] == "true"
    assert row["email"] == "john@example.com"


def test_read_xml_multiple(tmp_path):
    path = write_xml(tmp_path, "multi.xml", MULTI_XML)
    ds = ray.data.read_xml(paths=path, record_tag="user")
    rows = ds.take_all()
    assert len(rows) == 2
    john, jane = rows
    assert john["@id"] == "1"
    assert jane["name"] == "Jane"
    assert jane["info.city"] == "LA"
    assert "email" not in jane


def test_empty_xml(tmp_path):
    path = write_xml(tmp_path, "empty.xml", EMPTY_XML)
    ds = ray.data.read_xml(paths=path, record_tag="user")
    assert ds.count() == 0


def test_read_xml_many_files(tmp_path):
    path1 = write_xml(tmp_path, "multi1.xml", MULTI_XML)
    path2 = write_xml(tmp_path, "multi2.xml", MULTI_XML)
    ds = ray.data.read_xml(paths=[path1, path2], record_tag="user")
    rows = ds.take_all()
    assert len(rows) == 4
    # Ensure all user ids present (unordered)
    ids = sorted([int(r["@id"]) for r in rows if "@id" in r])
    assert ids == [1, 1, 2, 2]


def test_read_xml_empty_files(tmp_path):
    path1 = write_xml(tmp_path, "e1.xml", EMPTY_XML)
    path2 = write_xml(tmp_path, "e2.xml", EMPTY_XML)
    ds = ray.data.read_xml(paths=[path1, path2], record_tag="user")
    assert ds.count() == 0


@pytest.mark.parametrize("ignore_missing_paths", [True, False])
def test_read_xml_ignore_missing_paths(tmp_path, ignore_missing_paths):
    path = write_xml(tmp_path, "multi.xml", MULTI_XML)
    paths = [path, "missing.xml"]
    if ignore_missing_paths:
        ds = ray.data.read_xml(
            paths=paths, ignore_missing_paths=True, record_tag="user"
        )
        assert ds.count() == 2
    else:
        with pytest.raises(FileNotFoundError):
            ray.data.read_xml(
                paths=paths, ignore_missing_paths=False, record_tag="user"
            ).materialize()


def test_read_xml_schema(tmp_path):
    path = write_xml(tmp_path, "multi.xml", MULTI_XML)
    ds = ray.data.read_xml(paths=path, record_tag="user")
    schema = ds.schema()
    field_names = set(schema.names)
    # Check expected fields present
    assert "name" in field_names
    assert "info.age" in field_names
    assert "info.city" in field_names
    assert "@id" in field_names


def test_read_xml_row_missing_fields(tmp_path):
    # Jane is missing @active, email field.
    path = write_xml(tmp_path, "multi.xml", MULTI_XML)
    ds = ray.data.read_xml(paths=path, record_tag="user")
    jane = ds.take_all()[1]
    assert "email" not in jane
    assert "@active" not in jane


def test_read_xml_partitioning(tmp_path):
    # Hive-style directories: country=US/multi.xml, etc.
    subdir = os.path.join(tmp_path, "country=US")
    os.mkdir(subdir)
    xml = MULTI_XML
    path = os.path.join(subdir, "file.xml")
    with open(path, "w") as f:
        f.write(xml)
    ds = ray.data.read_xml(paths=tmp_path, partitioning="hive", record_tag="user")
    pdf = ds.to_pandas()
    assert "country" in pdf.columns
    assert all(pdf["country"] == "US")


def test_read_xml_meta_provider(tmp_path):
    from ray.data.datasource import FastFileMetadataProvider, BaseFileMetadataProvider

    path = write_xml(tmp_path, "simple.xml", SIMPLE_XML)
    ds = ray.data.read_xml(
        paths=path, meta_provider=FastFileMetadataProvider(), record_tag="user"
    )
    rows = ds.take_all()
    assert rows[0]["name"] == "John"
    with pytest.raises(NotImplementedError):
        ray.data.read_xml(
            paths=path, meta_provider=BaseFileMetadataProvider(), record_tag="user"
        )


def test_read_xml_remote_args(ray_start_cluster, tmp_path):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)
    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    bar_node_id = ray.get(get_node_id.options(resources={"bar": 1}).remote())

    path = write_xml(tmp_path, "foo.xml", MULTI_XML)
    ds = ray.data.read_xml(
        paths=path,
        override_num_blocks=2,
        ray_remote_args={"resources": {"bar": 1}},
        record_tag="user",
    )
    block_refs = list(ds.iter_torch_batches())
    # This does not actually check placement but exercises the API.


def test_read_xml_large(tmp_path):
    """Test with a large XML file."""
    n = 500
    content = (
        "<root>"
        + "".join(
            [
                f'<user id="{i}"><name>User{i}</name><info><age>{20+i}</age></info></user>'
                for i in range(n)
            ]
        )
        + "</root>"
    )
    path = write_xml(tmp_path, "large.xml", content)
    ds = ray.data.read_xml(paths=path, record_tag="user")
    assert ds.count() == n
    df = ds.to_pandas()
    assert df.shape[0] == n
    assert all(df["name"].str.startswith("User"))


# Test missing record_tag
def test_record_tag_none(tmp_path):
    xml = "<root><person><foo>x</foo></person></root>"
    path = write_xml(tmp_path, "x.xml", xml)
    ds = ray.data.read_xml(paths=path)
    rows = ds.take_all()
    assert len(rows) == 1
    assert rows[0]["foo"] == "x"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
