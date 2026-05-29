import sys
import urllib.parse
from unittest.mock import MagicMock, patch

import pytest

from ray._private.external_storage import (
    ExternalStorageHDFSImpl,
    create_url_with_offset,
    reset_external_storage,
    setup_external_storage,
)
from ray._private.ray_constants import DEFAULT_OBJECT_PREFIX

HDFS_NAMESERVICE = "nameservice"
HDFS_URI = f"hdfs://{HDFS_NAMESERVICE}/user/ray/spill"

HDFS_OBJECT_SPILLING_CONFIG = {
    "type": "hdfs",
    "params": {"uri": HDFS_URI},
}


@pytest.fixture(autouse=True)
def mock_pyarrow_fs():
    mock_pa_fs = MagicMock()
    mock_pyarrow = MagicMock()
    mock_pyarrow.fs = mock_pa_fs
    with patch.dict(sys.modules, {"pyarrow": mock_pyarrow, "pyarrow.fs": mock_pa_fs}):
        yield mock_pa_fs


def _make_storage(node_id="test-node-id", uri=None, **kwargs):
    if uri is None:
        uri = HDFS_OBJECT_SPILLING_CONFIG["params"]["uri"]
    return ExternalStorageHDFSImpl(node_id, uri=uri, **kwargs)


def _init_storage_with_mock_fs(storage):
    mock_fs = MagicMock()
    with patch.object(storage._pa_fs, "HadoopFileSystem", return_value=mock_fs):
        storage.ensure_initialized()
    return mock_fs


def test_setup_external_storage_hdfs():
    storage = setup_external_storage(
        HDFS_OBJECT_SPILLING_CONFIG, "node-id", "session-name"
    )
    assert isinstance(storage, ExternalStorageHDFSImpl)
    reset_external_storage()


def test_hdfs_lazy_init():
    storage = _make_storage()
    assert storage._fs is None


def test_hdfs_invalid_uri():
    with pytest.raises(ValueError, match="HDFS URI must start with 'hdfs://'"):
        _make_storage(uri="s3://bucket/path")


def test_hdfs_invalid_buffer_size():
    with pytest.raises(ValueError, match="buffer_size must be a positive integer"):
        _make_storage(buffer_size=0)


def test_hdfs_ensure_fs_creates_client_and_spill_dirs():
    node_id = "node-abc"
    storage = _make_storage(node_id=node_id)
    mock_fs = MagicMock()

    with patch.object(storage._pa_fs, "HadoopFileSystem", return_value=mock_fs) as ctor:
        storage.ensure_initialized()

    ctor.assert_called_once_with(
        host=HDFS_NAMESERVICE,
        port=0,
        user=None,
        kerb_ticket=None,
        extra_conf=None,
    )
    expected_spill_dir = f"/user/ray/spill/{DEFAULT_OBJECT_PREFIX}_{node_id}"
    mock_fs.create_dir.assert_called_once_with(expected_spill_dir, recursive=True)
    assert storage._fs is mock_fs


def test_hdfs_create_spill_dir_failure():
    storage = _make_storage()
    mock_fs = MagicMock()
    mock_fs.create_dir.side_effect = OSError("permission denied")

    with patch.object(storage._pa_fs, "HadoopFileSystem", return_value=mock_fs):
        with pytest.raises(ValueError, match="Failed to create HDFS spill directory"):
            storage.ensure_initialized()


def test_hdfs_node_id_in_spill_dirs():
    node_id = "unique-node-id"
    storage = _make_storage(
        node_id=node_id,
        uri=[
            f"hdfs://{HDFS_NAMESERVICE}/user/ray/spill-a",
            f"hdfs://{HDFS_NAMESERVICE}/user/ray/spill-b",
        ],
    )
    node_prefix = f"{DEFAULT_OBJECT_PREFIX}_{node_id}"
    assert storage._spill_dirs == [
        f"/user/ray/spill-a/{node_prefix}",
        f"/user/ray/spill-b/{node_prefix}",
    ]


def test_hdfs_spill_objects_writes_to_hdfs():
    storage = _make_storage()
    mock_fs = _init_storage_with_mock_fs(storage)
    mock_file = MagicMock()
    mock_file.write.side_effect = lambda data: len(data)
    mock_fs.open_output_stream.return_value.__enter__.return_value = mock_file

    object_refs = [MagicMock(), MagicMock()]
    owner_addresses = [b"owner-a", b"owner-b"]
    with patch.object(
        storage, "_get_objects_from_store"
    ) as mock_get_objects_from_store:
        mock_get_objects_from_store.return_value = [
            (b"buf-a", b"meta-a", None),
            (b"buf-b", b"meta-b", None),
        ]
        urls = storage.spill_objects(object_refs, owner_addresses)

    assert len(urls) == 2
    mock_fs.open_output_stream.assert_called_once()
    write_path = mock_fs.open_output_stream.call_args[0][0]
    assert write_path.startswith(
        f"/user/ray/spill/{DEFAULT_OBJECT_PREFIX}_test-node-id/"
    )
    assert urls[0].decode().startswith(f"hdfs://{HDFS_NAMESERVICE}/user/ray/spill/")


def test_hdfs_spill_objects_round_robin():
    storage = _make_storage(
        uri=[
            f"hdfs://{HDFS_NAMESERVICE}/user/ray/spill-a",
            f"hdfs://{HDFS_NAMESERVICE}/user/ray/spill-b",
        ],
    )
    mock_fs = _init_storage_with_mock_fs(storage)
    mock_file = MagicMock()
    mock_file.write.side_effect = lambda data: len(data)
    mock_fs.open_output_stream.return_value.__enter__.return_value = mock_file

    object_refs = [MagicMock()]
    with patch.object(
        storage, "_get_objects_from_store", return_value=[(b"buf", b"meta", None)]
    ):
        first_url = storage.spill_objects(object_refs, [b"owner"])[0].decode()
        second_url = storage.spill_objects(object_refs, [b"owner"])[0].decode()

    assert "/user/ray/spill-a/" in first_url or "/user/ray/spill-b/" in first_url
    assert first_url != second_url


def test_hdfs_restore_spilled_objects():
    storage = _make_storage()
    mock_fs = _init_storage_with_mock_fs(storage)

    owner_address = b"owner-address"
    metadata = b"metadata"
    payload = b"0123456789"
    address_len = len(owner_address)
    metadata_len = len(metadata)
    buf_len = len(payload)
    mock_file = MagicMock()
    mock_file.read.side_effect = [
        address_len.to_bytes(8, byteorder="little"),
        metadata_len.to_bytes(8, byteorder="little"),
        buf_len.to_bytes(8, byteorder="little"),
        owner_address,
        metadata,
    ]
    mock_fs.open_input_file.return_value.__enter__.return_value = mock_file

    base_url = (
        f"hdfs://{HDFS_NAMESERVICE}/user/ray/spill/"
        f"{DEFAULT_OBJECT_PREFIX}_test-node-id/spill-file"
    )
    url_with_offset = create_url_with_offset(
        url=base_url,
        offset=0,
        size=storage.HEADER_LENGTH + address_len + metadata_len + buf_len,
    )
    object_ref = MagicMock()

    with patch.object(storage, "_put_object_to_store") as mock_put:
        restored = storage.restore_spilled_objects(
            [object_ref], [url_with_offset.encode()]
        )

    read_path = mock_fs.open_input_file.call_args[0][0]
    assert read_path == urllib.parse.urlparse(base_url).path
    mock_put.assert_called_once_with(
        metadata, buf_len, mock_file, object_ref, owner_address
    )
    assert restored == buf_len


def test_hdfs_restore_spilled_objects_accepts_str_url():
    storage = _make_storage()
    mock_fs = _init_storage_with_mock_fs(storage)

    owner_address = b"owner-address"
    metadata = b"metadata"
    payload = b"x"
    address_len = len(owner_address)
    metadata_len = len(metadata)
    buf_len = len(payload)
    mock_file = MagicMock()
    mock_file.read.side_effect = [
        address_len.to_bytes(8, byteorder="little"),
        metadata_len.to_bytes(8, byteorder="little"),
        buf_len.to_bytes(8, byteorder="little"),
        owner_address,
        metadata,
    ]
    mock_fs.open_input_file.return_value.__enter__.return_value = mock_file

    base_url = (
        f"hdfs://{HDFS_NAMESERVICE}/user/ray/spill/"
        f"{DEFAULT_OBJECT_PREFIX}_test-node-id/spill-file"
    )
    url_with_offset = create_url_with_offset(
        url=base_url,
        offset=0,
        size=storage.HEADER_LENGTH + address_len + metadata_len + buf_len,
    )
    object_ref = MagicMock()

    with patch.object(storage, "_put_object_to_store"):
        restored = storage.restore_spilled_objects([object_ref], [url_with_offset])

    assert restored == buf_len


def test_hdfs_delete_spilled_objects():
    storage = _make_storage()
    mock_fs = _init_storage_with_mock_fs(storage)

    base_url = (
        f"hdfs://{HDFS_NAMESERVICE}/user/ray/spill/"
        f"{DEFAULT_OBJECT_PREFIX}_test-node-id/spill-file"
    )
    url_with_offset = create_url_with_offset(url=base_url, offset=0, size=10)
    storage.delete_spilled_objects([url_with_offset.encode()])

    mock_fs.delete_file.assert_called_once_with(urllib.parse.urlparse(base_url).path)

    mock_fs.reset_mock()
    storage.delete_spilled_objects([url_with_offset])
    mock_fs.delete_file.assert_called_once_with(urllib.parse.urlparse(base_url).path)


def test_hdfs_destroy_external_storage():
    storage = _make_storage()
    mock_fs = _init_storage_with_mock_fs(storage)

    storage.destroy_external_storage()

    expected_spill_dir = f"/user/ray/spill/{DEFAULT_OBJECT_PREFIX}_test-node-id"
    mock_fs.delete_dir.assert_called_once_with(expected_spill_dir)
