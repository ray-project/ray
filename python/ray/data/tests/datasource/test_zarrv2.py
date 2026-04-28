import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import ray.data.read_api as read_api
from ray.data._internal.datasource import zarrv2_datasource


def _write_zmetadata(store_path: Path, metadata: dict) -> Path:
    store_path.mkdir()
    (store_path / ".zmetadata").write_text(json.dumps({"metadata": metadata}))
    return store_path


def _execute_read_tasks(tasks) -> pd.DataFrame:
    frames = [block for task in tasks for block in task()]
    return pd.concat(frames, ignore_index=True)


@pytest.fixture(autouse=True)
def stub_optional_imports(monkeypatch):
    monkeypatch.setattr(
        zarrv2_datasource,
        "_check_import",
        lambda *args, **kwargs: None,
    )


@pytest.fixture
def zarrv2_store(tmp_path) -> Path:
    return _write_zmetadata(
        tmp_path / "sample.zarr",
        {
            ".zarray": {
                "shape": [5, 4],
                "chunks": [2, 3],
                "dtype": "<i4",
            },
            "nested/.zarray": {
                "shape": [3],
                "chunks": [2],
                "dtype": "|u1",
            },
            ".zgroup": {"zarr_format": 2},
        },
    )


def test_zarrv2_datasource_selects_all_arrays_and_estimates_size(zarrv2_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(str(zarrv2_store))

    assert set(datasource._selected_arrays) == {"", "nested"}
    assert datasource._grid_shape_dict[""]["grid_shape"] == (3, 2)
    assert datasource._grid_shape_dict["nested"]["grid_shape"] == (2,)
    assert datasource.estimate_inmemory_data_size() == 83


def test_zarrv2_datasource_normalizes_requested_array_paths(zarrv2_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(zarrv2_store),
        array_paths=[".", "nested/"],
    )

    assert list(datasource._selected_arrays) == ["", "nested"]


def test_zarrv2_datasource_rejects_missing_array_paths(zarrv2_store):
    with pytest.raises(
        ValueError,
        match=r"Array\(s\) not found: missing\. Available: \., nested",
    ):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_store),
            array_paths=["missing"],
        )


@pytest.mark.parametrize("chunk_shape", [[0, 2], [-1, 2], [1.5, 2]])
def test_zarrv2_datasource_rejects_invalid_chunk_shape(zarrv2_store, chunk_shape):
    with pytest.raises(ValueError, match="chunk shape must only contain positive"):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_store),
            chunk_shape=chunk_shape,
        )


def test_zarrv2_datasource_rejects_chunk_rank_mismatch(zarrv2_store):
    with pytest.raises(ValueError, match="same dimension length as the array"):
        zarrv2_datasource.ZarrV2Datasource(
            str(zarrv2_store),
            chunk_shape=[4, 4],
            array_paths=["nested"],
        )


def test_zarrv2_datasource_applies_chunk_shape_override(zarrv2_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(
        str(zarrv2_store),
        chunk_shape=[4, 2],
        array_paths=["."],
    )

    read_tasks = datasource.get_read_tasks(parallelism=8)
    rows = _execute_read_tasks(read_tasks).to_dict("records")

    assert datasource._grid_shape_dict[""]["grid_shape"] == (2, 2)
    assert len(rows) == 4

    truncated_chunk = next(
        row
        for row in rows
        if row["chunk_slices"] == [(4, 5), (2, 4)]
    )
    assert truncated_chunk["chunk_shape"] == (1, 2)
    assert truncated_chunk["padding"] == [3, 0]


def test_zarrv2_datasource_requires_consolidated_metadata(tmp_path):
    store_path = tmp_path / "broken.zarr"
    store_path.mkdir()
    (store_path / ".zmetadata").write_text(json.dumps({}))

    with pytest.raises(ValueError, match="Missing 'metadata'"):
        zarrv2_datasource.ZarrV2Datasource(str(store_path))


def test_zarrv2_datasource_get_read_tasks_batches_chunks_by_parallelism(zarrv2_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(str(zarrv2_store))

    read_tasks = datasource.get_read_tasks(parallelism=3)

    assert len(read_tasks) == 3
    assert [task.metadata.num_rows for task in read_tasks] == [3, 3, 2]
    assert all(task.metadata.input_files == [str(zarrv2_store)] for task in read_tasks)


def test_zarrv2_datasource_get_read_tasks_returns_chunk_descriptors(zarrv2_store):
    datasource = zarrv2_datasource.ZarrV2Datasource(str(zarrv2_store))

    read_tasks = datasource.get_read_tasks(parallelism=16)
    rows = _execute_read_tasks(read_tasks).to_dict("records")

    assert len(read_tasks) == 8
    assert all(task.metadata.num_rows == 1 for task in read_tasks)

    first_root_chunk = next(
        row
        for row in rows
        if row["array"] == "" and row["chunk_slices"] == [(0, 2), (0, 3)]
    )
    assert first_root_chunk["array_shape"] == [5, 4]
    assert first_root_chunk["chunk_shape"] == (2, 3)
    assert first_root_chunk["dtype"] == "<i4"
    assert first_root_chunk["padding"] == [0, 0]

    truncated_root_chunk = next(
        row
        for row in rows
        if row["array"] == "" and row["chunk_slices"] == [(4, 5), (3, 4)]
    )
    assert truncated_root_chunk["chunk_shape"] == (1, 1)
    assert truncated_root_chunk["padding"] == [1, 2]

    truncated_nested_chunk = next(
        row
        for row in rows
        if row["array"] == "nested" and row["chunk_slices"] == [(2, 3)]
    )
    assert truncated_nested_chunk["chunk_shape"] == (1,)
    assert truncated_nested_chunk["dtype"] == "|u1"
    assert truncated_nested_chunk["padding"] == [1]


def test_read_zarrv2_builds_datasource_and_delegates_to_read_datasource():
    datasource = object()
    dataset = object()

    with patch(
        "ray.data.read_api.ZarrV2Datasource",
        return_value=datasource,
    ) as mock_datasource:
        with patch(
            "ray.data.read_api.read_datasource",
            return_value=dataset,
        ) as mock_read_datasource:
            result = read_api.read_zarrv2(
                "/tmp/sample.zarr",
                chunk_shape=[4, 2],
                array_paths=["nested"],
                concurrency=3,
                override_num_blocks=2,
                num_cpus=0.5,
                num_gpus=1,
                memory=1024,
                ray_remote_args={"resources": {"custom": 1}},
            )

    assert result is dataset
    mock_datasource.assert_called_once_with(
        path="/tmp/sample.zarr",
        chunk_shape=[4, 2],
        array_paths=["nested"],
    )
    mock_read_datasource.assert_called_once()
    args, kwargs = mock_read_datasource.call_args
    assert args == (datasource,)
    assert kwargs == {
        "ray_remote_args": {"resources": {"custom": 1}},
        "num_cpus": 0.5,
        "num_gpus": 1,
        "memory": 1024,
        "concurrency": 3,
        "override_num_blocks": 2,
    }


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
