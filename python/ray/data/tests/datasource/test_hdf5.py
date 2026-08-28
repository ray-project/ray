from unittest.mock import patch
from zipfile import ZIP_DEFLATED, ZipFile

import numpy as np
import pytest

import ray
from ray.data.block import BlockAccessor
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_read_hdf5_numeric_dataset(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "data.h5"
    expected = np.arange(12).reshape(4, 3)
    with h5py.File(path, "w") as file:
        file["observations"] = expected

    rows = ray.data.read_hdf5(path, dataset="observations").take_all()

    assert len(rows) == 4
    np.testing.assert_array_equal(np.stack([row["data"] for row in rows]), expected)


def test_hdf5_datasource_splits_single_file(ray_start_regular_shared, tmp_path):
    import h5py

    from ray.data._internal.datasource.hdf5_datasource import HDF5Datasource

    path = tmp_path / "data.h5"
    with h5py.File(path, "w") as file:
        file["observations"] = np.arange(40).reshape(10, 4)

    datasource = HDF5Datasource(path, dataset="observations")

    tasks = datasource.get_read_tasks(4)
    blocks = [block for task in tasks for block in task()]

    assert [BlockAccessor.for_block(block).num_rows() for block in blocks] == [
        3,
        3,
        2,
        2,
    ]
    actual = np.concatenate(
        [
            BlockAccessor.for_block(block).to_pandas()["data"].to_numpy()
            for block in blocks
        ]
    )
    np.testing.assert_array_equal(np.stack(actual), np.arange(40).reshape(10, 4))


def test_read_hdf5_requires_nonempty_dataset(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "data.h5"
    with h5py.File(path, "w") as file:
        file["observations"] = np.arange(4)

    with pytest.raises(ValueError, match="dataset must be a non-empty string"):
        ray.data.read_hdf5(path, dataset="/")


def test_read_hdf5_missing_dataset_fails_during_planning(
    ray_start_regular_shared, tmp_path
):
    import h5py

    path = tmp_path / "data.h5"
    with h5py.File(path, "w") as file:
        file["observations"] = np.arange(4)

    with pytest.raises(ValueError, match="Dataset 'missing'.*not found.*data.h5"):
        ray.data.read_hdf5(path, dataset="missing")


def test_read_hdf5_rejects_group(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "data.h5"
    with h5py.File(path, "w") as file:
        file.create_group("observations")

    with pytest.raises(ValueError, match="'observations'.*group, not a dataset"):
        ray.data.read_hdf5(path, dataset="observations")


def test_read_hdf5_rejects_inconsistent_row_shapes(ray_start_regular_shared, tmp_path):
    import h5py

    paths = [tmp_path / "a.h5", tmp_path / "b.h5"]
    for path, shape in zip(paths, [(2, 3), (2, 4)]):
        with h5py.File(path, "w") as file:
            file["observations"] = np.zeros(shape)

    with pytest.raises(ValueError, match="same per-row shape.*a.h5.*b.h5"):
        ray.data.read_hdf5(paths, dataset="observations")


def test_read_hdf5_rejects_inconsistent_dtypes(ray_start_regular_shared, tmp_path):
    import h5py

    paths = [tmp_path / "a.h5", tmp_path / "b.h5"]
    for path, dtype in zip(paths, [np.int32, np.float32]):
        with h5py.File(path, "w") as file:
            file.create_dataset("observations", (2, 3), dtype=dtype)

    with pytest.raises(ValueError, match="same dtype.*a.h5.*int32.*b.h5.*float32"):
        ray.data.read_hdf5(paths, dataset="observations")


def test_read_hdf5_rejects_inconsistent_vlen_dtypes(ray_start_regular_shared, tmp_path):
    import h5py

    paths = [tmp_path / "a.h5", tmp_path / "b.h5"]
    for path, dtype in zip(paths, [np.int32, np.float64]):
        with h5py.File(path, "w") as file:
            values = file.create_dataset(
                "observations", (1,), dtype=h5py.vlen_dtype(dtype)
            )
            values[0] = np.asarray([1, 2], dtype=dtype)

    with pytest.raises(ValueError, match="same dtype.*vlen:int32.*vlen:float64"):
        ray.data.read_hdf5(paths, dataset="observations")


def test_read_hdf5_scalar_vlen_dataset(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "scalar-vlen.h5"
    expected = np.array([1, 2, 3], dtype=np.int32)
    with h5py.File(path, "w") as file:
        values = file.create_dataset(
            "observations", (), dtype=h5py.vlen_dtype(np.int32)
        )
        values[()] = expected

    rows = ray.data.read_hdf5(path, dataset="observations").take_all()

    assert rows == [{"data": [1, 2, 3]}]


def test_read_hdf5_rejects_non_native_vlen_dtype(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "non-native-vlen.h5"
    non_native_dtype = np.dtype(">i4" if np.little_endian else "<i4")
    with h5py.File(path, "w") as file:
        values = file.create_dataset(
            "observations", (1,), dtype=h5py.vlen_dtype(non_native_dtype)
        )
        values[0] = np.asarray([1, 2], dtype=np.int32)

    with pytest.raises(
        ValueError, match="variable-length.*non-native byte order.*not supported"
    ):
        ray.data.read_hdf5(path, dataset="observations")


def test_read_hdf5_rejects_null_dataspace(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "null.h5"
    with h5py.File(path, "w") as file:
        file.create_dataset("observations", data=h5py.Empty(np.float32))

    with pytest.raises(ValueError, match="null dataspace.*not supported.*null.h5"):
        ray.data.read_hdf5(path, dataset="observations")


def test_read_hdf5_empty_dataset(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "empty.h5"
    with h5py.File(path, "w") as file:
        file.create_dataset("observations", (0, 3), dtype=np.float32)

    dataset = ray.data.read_hdf5(path, dataset="observations")

    assert dataset.count() == 0


def test_read_hdf5_zero_width_rows(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "zero-width.h5"
    with h5py.File(path, "w") as file:
        file.create_dataset("observations", shape=(5, 0), dtype=np.int32, chunks=True)

    rows = ray.data.read_hdf5(path, dataset="observations").take_all()

    assert len(rows) == 5
    assert all(row["data"].shape == (0,) for row in rows)


def test_read_hdf5_converts_non_native_byte_order(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "big-endian.h5"
    expected = np.arange(6, dtype=np.int32).reshape(3, 2)
    with h5py.File(path, "w") as file:
        file.create_dataset("observations", data=expected.astype(">i4"))

    rows = ray.data.read_hdf5(path, dataset="observations").take_all()

    np.testing.assert_array_equal(np.stack([row["data"] for row in rows]), expected)


def test_read_hdf5_rejects_compound_dtype(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "compound.h5"
    with h5py.File(path, "w") as file:
        file.create_dataset(
            "observations", (2,), dtype=np.dtype([("x", np.int32), ("y", np.float32)])
        )

    with pytest.raises(
        ValueError, match="observations.*compound.h5.*compound dtype.*not supported"
    ):
        ray.data.read_hdf5(path, dataset="observations")


def test_read_hdf5_rejects_vlen_compound_dtype(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "vlen-compound.h5"
    compound_dtype = np.dtype([("x", np.int32), ("y", np.float32)])
    with h5py.File(path, "w") as file:
        values = file.create_dataset(
            "observations", (1,), dtype=h5py.vlen_dtype(compound_dtype)
        )
        values[0] = np.asarray([(1, 2.0)], dtype=compound_dtype)

    with pytest.raises(
        ValueError,
        match="observations.*vlen-compound.h5.*compound dtype.*not supported",
    ):
        ray.data.read_hdf5(path, dataset="observations")


def test_read_hdf5_rejects_vlen_subarray_dtype(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "vlen-subarray.h5"
    subarray_dtype = np.dtype((np.int32, (2,)))
    with h5py.File(path, "w") as file:
        file.create_dataset("observations", (1,), dtype=h5py.vlen_dtype(subarray_dtype))

    with pytest.raises(
        ValueError,
        match="observations.*vlen-subarray.h5.*subarray dtype.*not supported",
    ):
        ray.data.read_hdf5(path, dataset="observations")


@pytest.mark.parametrize("reference_dtype", ["object", "region"])
def test_read_hdf5_rejects_reference_dtype(
    ray_start_regular_shared, tmp_path, reference_dtype
):
    import h5py

    path = tmp_path / f"{reference_dtype}-reference.h5"
    with h5py.File(path, "w") as file:
        target = file.create_dataset("target", data=np.arange(4))
        dtype = h5py.ref_dtype if reference_dtype == "object" else h5py.regionref_dtype
        references = file.create_dataset("references", (1,), dtype=dtype)
        references[0] = (
            target.ref if reference_dtype == "object" else target.regionref[:]
        )

    with pytest.raises(
        ValueError, match="references.*reference.h5.*reference dtype.*not supported"
    ):
        ray.data.read_hdf5(path, dataset="references")


def test_read_hdf5_rejects_external_storage(ray_start_regular_shared, tmp_path):
    import h5py

    raw_path = tmp_path / "sidecar.bin"
    raw_path.write_bytes(b"private!")
    path = tmp_path / "external.h5"
    with h5py.File(path, "w") as file:
        file.create_dataset(
            "observations",
            (8,),
            dtype=np.uint8,
            external=[(str(raw_path), 0, 8)],
        )

    with pytest.raises(
        ValueError, match="observations.*external.h5.*external storage.*not supported"
    ):
        ray.data.read_hdf5(path, dataset="observations")


def test_read_hdf5_rejects_external_link(ray_start_regular_shared, tmp_path):
    import h5py

    target_path = tmp_path / "target.h5"
    with h5py.File(target_path, "w") as file:
        file["observations"] = np.arange(4)

    path = tmp_path / "external-link.h5"
    with h5py.File(path, "w") as file:
        file["observations"] = h5py.ExternalLink(str(target_path), "/observations")

    with pytest.raises(
        ValueError, match="observations.*external-link.h5.*external link.*not supported"
    ):
        ray.data.read_hdf5(path, dataset="observations")


def test_read_hdf5_rejects_virtual_dataset_with_external_source(
    ray_start_regular_shared, tmp_path
):
    import h5py

    raw_path = tmp_path / "sidecar.bin"
    raw_path.write_bytes(b"private!")
    path = tmp_path / "virtual.h5"
    with h5py.File(path, "w", libver="latest") as file:
        file.create_dataset(
            "raw", (8,), dtype=np.uint8, external=[(str(raw_path), 0, 8)]
        )
        layout = h5py.VirtualLayout(shape=(8,), dtype=np.uint8)
        layout[:] = h5py.VirtualSource(".", "raw", shape=(8,))
        file.create_virtual_dataset("observations", layout)

    with pytest.raises(
        ValueError, match="observations.*virtual.h5.*virtual dataset.*not supported"
    ):
        ray.data.read_hdf5(path, dataset="observations")


def test_hdf5_datasource_aligns_tasks_to_axis_zero_chunks(
    ray_start_regular_shared, tmp_path
):
    import h5py

    from ray.data._internal.datasource import hdf5_datasource

    path = tmp_path / "chunked.h5"
    expected = np.arange(10)
    with h5py.File(path, "w") as file:
        file.create_dataset(
            "observations", data=expected, chunks=(4,), compression="gzip"
        )

    datasource = hdf5_datasource.HDF5Datasource(path, dataset="observations")
    original_read_segment = hdf5_datasource._read_segment
    with patch.object(
        hdf5_datasource, "_read_segment", wraps=original_read_segment
    ) as read_segment:
        blocks = [block for task in datasource.get_read_tasks(32) for block in task()]

    segments = [call.args[1] for call in read_segment.call_args_list]
    assert [(segment.start, segment.stop) for segment in segments] == [
        (0, 4),
        (4, 8),
        (8, 10),
    ]
    actual = np.concatenate(
        [BlockAccessor.for_block(block).to_pandas()["data"] for block in blocks]
    )
    np.testing.assert_array_equal(actual, expected)

    paths = []
    for file_index in range(3):
        file_path = tmp_path / f"chunked-{file_index}.h5"
        with h5py.File(file_path, "w") as file:
            file.create_dataset(
                "observations",
                data=np.arange(2) + 2 * file_index,
                chunks=(2,),
                compression="gzip",
            )
        paths.append(file_path)

    datasource = hdf5_datasource.HDF5Datasource(paths, dataset="observations")
    tasks = datasource.get_read_tasks(2)
    original_read_segment = hdf5_datasource._read_segment
    with patch.object(
        hdf5_datasource, "_read_segment", wraps=original_read_segment
    ) as read_segment:
        actual = [
            int(value)
            for task in tasks
            for block in task()
            for value in BlockAccessor.for_block(block).to_pandas()["data"]
        ]

    assert len(tasks) == 2
    assert actual == list(range(6))
    assert [
        (
            call.args[1].metadata.path.rsplit("/", 1)[-1],
            call.args[1].start,
            call.args[1].stop,
        )
        for call in read_segment.call_args_list
    ] == [
        ("chunked-0.h5", 0, 2),
        ("chunked-1.h5", 0, 2),
        ("chunked-2.h5", 0, 2),
    ]


def test_hdf5_datasource_plans_segments_for_block_size_and_chunks(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    import h5py

    from ray.data._internal.datasource import hdf5_datasource

    data_context = ray.data.DataContext.get_current()
    monkeypatch.setattr(data_context, "target_max_block_size", 128)

    fixed_path = tmp_path / "oversized-chunk.h5"
    with h5py.File(fixed_path, "w") as file:
        file.create_dataset(
            "observations",
            data=np.arange(64, dtype=np.int64).reshape(16, 4),
            chunks=(16, 4),
            compression="gzip",
        )

    contiguous_path = tmp_path / "contiguous.h5"
    with h5py.File(contiguous_path, "w") as file:
        file.create_dataset("observations", data=np.arange(64, dtype=np.int64))

    near_target_path = tmp_path / "near-target-chunks.h5"
    with h5py.File(near_target_path, "w") as file:
        file.create_dataset(
            "observations",
            data=np.arange(30, dtype=np.int64).reshape(10, 3),
            chunks=(4, 3),
            compression="gzip",
        )

    small_chunks_path = tmp_path / "small-chunks.h5"
    with h5py.File(small_chunks_path, "w") as file:
        file.create_dataset(
            "observations",
            data=np.arange(100, dtype=np.int8),
            chunks=(10,),
            compression="gzip",
        )

    vlen_path = tmp_path / "unknown-chunk.h5"
    with h5py.File(vlen_path, "w") as file:
        values = file.create_dataset(
            "observations", (8,), dtype=h5py.vlen_dtype(np.int32), chunks=(8,)
        )
        for index in range(8):
            values[index] = np.arange(index + 1, dtype=np.int32)

    for path, parallelism, expected_ranges, expected_opens in [
        (fixed_path, 1, [(0, 4), (4, 8), (8, 12), (12, 16)], 1),
        (contiguous_path, 1, [(0, 16), (16, 32), (32, 48), (48, 64)], 1),
        (near_target_path, 2, [(0, 4), (4, 8), (8, 10)], 2),
        (small_chunks_path, 2, [(0, 50), (50, 100)], 2),
        (vlen_path, 4, [(0, 2), (2, 4), (4, 6), (6, 8)], 4),
    ]:
        datasource = hdf5_datasource.HDF5Datasource(path, dataset="observations")
        original_read_segment = hdf5_datasource._read_segment
        original_open_input_file = datasource._filesystem.open_input_file
        with (
            patch.object(
                hdf5_datasource, "_read_segment", wraps=original_read_segment
            ) as read_segment,
            patch.object(
                datasource._filesystem,
                "open_input_file",
                wraps=original_open_input_file,
            ) as open_input_file,
        ):
            blocks = []
            for task in datasource.get_read_tasks(
                parallelism, data_context=data_context
            ):
                blocks.extend(task())

        assert [
            (call.args[1].start, call.args[1].stop)
            for call in read_segment.call_args_list
        ] == expected_ranges
        assert open_input_file.call_count == expected_opens
        if path == contiguous_path:
            actual = np.concatenate(
                [BlockAccessor.for_block(block).to_pandas()["data"] for block in blocks]
            )
            np.testing.assert_array_equal(actual, np.arange(64, dtype=np.int64))
        if path == vlen_path:
            rows = [
                value
                for block in blocks
                for value in BlockAccessor.for_block(block).to_pandas()["data"]
            ]
            assert len(rows) == 8
            for index, value in enumerate(rows):
                np.testing.assert_array_equal(
                    value, np.arange(index + 1, dtype=np.int32)
                )


def test_hdf5_datasource_honors_file_shuffle(ray_start_regular_shared, tmp_path):
    import h5py

    from ray.data import FileShuffleConfig
    from ray.data._internal.datasource.hdf5_datasource import HDF5Datasource

    paths = [tmp_path / f"{name}.h5" for name in ["a", "b", "c"]]
    for index, path in enumerate(paths):
        with h5py.File(path, "w") as file:
            file["observations"] = np.array([index])

    datasource = HDF5Datasource(
        paths,
        dataset="observations",
        shuffle=FileShuffleConfig(seed=0, reseed_after_execution=False),
    )

    assert [
        path.rsplit("/", 1)[-1]
        for path in datasource.get_read_tasks(1)[0].metadata.input_files
    ] == [
        "c.h5",
        "a.h5",
        "b.h5",
    ]


def test_read_hdf5_fsspec_filesystem(ray_start_regular_shared, tmp_path):
    import fsspec
    import h5py

    expected = np.arange(6).reshape(3, 2)
    source_path = tmp_path / "data.h5"
    with h5py.File(source_path, "w") as file:
        file["observations"] = expected
    archive_path = tmp_path / "data.zip"
    with ZipFile(archive_path, "w", ZIP_DEFLATED) as archive:
        archive.write(source_path, arcname="inside.h5")

    rows = ray.data.read_hdf5(
        "inside.h5",
        dataset="observations",
        filesystem=fsspec.filesystem("zip", fo=str(archive_path)),
    ).take_all()

    np.testing.assert_array_equal(np.stack([row["data"] for row in rows]), expected)


def test_read_hdf5_nested_scalar_dataset(ray_start_regular_shared, tmp_path):
    import h5py

    path = tmp_path / "data.h5"
    with h5py.File(path, "w") as file:
        file["metadata/version"] = np.int64(7)

    rows = ray.data.read_hdf5(path, dataset="/metadata/version").take_all()

    assert rows == [{"data": 7}]


def test_read_hdf5_multiple_files_with_paths(ray_start_regular_shared, tmp_path):
    import h5py

    paths = [tmp_path / "a.h5", tmp_path / "b.h5"]
    for index, path in enumerate(paths):
        with h5py.File(path, "w") as file:
            file["observations"] = np.array([[2 * index], [2 * index + 1]])

    rows = ray.data.read_hdf5(
        paths,
        dataset="observations",
        include_paths=True,
        override_num_blocks=2,
    ).take_all()

    actual = sorted(
        (int(row["data"][0]), row["path"].rsplit("/", 1)[-1]) for row in rows
    )
    assert actual == [(0, "a.h5"), (1, "a.h5"), (2, "b.h5"), (3, "b.h5")]


def test_read_hdf5_adds_hive_partitions(ray_start_regular_shared, tmp_path):
    import h5py

    from ray.data.datasource import Partitioning

    path = tmp_path / "year=2026" / "month=08" / "data.h5"
    path.parent.mkdir(parents=True)
    with h5py.File(path, "w") as file:
        file["observations"] = np.array([1, 2])

    rows = ray.data.read_hdf5(
        tmp_path,
        dataset="observations",
        partitioning=Partitioning("hive"),
    ).take_all()

    assert rows == [
        {"data": 1, "year": "2026", "month": "08"},
        {"data": 2, "year": "2026", "month": "08"},
    ]


def test_hdf5_local_scheme_pins_reads(ray_start_regular_shared, tmp_path):
    import h5py

    from ray.data._internal.datasource.hdf5_datasource import HDF5Datasource

    path = tmp_path / "data.h5"
    with h5py.File(path, "w") as file:
        file["observations"] = np.arange(3)

    datasource = HDF5Datasource(f"local://{path}", dataset="observations")

    assert datasource.supports_distributed_reads is False
    assert ray.data.read_hdf5(f"local://{path}", dataset="observations").count() == 3


def test_read_hdf5_missing_dependency(ray_start_regular_shared, tmp_path):
    path = tmp_path / "data.h5"
    with patch("importlib.import_module", side_effect=ImportError):
        with pytest.raises(ImportError, match="HDF5Datasource.*pip install h5py"):
            ray.data.read_hdf5(path, dataset="observations")


def test_read_hdf5_decodes_strings(ray_start_regular_shared, tmp_path, monkeypatch):
    import h5py

    data_context = ray.data.DataContext.get_current()
    monkeypatch.setattr(data_context, "enable_fallback_to_arrow_object_ext_type", False)
    path = tmp_path / "strings.h5"
    with h5py.File(path, "w") as file:
        file.create_dataset(
            "labels",
            data=["猫", "café"],
            dtype=h5py.string_dtype(encoding="utf-8"),
        )
        file.create_dataset(
            "fixed_labels", data=np.asarray([b"cat", b"dog"], dtype="S3")
        )
        file.create_dataset(
            "label_pairs",
            data=np.asarray([[b"cat", b"dog"], [b"fox", b"owl"]], dtype="S3"),
        )

    rows = ray.data.read_hdf5(path, dataset="labels").take_all()
    fixed_rows = ray.data.read_hdf5(path, dataset="fixed_labels").take_all()
    pair_rows = ray.data.read_hdf5(path, dataset="label_pairs").take_all()

    assert rows == [{"data": "猫"}, {"data": "café"}]
    assert fixed_rows == [{"data": "cat"}, {"data": "dog"}]
    assert pair_rows == [
        {"data": ["cat", "dog"]},
        {"data": ["fox", "owl"]},
    ]


def test_read_hdf5_variable_length_values_have_unknown_size(
    ray_start_regular_shared, tmp_path
):
    import h5py

    from ray.data._internal.datasource.hdf5_datasource import HDF5Datasource

    path = tmp_path / "strings.h5"
    with h5py.File(path, "w") as file:
        file.create_dataset(
            "labels",
            data=["x" * 10_000],
            dtype=h5py.string_dtype(encoding="utf-8"),
        )

    datasource = HDF5Datasource(path, dataset="labels")

    assert datasource.estimate_inmemory_data_size() is None
    assert datasource.get_read_tasks(1)[0].metadata.size_bytes is None

    fixed_string_path = tmp_path / "fixed-strings.h5"
    with h5py.File(fixed_string_path, "w") as file:
        file.create_dataset(
            "labels", data=np.asarray([b"a", b"b"], dtype="S1"), chunks=(2,)
        )

    datasource = HDF5Datasource(fixed_string_path, dataset="labels")

    assert datasource.estimate_inmemory_data_size() is None
    assert datasource.get_read_tasks(1)[0].metadata.size_bytes is None

    numeric_path = tmp_path / "numeric-vlen.h5"
    with h5py.File(numeric_path, "w") as file:
        values = file.create_dataset("values", (1,), dtype=h5py.vlen_dtype(np.int32))
        values[0] = np.arange(10_000, dtype=np.int32)

    datasource = HDF5Datasource(numeric_path, dataset="values")

    assert datasource.estimate_inmemory_data_size() is None
    assert datasource.get_read_tasks(1)[0].metadata.size_bytes is None


def test_read_hdf5_vlen_arrays_use_arrow_lists(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    import h5py

    from ray.data._internal.datasource.hdf5_datasource import _to_arrow_nested_list

    numeric_leaf = np.arange(4, dtype=np.int32)
    container = np.empty(1, dtype=object)
    container[0] = numeric_leaf
    converted = _to_arrow_nested_list(container)
    assert converted[0] is numeric_leaf

    data_context = ray.data.DataContext.get_current()
    monkeypatch.setattr(data_context, "enable_fallback_to_arrow_object_ext_type", False)
    path = tmp_path / "vlen-arrays.h5"
    expected = [[[1], [2, 3]], [[4, 5, 6], [7]]]
    with h5py.File(path, "w") as file:
        values = file.create_dataset(
            "observations", (2, 2), dtype=h5py.vlen_dtype(np.int32)
        )
        for row_index, row in enumerate(expected):
            for column_index, value in enumerate(row):
                values[row_index, column_index] = np.asarray(value, dtype=np.int32)

    rows = ray.data.read_hdf5(path, dataset="observations").take_all()

    assert rows == [{"data": value} for value in expected]


def test_hdf5_per_task_row_limit_bounds_physical_read(
    ray_start_regular_shared, tmp_path
):
    import h5py

    from ray.data._internal.datasource import hdf5_datasource

    paths = [tmp_path / "a.h5", tmp_path / "b.h5"]
    for path, start in zip(paths, [0, 10]):
        with h5py.File(path, "w") as file:
            file["observations"] = np.arange(start, start + 10)

    datasource = hdf5_datasource.HDF5Datasource(paths, dataset="observations")
    original_read_segment = hdf5_datasource._read_segment
    with patch.object(
        hdf5_datasource, "_read_segment", wraps=original_read_segment
    ) as read_segment:
        blocks = list(datasource.get_read_tasks(1, per_task_row_limit=2)[0]())

    assert sum(BlockAccessor.for_block(block).num_rows() for block in blocks) == 2
    segment = read_segment.call_args.args[1]
    assert (segment.start, segment.stop) == (0, 2)
    assert segment.metadata.path.endswith("a.h5")
    assert read_segment.call_count == 1


def test_hdf5_segment_read_retries_transient_io(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    import h5py

    from ray.data._internal.datasource import hdf5_datasource

    monkeypatch.setattr("time.sleep", lambda *_: None)
    path = tmp_path / "data.h5"
    with h5py.File(path, "w") as file:
        file["observations"] = np.arange(2)

    datasource = hdf5_datasource.HDF5Datasource(path, dataset="observations")
    data_context = ray.data.DataContext.get_current()
    monkeypatch.setattr(data_context, "retried_io_errors", ["Connection reset"])
    with patch.object(
        hdf5_datasource,
        "_read_segment",
        side_effect=[OSError("Connection reset by peer"), np.arange(2)],
    ) as read_segment:
        blocks = list(datasource.get_read_tasks(1, data_context=data_context)[0]())

    assert sum(BlockAccessor.for_block(block).num_rows() for block in blocks) == 2
    assert read_segment.call_count == 2

    with patch.object(
        hdf5_datasource,
        "_read_segment",
        side_effect=OSError("Invalid HDF5 header"),
    ) as read_segment:
        with pytest.raises(OSError, match="Invalid HDF5 header"):
            list(datasource.get_read_tasks(1, data_context=data_context)[0]())

    assert read_segment.call_count == 1


def test_hdf5_segment_retry_does_not_duplicate_completed_segments(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    import h5py

    from ray.data._internal.datasource import hdf5_datasource

    monkeypatch.setattr("time.sleep", lambda *_: None)
    paths = [tmp_path / "a.h5", tmp_path / "b.h5"]
    for index, path in enumerate(paths):
        with h5py.File(path, "w") as file:
            file["observations"] = np.array([index])

    datasource = hdf5_datasource.HDF5Datasource(paths, dataset="observations")
    data_context = ray.data.DataContext.get_current()
    monkeypatch.setattr(data_context, "retried_io_errors", ["Connection reset"])
    original_read_segment = hdf5_datasource._read_segment
    calls = []

    def read_segment(dataset, segment):
        filename = segment.metadata.path.rsplit("/", 1)[-1]
        calls.append(filename)
        if filename == "b.h5" and calls.count("b.h5") == 1:
            raise OSError("Connection reset by peer")
        return original_read_segment(dataset, segment)

    with patch.object(hdf5_datasource, "_read_segment", side_effect=read_segment):
        blocks = list(datasource.get_read_tasks(1, data_context=data_context)[0]())

    actual = [
        int(value)
        for block in blocks
        for value in BlockAccessor.for_block(block).to_pandas()["data"]
    ]
    assert actual == [0, 1]
    assert calls == ["a.h5", "b.h5", "a.h5", "b.h5"]


def test_hdf5_metadata_inspection_is_batched_for_many_files(
    ray_start_regular_shared, tmp_path
):
    import h5py

    from ray.data._internal.datasource import hdf5_datasource

    paths = []
    for index in range(17):
        path = tmp_path / f"{index}.h5"
        with h5py.File(path, "w") as file:
            file["observations"] = np.array([index])
        paths.append(path)

    batches = []

    def fetch_metadata(uris, fetch_func, desired_uris_per_task):
        batches.append((list(uris), desired_uris_per_task))
        yield from fetch_func(uris)

    with patch.object(
        hdf5_datasource, "_fetch_metadata_parallel", side_effect=fetch_metadata
    ):
        datasource = hdf5_datasource.HDF5Datasource(paths, dataset="observations")

    assert len(datasource._hdf5_metadata) == 17
    assert batches == [(list(datasource._paths()), 16)]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
