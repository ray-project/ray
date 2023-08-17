import pytest

import ray
from ray.anyscale.data import read_images


@pytest.mark.parametrize(
    "is_shuffle_enabled,shuffle_seed",
    [
        (False, None),
        (True, None),
        (True, 9176),
    ],
)
def test_random_shuffle(is_shuffle_enabled, shuffle_seed):
    # NOTE: set preserve_order to True to allow consistent output behavior.
    ray.data.DataContext.get_current().execution_options.preserve_order = True

    dir_path = "s3://anonymous@air-example-data/mnist"
    file_paths = [
        "00000.png",
        "00001.png",
        "00002.png",
        "00003.png",
        "00004.png",
        "00005.png",
        "00006.png",
        "00007.png",
        "00008.png",
        "00009.png",
    ]
    input_uris = [f"{dir_path}/{file_path}" for file_path in file_paths]

    ds = read_images(
        paths=input_uris,
        include_paths=True,
        shuffle=is_shuffle_enabled,
        shuffle_seed=shuffle_seed,
    )

    # Execute 10 times to get a set of output paths.
    output_paths_list = []
    for _ in range(10):
        uris = [row["path"][-len(file_paths[0]) :] for row in ds.take_all()]
        output_paths_list.append(uris)
    all_paths_matched = [
        file_paths == output_paths for output_paths in output_paths_list
    ]

    # Check when shuffle is enabled, output order has at least one different case.
    assert not all(all_paths_matched) == is_shuffle_enabled
    # Check all files are output properly without missing one.
    assert all(
        [file_paths == sorted(output_paths) for output_paths in output_paths_list]
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
