import pytest
import ray
import datasets


@pytest.fixture(scope="module")
def ray_start_regular(request):  # pragma: no cover
    try:
        yield ray.init(num_cpus=16)
    finally:
        ray.shutdown()


def test_huggingface(ray_start_regular):
    data = datasets.load_dataset("emotion")

    assert isinstance(data, datasets.DatasetDict)

    ray_datasets = ray.data.from_huggingface(data)
    assert isinstance(ray_datasets, dict)

    assert ray.get(ray_datasets["train"].to_arrow_refs())[0].equals(
        data["train"].data.table
    )

    ray_dataset = ray.data.from_huggingface(data["train"])
    assert isinstance(ray_dataset, ray.data.Dataset)

    assert ray.get(ray_dataset.to_arrow_refs())[0].equals(data["train"].data.table)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
