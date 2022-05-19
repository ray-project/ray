import pytest

import ray
import ray.data

from ray.ml import train_test_split


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


# we could split this into multiple unit tests,
# but it is not worth it due to the overhead of
# starting ray each time
def test_train_test_split(ray_start_4_cpus):
    ds = ray.data.range(8)

    # float
    train, test = train_test_split(ds, test_size=0.25)
    assert train.take() == [0, 1, 2, 3, 4, 5]
    assert test.take() == [6, 7]

    # int
    train, test = train_test_split(ds, test_size=2)
    assert train.take() == [0, 1, 2, 3, 4, 5]
    assert test.take() == [6, 7]

    # shuffle
    train, test = train_test_split(ds, test_size=0.25, shuffle=True, seed=1)
    assert train.take() == [5, 7, 6, 3, 0, 4]
    assert test.take() == [2, 1]

    # error handling
    with pytest.raises(TypeError):
        train_test_split(ds, test_size=[1])

    with pytest.raises(ValueError):
        train_test_split(ds, test_size=-1)

    with pytest.raises(ValueError):
        train_test_split(ds, test_size=0)

    with pytest.raises(ValueError):
        train_test_split(ds, test_size=1.1)

    with pytest.raises(ValueError):
        train_test_split(ds, test_size=9)
