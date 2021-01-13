import ray
import ray.autoscaler.sdk

from time import sleep


MAX_RETURNS = 1000

def test_many_returns():
    @ray.remote(num_returns=MAX_RETURNS)
    def f():
        to_return = []
        for _ in range(MAX_RETURNS):
            obj = list(range(10000))
            to_return.append(obj)

        return tuple(to_return)

    returned_refs = f.remote()
    assert len(returned_refs) == MAX_RETURNS

    returned_values = ray.get(returned_refs)
    for obj in returned_values:
        expected = list(range(10000))
        assert obj == expected

ray.init(address="auto")

test_many_returns()
