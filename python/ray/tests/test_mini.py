import ray

test_values = [1, 1.0, "test", b"test", (0, 1), [0, 1], {0: 1}]


def test_basic_task_api(ray_start_regular):

    # Test a simple function.

    @ray.remote
    def f_simple():
        return 1

    assert ray.get(f_simple.remote()) == 1

    # Test multiple return values.

    @ray.remote(num_returns=3)
    def f_multiple_returns():
        return 1, 2, 3

    x_id1, x_id2, x_id3 = f_multiple_returns.remote()
    assert ray.get([x_id1, x_id2, x_id3]) == [1, 2, 3]

    # Test arguments passed by value.

    @ray.remote
    def f_args_by_value(x):
        return x

    for arg in test_values:
        assert ray.get(f_args_by_value.remote(arg)) == arg

    # Test arguments passed by ID.

    # Test keyword arguments.


def test_put_api(ray_start_regular):

    for obj in test_values:
        assert ray.get(ray.put(obj)) == obj

    # Test putting object refs.
    x_id = ray.put(0)
    for obj in [[x_id], (x_id, ), {x_id: x_id}]:
        assert ray.get(ray.put(obj)) == obj


def test_actor_api(ray_start_regular):
    @ray.remote
    class Foo:
        def __init__(self, val):
            self.x = val

        def get(self):
            return self.x

    x = 1
    f = Foo.remote(x)
    assert (ray.get(f.get.remote()) == x)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
