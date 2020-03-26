import sys
import pytest

import ray

def test_deprecate_remote_func(capsys):
    ray.init(num_cpus=1)

    @ray.remote
    def f(n):
        return list(range(n))

    # Call options.
    assert f.options(num_return_vals=0).remote([0]) is None
    # Verify the warning message not show up when calling the `options` method.
    captured = capsys.readouterr()
    assert "DeprecationWarning" not in captured.err
    assert "Use `.options` instead." not in captured.err

    # Call _remote.
    id1 = f._remote(args=[1], num_return_vals=1)
    assert ray.get(id1) == [0]
    # Verify the warning message showed up when calling the `_remote` method.
    captured = capsys.readouterr()
    assert "DeprecationWarning" in captured.err
    assert "Use `.options` instead." in captured.err


def test_deprecate_remote_actor(capsys):
    @ray.remote
    class Actor:
        def __init__(self, x, y=0):
            self.x = x
            self.y = y

        def method(self, a, b=0):
            return self.x, self.y, a, b

        def gpu_ids(self):
            return ray.get_gpu_ids()

    # Call _remote.
    a = Actor._remote(
        args=[0], kwargs={"y": 1}, num_gpus=1, resources={"Custom": 1})
    # Verify the warning message showed up when calling the `_remote` method.
    captured = capsys.readouterr()
    assert "DeprecationWarning" in captured.err
    assert "Use `.options` instead." in captured.err

    # Call options.
    a = Actor.options(
        args=[0], kwargs={"y": 1}, num_gpus=1, resources={"Custom": 1})
    print(a)
    # Verify the warning message not show up when calling the `options` method.
    captured = capsys.readouterr()
    assert "DeprecationWarning" not in captured.err
    assert "Use `.options` instead." not in captured.err


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
